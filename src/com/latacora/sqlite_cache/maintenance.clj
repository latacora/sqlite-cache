(ns com.latacora.sqlite-cache.maintenance
  "Cache maintenance utilities for sqlite-cache.

  This namespace provides functions for:
  - Introspecting cache contents (counts, sizes, functions)
  - Analyzing cache usage patterns
  - Sampling cache entries with predicates
  - Deleting cache entries with dry-run support

  All deletion functions support dry-run mode using honeysql query builders."
  (:require
   [honey.sql.helpers :as h]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as rs]
   [com.latacora.sqlite-cache.bridge :as bridge]
   [com.latacora.sqlite-cache.serialization :as ser]
   [com.latacora.sqlite-cache.db :as db]
   [com.latacora.sqlite-cache.ddl :as ddl])
  (:import
   (java.time Instant)))

;; ============================================================================
;; Cache Entry Predicates
;; ============================================================================

(defn in-the-past [time] [:<= time [:unixepoch]])

(def goes-cold-at [:+ [:coalesce :last-hit :created-at] :ttl])
(def cold? (-> goes-cold-at in-the-past))

(def goes-stale-at [:+ :created-at :max-age])
(def stale? (-> goes-stale-at in-the-past))

;; ============================================================================
;; Core Maintenance Operations
;; ============================================================================

(defn ^:private checkpoint!
  "Runs a WAL checkpoint to move changes from WAL to main database file.
  Uses TRUNCATE mode to minimize WAL size after checkpoint."
  [conn]
  (jdbc/execute-one! conn ["PRAGMA wal_checkpoint(TRUNCATE);"]))

(defn ^:private expire!
  "Expires entries in the cache that are cold or stale."
  [conn]
  (db/exec-one! conn (-> (h/delete-from :cache) (h/where [:or cold? stale?]))))

(defn ^:private maintain-now!
  "Run maintenance immediately on the given connection, ignoring delayed mode.

  Maintenance includes:
  - Deleting expired entries (based on TTL and max-age)
  - Running VACUUM to reclaim space
  - Checkpointing the WAL"
  [conn]
  (expire! conn)
  (jdbc/execute-one! conn ["VACUUM;"])
  (checkpoint! conn))

(def ^:private maintenance-delay-count
  "Counter to track nesting depth of delayed maintenance blocks."
  (atom 0))

(defn maintain!
  "Runs maintenance on the cache db, including expiring old entries and
  running VACUUM. Respects delayed maintenance mode."
  [conn]
  (when (zero? @maintenance-delay-count)
    (maintain-now! conn)))

(defmacro with-delayed-maintenance
  "Execute body with maintenance operations delayed until completion.

  Supports nesting - only the outermost block will run maintenance.
  All maintenance operations within body will be deferred and run once
  at the end when exiting the outermost block."
  [conn & body]
  `(try
     (swap! @#'maintenance-delay-count inc)
     ~@body
     (finally
       (when (zero? (swap! @#'maintenance-delay-count dec))
         (@#'maintain-now! ~conn)))))

;; ============================================================================
;; Predicate Infrastructure
;; ============================================================================

(defn ^:private maybe-inst
  "Converts an epoch second to an Instant, or returns nil if input is nil."
  [epoch-second]
  (when epoch-second
    (Instant/ofEpochSecond epoch-second)))

(defn ^:private build-entry-map
  "Builds a cache entry map from column values (assumed to be complete & in DDL order)."
  [column-values]
  (-> (zipmap ddl/cache-column-names column-values)
      (update :args ser/deserialize)
      (update :result ser/deserialize)
      (update :last-hit maybe-inst)
      (update :created-at maybe-inst)))

(defn ^:private build-sqlite-predicate
  "Builds a SQLite UDF that evaluates a Clojure predicate on cache entries.

   Returns false for any error (deserialization or predicate evaluation)
   to ensure malformed entries are not included in results or deletions."
  [predicate-fn]
  (fn sqlite-udf-wrapper [& column-values]
    (try
      (-> column-values
          build-entry-map
          predicate-fn
          boolean)
      (catch Exception e
        ;; Any error returns false - don't include in results/don't delete
        (println "Warning: Error evaluating cache entry:" (Throwable/.getMessage e))
        false))))

(defn with-predicate
  "Execute a function with a predicate registered as a SQLite UDF.

   The predicate-fn receives cache entry maps.
   The function f receives (conn predicate-call) where predicate-call is
   a honeysql expression that can be used in WHERE clauses."
  [conn predicate-fn f]
  (let [func-name (str (gensym "sqlite_cache_predicate_"))
        sqlite-predicate (build-sqlite-predicate predicate-fn)
        predicate-call (into [(keyword func-name)] ddl/cache-column-names)]
    (bridge/with-func
      {:conn conn
       :func-name func-name
       :func sqlite-predicate}
      (f conn predicate-call))))

;; ============================================================================
;; Introspection Functions
;; ============================================================================

(defn count-entries
  "Returns the total count of entries in the cache."
  [conn]
  (-> conn
      (jdbc/execute-one! ["SELECT COUNT(*) AS c FROM cache;"])
      :c))

(defn count-entries-by-function
  "Returns a map of function names to entry counts."
  [conn]
  (let [query (-> (h/select :function [[:count :*] :count])
                  (h/from :cache)
                  (h/group-by :function))
        results (db/exec! conn query {:builder-fn rs/as-unqualified-arrays})]
    (into {} (rest results))))

(defn list-functions
  "Returns a set of all function names present in the cache."
  [conn]
  (let [query (-> (h/select-distinct :function)
                  (h/from :cache))
        results (db/exec! conn query {:builder-fn rs/as-unqualified-arrays})]
    (into #{} (map first) (rest results))))

(defn size-analysis-by-function
  "Returns size analysis grouped by function.

   Each function's analysis includes separate statistics for args and results."
  [conn]
  (let [;; Build column specs programmatically
        stats [:sum :avg :min :max]
        size-exprs {:args [:length :args]
                    :result [:length :result]
                    :total [:+ [:length :args] [:length :result]]}

        ;; Generate stat columns
        stat-columns (for [stat stats
                           [size-name size-expr] size-exprs
                           ;; Exclude min/max for total (they're not meaningful for sums)
                           :when (not (and (= size-name :total)
                                          (#{:min :max} stat)))]
                       [[stat size-expr]
                        (keyword (str (name stat) "-" (name size-name) "-size"))])

        query (-> (apply h/select
                         :function
                         [[:count :*] :count]
                         stat-columns)
                  (h/from :cache)
                  (h/group-by :function))

        results (db/exec! conn query)]
    (into {}
          (map (fn [row]
                 [(:function row)
                  (dissoc row :function)]))
          results)))

;; ============================================================================
;; Sampling Functions
;; ============================================================================

(defn sample-entries
  "Returns a random sample of cache entry maps matching the given predicate.

   Options:
   - :predicate - Function that receives an entry map and returns true/false
                  (default: constantly true, returns all entries)
   - :limit - Maximum number of entries to return (default: 20)

   Example:
   (sample-entries conn {:predicate #(= (:function %) \"my.ns/expensive-fn\")
                         :limit 10})"
  ([conn] (sample-entries conn {}))
  ([conn {:keys [predicate limit] :or {predicate (constantly true) limit 20}}]
   (with-predicate conn predicate
     (fn [conn predicate-call]
       (let [query (-> (h/select :*)
                       (h/from :cache)
                       (h/where predicate-call)
                       (h/order-by [[:random]])
                       (h/limit limit))
             results (db/exec! conn query {:builder-fn rs/as-unqualified-arrays})]
         (map build-entry-map (rest results)))))))

;; ============================================================================
;; Deletion Functions
;; ============================================================================

(defn ^:private delete-where!
  "Generic deletion function that deletes entries matching a where clause.
   Returns the number of entries deleted and runs maintenance."
  [conn where-clause]
  (let [query (-> (h/delete-from :cache)
                  (h/where where-clause))
        result (-> conn
                   (db/exec-one! query)
                   (:next.jdbc/update-count 0))]
    (maintain! conn)
    result))

(defn delete-cache-entries!
  "Delete cache entries matching the given predicate function.

   The predicate function receives a cache entry map with keys:
   :id, :function, :args, :result, :hits, :last-hit, :ttl, :created-at, :max-age

   Returns the number of entries deleted.
   Runs maintenance after successful deletion."
  [conn predicate-fn]
  (with-predicate conn predicate-fn
    (fn [conn predicate-call]
      (delete-where! conn predicate-call))))

(defn delete-cache-entries-by-function!
  "Delete all cache entries for a specific function name.

   Returns the number of entries deleted.
   Runs maintenance after successful deletion."
  [conn function-name]
  (delete-where! conn [:= :function function-name]))

(defn delete-cache-entry-by-call!
  "Delete a specific cache entry by exact function name and args match.

   IMPORTANT: The args must be provided in the exact format they were stored.
   Since the cache function uses & args (creating a seq), args are stored as
   lists, not vectors. For example:
   - If you cached (my-fn 5), provide args as '(5) not [5]
   - If you cached (my-fn 1 2), provide args as '(1 2) not [1 2]

   The args are serialized and must match exactly for deletion to succeed.

   Returns the number of entries deleted.
   Runs maintenance after successful deletion."
  [conn function-name args]
  (delete-where!
   conn
   [:and
    [:= :function function-name]
    [:= :args (ser/serialize args)]]))

;; ============================================================================
;; Quartile Statistics
;; ============================================================================

(defn size-quartiles-by-function
  "Returns quartile statistics for cache entry sizes grouped by function.

  Uses NTILE window function to calculate quartiles (Q1, median, Q3) for
  both args and result sizes. Returns a map keyed by function name with
  statistics for each function."
  [conn]
  (let [;; Build the query using HoneySQL
        ;; Note: Using raw SQL for window functions as HoneySQL's :over syntax
        ;; requires specific formatting that isn't well documented
        quartile-query
        {:with [[:quartile-data
                 (-> (h/select :function
                              [[:length :args] :args-size]
                              [[:length :result] :result-size]
                              [[:raw "NTILE(4) OVER (PARTITION BY function ORDER BY LENGTH(args))"]
                               :args-quartile]
                              [[:raw "NTILE(4) OVER (PARTITION BY function ORDER BY LENGTH(result))"]
                               :result-quartile])
                     (h/from :cache))]]
         :select [:function
                  [[:min :args-size] :args-min]
                  [[:max [:case [:= :args-quartile 1] :args-size]] :args-q1]
                  [[:max [:case [:<= :args-quartile 2] :args-size]] :args-median]
                  [[:min [:case [:>= :args-quartile 3] :args-size]] :args-q3]
                  [[:max :args-size] :args-max]
                  [[:min :result-size] :result-min]
                  [[:max [:case [:= :result-quartile 1] :result-size]] :result-q1]
                  [[:max [:case [:<= :result-quartile 2] :result-size]] :result-median]
                  [[:min [:case [:>= :result-quartile 3] :result-size]] :result-q3]
                  [[:max :result-size] :result-max]
                  [[:count :*] :count]]
         :from [:quartile-data]
         :group-by [:function]}

        results (db/exec! conn quartile-query)]

    ;; Convert to map keyed by function name
    (into {}
          (map (fn [row]
                 [(:function row)
                  (dissoc row :function)]))
          results)))

;; ============================================================================
;; Convenience Functions
;; ============================================================================

(defn cache-summary
  "Returns a comprehensive summary of cache contents."
  [conn]
  (let [by-function (size-analysis-by-function conn)
        by-function-quartiles (size-quartiles-by-function conn)
        functions (set (keys by-function))
        total-entries (reduce + 0 (map :count (vals by-function)))
        total-size (reduce + 0 (map :sum-total-size (vals by-function)))]
    {:total-entries total-entries
     :functions functions
     :by-function by-function
     :by-function-quartiles by-function-quartiles
     :total-size total-size}))
