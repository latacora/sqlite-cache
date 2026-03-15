(ns com.latacora.sqlite-cache.test-utils
  "Shared test utilities for sqlite-cache tests"
  (:require
   [com.latacora.sqlite-cache.core :as c]
   [com.latacora.sqlite-cache.ddl :as ddl]
   [com.latacora.sqlite-cache.maintenance :as maint]
   [com.latacora.sqlite.bridge :as sqlite]
   [next.jdbc :as jdbc]
   [clojure.test :as t])
  (:import
   (java.io File)
   (java.sql Connection)
   (java.util.concurrent LinkedBlockingQueue)))

(defn sync-write-queue!
  "Synchronize with the write queue to ensure all writes have completed.

  Guarantees that all queued write operations have finished processing.
  Takes a cache function that contains the write queue in its metadata."
  [cache-fn]
  (let [queue (-> cache-fn meta :write-queue)
        completion-promise (promise)]
    (LinkedBlockingQueue/.put queue {:op-fn (fn [_] (deliver completion-promise :done))})
    (deref completion-promise)))

(defn assert-n-entries!
  "Assert that cache has exactly n entries, waiting for async writes to complete"
  [cache-fn expected-count]
  (sync-write-queue! cache-fn)
  (t/is (= expected-count (-> cache-fn meta :read-conn maint/count-entries))))

(defn cleanup-test-db!
  "Clean up test database files"
  [db-name]
  (->> ["" "-shm" "-wal"]
       (map #(File. (str db-name %)))
       (filter #(File/.exists %))
       (run! #(File/.delete %))))

(defn make-test-db
  "Create test database configuration with optional prefix"
  ([] (make-test-db "test"))
  ([prefix]
   (let [test-id (str prefix "-" (java.util.UUID/randomUUID))
         db-file (str "/tmp/" test-id ".db")]
     {:dbtype "sqlite" :dbname db-file})))

(defn run-parallel!
  "Run n-threads in parallel, each executing the given function"
  [n-threads thread-fn]
  (->> #(future (thread-fn)) (repeatedly n-threads) doall (map deref) dorun))

(defn with-test-db
  "Base database harness for testing cache logic.

  Sets up a temporary SQLite database with proper schema and cleanup.
  Passes a map with :db and :conn to the test function."
  ([test-fn] (with-test-db {} test-fn))
  ([{:keys [prefix] :or {prefix "test"}} test-fn]
   (let [db (make-test-db prefix)
         conn (jdbc/get-connection db)]
     (try
       ;; Set up the database schema
       (ddl/ensure-cache! {:conn conn})

       (test-fn {:db db :conn conn})
       (finally
         (Connection/.close conn)
         (cleanup-test-db! (:dbname db)))))))


;; Constants for time-based testing
(def clock-start 1700000000)

;; ============================================================================
;; Ring-style Middleware Test Harness
;; ============================================================================

(defn wrap-db
  "Middleware that provides database connection and cleanup.

  Adds :db and :conn to context, ensures proper cleanup."
  [handler]
  (fn [ctx]
    (let [prefix (get-in ctx [:config :db-prefix] "test")
          db (make-test-db prefix)
          conn (jdbc/get-connection db)]
      (try
        (ddl/ensure-cache! {:conn conn})
        (handler (assoc ctx :db db :conn conn))
        (finally
          (Connection/.close conn)
          (cleanup-test-db! (:dbname db)))))))

(defn wrap-mock-clock
  "Middleware that mocks the unixepoch function for deterministic time testing.

  Adds :clock, :advance-clock!, and :unixepoch to context.
  Uses with-redefs for clean, idiomatic mocking.
  Also verifies the mock clock is working correctly."
  [handler]
  (fn [ctx]
    (let [clock (atom clock-start)
          advance-clock! (partial swap! clock +)
          unixepoch (fn [] @clock)
          original-get-connection jdbc/get-connection]
      ;; Add clock to existing connection if present
      (when-let [conn (:conn ctx)]
        (sqlite/add-func! conn "unixepoch" unixepoch))

      ;; Use with-redefs to mock get-connection for new connections
      (with-redefs [jdbc/get-connection
                    (fn [db-spec]
                      (doto (original-get-connection db-spec)
                        (sqlite/add-func! "unixepoch" unixepoch)))]
        (let [ctx-with-clock (assoc ctx
                                    :clock clock
                                    :advance-clock! advance-clock!
                                    :unixepoch unixepoch)]
          ;; Verify the mock clock is working (if we have a connection)
          (when (:conn ctx-with-clock)
            (t/is (-> (:conn ctx-with-clock)
                      (jdbc/execute-one! ["SELECT unixepoch() AS t;"])
                      :t
                      (= clock-start))
                  "fake clock works as expected"))
          (handler ctx-with-clock))))))

(defn wrap-call-counter
  "Middleware that wraps a function with call counting.

  Transforms :func to count invocations, adds :n-calls atom."
  [handler]
  (fn [ctx]
    (let [n-calls (atom 0)
          original-func (or (:func ctx) (get-in ctx [:config :func]) +)
          counting-func (fn [& args]
                          (swap! n-calls inc)
                          (apply original-func args))]
      (handler (assoc ctx
                      :n-calls n-calls
                      :func counting-func)))))

(defn wrap-cached-fn
  "Middleware that creates a cached version of :func and adds testing utilities.

  Uses :db and :func from context to create :cached-fn.
  Also adds :assert-n-entries! and :sync-write-queue! utility functions.

  By default, the cached-fn will automatically sync the write queue after each call
  to ensure deterministic test behavior. This can be disabled by setting
  :auto-sync false in the config."
  [handler]
  (fn [ctx]
    ;; Ensure cache is created inside the execution context, not during middleware construction
    (let [cache-opts (merge {:db (:db ctx)
                            :func (:func ctx)
                            :func-name (get-in ctx [:config :func-name] "my-func")}
                           (get-in ctx [:config :cache-opts] {}))
          auto-sync? (get-in ctx [:config :auto-sync] true)
          base-cached-fn (c/cache cache-opts)
          ;; Wrap cached-fn to automatically sync after each call in tests
          cached-fn (if auto-sync?
                      (fn [& args]
                        (let [result (apply base-cached-fn args)]
                          (sync-write-queue! base-cached-fn)
                          result))
                      base-cached-fn)]
      (handler (assoc ctx
                      :cached-fn cached-fn
                      :assert-n-entries! (partial assert-n-entries! base-cached-fn)
                      :sync-write-queue! (partial sync-write-queue! base-cached-fn))))))

(defn wrap-maintenance-tracking
  "Middleware that tracks maintenance calls.

  Mocks maintain-now! to count invocations, adds :maintenance-calls atom."
  [handler]
  (fn [ctx]
    (let [maintenance-calls (atom 0)
          original-maintain-now! @#'maint/maintain-now!]
      (with-redefs [com.latacora.sqlite-cache.maintenance/maintain-now!
                    (fn [conn]
                      (swap! maintenance-calls inc)
                      (original-maintain-now! conn))]
        (handler (assoc ctx :maintenance-calls maintenance-calls))))))

(defn wrap-exception-tracking
  "Middleware that tracks exceptions in parallel test scenarios.

  Adds :exceptions atom and :with-exception-tracking wrapper function."
  [handler]
  (fn [ctx]
    (let [exceptions (atom [])
          with-exception-tracking
          (fn [thread-fn]
            (fn []
              (try
                (thread-fn)
                (catch Exception e
                  (swap! exceptions conj e)))))
          result (handler (assoc ctx
                                :exceptions exceptions
                                :with-exception-tracking with-exception-tracking))]
      ;; Assert no exceptions occurred after handler completes
      (t/is (empty? @exceptions)
            "Should have no exceptions during test execution")
      result)))

;; ============================================================================
;; Pre-composed Middleware Stacks
;; ============================================================================

(def standard-cache-middleware
  "Standard middleware stack for cache testing.

  Provides database, mock clock, call counting, and cached function.

  With comp, execution is left-to-right in the list.
  Dependencies: wrap-cached-fn needs :db from wrap-db and :func from wrap-call-counter."
  (comp
   wrap-mock-clock
   wrap-db
   wrap-call-counter
   wrap-cached-fn))

(def cache-with-maintenance-middleware
  "Cache testing middleware with maintenance tracking.

  Includes everything from standard-cache-middleware plus maintenance tracking."
  (comp
   wrap-mock-clock
   wrap-maintenance-tracking
   wrap-db
   wrap-call-counter
   wrap-cached-fn))

(def stress-test-middleware
  "Middleware stack for stress testing.

  Includes database, exception tracking, and utilities without clock mocking."
  (comp
   wrap-exception-tracking
   wrap-db))

;; ============================================================================
;; Public Test Harness Functions
;; ============================================================================

(defn with-harness
  "A testing harness for testing cache logic.

  This will set up a simple function (sum) with a cache around it. Each function
  call will also increment the n-calls; the idea is that this side effect makes
  it easy to check if the underlying function was called, or its result pulled
  from cache.

  Options can include:
  - :cache-opts - Options to pass to the cache function
  - :f - Function to cache (default: +)
  - :auto-sync - Whether to automatically sync write queue after each cached-fn call (default: true)"
  ([test-fn]
   (with-harness {} test-fn))
  ([{:keys [cache-opts f auto-sync] :or {f + auto-sync true}} test-fn]
   ((standard-cache-middleware test-fn)
    {:config {:cache-opts cache-opts
              :func f
              :auto-sync auto-sync}})))

(defn with-harness-and-maintenance
  "Testing harness that includes maintenance tracking.

  Like with-harness but also tracks maintenance calls via :maintenance-calls atom.

  Options can include:
  - :cache-opts - Options to pass to the cache function
  - :f - Function to cache (default: +)
  - :auto-sync - Whether to automatically sync write queue after each cached-fn call (default: true)"
  ([test-fn]
   (with-harness-and-maintenance {} test-fn))
  ([{:keys [cache-opts f auto-sync] :or {f + auto-sync true}} test-fn]
   ((cache-with-maintenance-middleware test-fn)
    {:config {:cache-opts cache-opts
              :func f
              :auto-sync auto-sync}})))

(defn with-stress-harness
  "A testing harness for stress testing cache logic.

  This sets up a temporary database with proper cleanup, and provides utilities
  for parallel testing scenarios. Unlike the regular test harness, this doesn't
  mock the clock since stress tests focus on concurrency rather than time-based
  behavior.

  The test-fn receives a map with:
  - :db - Database configuration
  - :conn - Database connection
  - :exceptions - Atom containing any exceptions that occurred
  - :with-exception-tracking - Function to wrap thread functions for exception collection

  Tests can directly use sync-write-queue! and run-parallel! from tu namespace."
  [test-fn]
  ((stress-test-middleware
    (fn [ctx]
      (test-fn (assoc ctx :assert-n-entries! assert-n-entries!))))
   {:config {:db-prefix "stress"}}))

;; ============================================================================
;; Stress Test Helper Functions
;; ============================================================================

(defn counting-fn
  "Creates a function that increments a counter and returns 'prefix:input'.
  Used for stress testing - provides predictable, distinguishable results."
  ([counter] (counting-fn counter "result"))
  ([counter prefix]
   (fn [& args]
     (swap! counter inc)
     (str prefix ":" (pr-str args)))))

(defn sync-all-caches!
  "Synchronize with multiple cache write queues to ensure all writes have completed."
  [cache-fns]
  (doseq [cache-fn cache-fns]
    (sync-write-queue! cache-fn)))

(defn create-test-cache
  "Create a test cache with standard configuration.
  Uses counting-fn to track calls and provide distinguishable results."
  [db call-counter name & [prefix]]
  (c/cache {:db db
            :func (counting-fn call-counter (or prefix name))
            :func-name name}))
