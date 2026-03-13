(ns com.latacora.sqlite-cache.core
  "A disk-persistent cache for Clojure functions, backed by SQLite.

  This is intended for longer-term caches of expensive functions, usually
  third-party services that charge by the request. Since it's SQLite, this cache
  would work perfectly well in-memory, but you may be better served by
  clojure/core.cache.

  This namespace has two expiry concepts: ttl and max-age. ttl is the time since
  the last hit after which the cache entry is considered cold, and _may_ be
  deleted. max-age is the maximum time after which, no matter how hot the cache
  entry, it should not be returned.

  All cache analysis and cleanup (actually deleting expired records and
  VACUUMing afterwards) is done manually (see `maintain!`)."
  (:require
   [honey.sql.helpers :as h]
   [next.jdbc :as jdbc]
   [com.latacora.sqlite-cache.serialization :as ser]
   [com.latacora.sqlite-cache.db :as db]
   [com.latacora.sqlite-cache.ddl :as ddl]
   [com.latacora.sqlite-cache.maintenance :as maint])
  (:import
   (java.util.concurrent LinkedBlockingQueue TimeUnit)))

;; This currently does not use clojure.core.cache, but it probably could stand
;; to be compatible with it. The problem with clojure.core.cache is that it's
;; not persistent (to disk, not in the Clojure data structure sense), and it's
;; not clear how to make it persistent without a lot of work.

;; Forward declarations
(declare put-queue queue-write! hit!)

;; Time constants
(def one-hour (* 60 60))
(def one-day (* 60 60 24))
(def three-days (* one-day 3))
(def one-week (* one-day 7))
(def one-month (* one-day 30))
(def one-year (* one-day 365))
(def one-quarter (/ one-year 4))

(def default-ttl three-days)
(def default-max-age one-week)

;; The cache could definite "creation" as a hit, and make the :last-hit
;; be [:unixepoch] by default. Instead, we choose to make it nil, since it
;; doesn't make sense for the number of hits to be 0 but there to be a defined
;; time when the last hit happened. This does make the cold? query a bit more
;; complicated, since we have to coalesce the last-hit with the created-at,
;; otherwise:

;; [:<= [:+ :last-hit :ttl] [:unixepoch]]
;; [:<= [:+ :last-hit nil]  [:unixepoch]]
;; [:<= nil                 [:unixepoch]]
;; true

;; ... and hence all entries would be cold.

(defn ^:private get!
  "Gets a value from the cache based on the provided options and cache arguments.

  Options should include:
  - conn: The database connection
  - func-name: The function name for which the cache is being checked

  Returns the cached value if found and not stale, or nil otherwise."
  [opts cache-args]
  ;; This function has to deal with a subtle concurrency problem. Originally,
  ;; the attempt to get a cache entry and the hit counter increment happened
  ;; inside of a transaction. If two threads try to get something form the
  ;; cache and both hit, they'll both try to increment the hit count
  ;; almost simultaneously. This results in an upgraded transaction:
  ;;
  ;; 1. Thread A starts transaction A, performs a SELECT, acquiring read lock
  ;; 2. Thread B starts transaction B, performs a SELECT, acquiring read lock)
  ;; 3. Thread A gets a hit, runs an UPDATE, acquiring intent-to-write-lock
  ;; 4. Thread B gets a hit, runs an UPDATE, *fails to acquire the lock*
  ;;
  ;; Step 4 always fails because SQLite knows that A can't complete: it would
  ;; need an exclusive lock to commit to the database, but it can't get it
  ;; because B has the intent-to-write lock. This can't be solved by adding a
  ;; busy handler to wait, because it's a deadlock: B also can't complete
  ;; because A also has an intent-to-write lock preventing B from acquiring an
  ;; exclusive lock.
  ;;
  ;; There are a few ways to solve this. One is to tell SQLite that all of these
  ;; transactions require IMMEDIATE. That reduces reader concurrency to 1.
  ;; Another is to do the cache hit outside of the transaction (because the
  ;; first statement in the hit process is an UPDATE, SQLite knows it needs a
  ;; write lock). We could even have a different thread worry about updating the
  ;; hit counter: it's not critical for correctness, just an approximate
  ;; statistical measure.
  ;;
  ;; For now, we're assuming that hit updates won't block for long enough to
  ;; warrant shunting them off to a separate thread for processing, and that
  ;; hits can be done out-of-transaction and still be almost always correct:
  ;; close enough for this purpose.
  (let [{:keys [read-conn func-name]} opts
        q (-> (h/select :id :result)
              (h/from :cache)
              (h/where
               [:= :function func-name]
               [:= :args cache-args]
               [:not maint/stale?]))
        {:keys [id result]} (db/exec-one! read-conn q)]
    (when id
      (queue-write! (assoc opts :op-fn hit! :entry-id id))
      (ser/deserialize result))))

(defn ^:private hit!
  "Updates hit count and last-hit time for a cache entry."
  [write-op]
  (let [{:keys [write-conn entry-id]} write-op
        q (-> (h/update :cache)
              (h/set {:hits [:+ :hits [:inline 1]]
                      :last-hit [[:unixepoch]]})
              (h/where [:= :id entry-id]))]
    (db/exec-one! write-conn q)))

(defn ^:private put!
  "Puts a result into the cache based on the write-op map."
  [write-op]
  (let [{:keys [write-conn func-name max-age ttl serialized-args result queue-key]} write-op
        values {:function func-name
                :args serialized-args
                :result (ser/serialize result)
                :ttl ttl
                :max-age max-age
                :created-at [[:unixepoch]]
                :hits 0
                :last-hit nil}
        q (-> (h/insert-into :cache)
              (h/values [values])
              (h/on-conflict)
              (h/do-update-set values))]
    (try
      (db/exec-one! write-conn q)
      (finally
        (swap! put-queue dissoc queue-key)))))

(def ^:private put-queue
  "A queue map of `[db func args]` to a delay that will compute the result, put it
  in the cache, and remove itself from this queue.

  This is used to prevent more than one thread from trying to compute the same
  result simultaneously."
  (atom {}))

(defn ^:private make-write-queue!
  "Creates a new write queue with a worker thread that owns the write connection."
  [write-conn]
  (let [queue (LinkedBlockingQueue. 100)]
    (doto (Thread.
           (fn []
             (loop []
               (if-let [{:keys [op-fn] :as write-op} (LinkedBlockingQueue/.poll queue 30 TimeUnit/SECONDS)]
                 (op-fn write-op)
                 (@#'maint/checkpoint! write-conn))
               (recur)))
           "sqlite-cache-writer")
      (Thread/.setDaemon true)
      (Thread/.start))
    queue))

(defn ^:private queue-write!
  "Queues a write operation for asynchronous processing."
  [write-op]
  (-> write-op :write-queue (LinkedBlockingQueue/.put write-op)))

(comment
  ;; Evaluate this to watch the put queue:
  (add-watch
   put-queue :debug
   (fn [_k _atom old new]
     (println "put-queue changed from" old "to" new))))

(defn ^:private cached
  [{:keys [func args-cache-key db] :as opts} & args]
  (let [cache-args (args-cache-key args)
        serialized-args (ser/serialize cache-args)]
    (or
     (get! opts serialized-args)
     ;; Prevent more than 1 thread from trying to compute the same thing at once.
     (let [queue-key [db func cache-args]]
       (->
        (swap!
         put-queue update queue-key
         (fn [maybe-delay]
           (or
            maybe-delay ;; Another thread is already working on this.
            (delay
              (try
                (let [result (apply func args)]
                  ;; Asynchronously write to cache, return immediately, rely on put-queue delay for other, simultaneous requests of the same data
                  (queue-write! (assoc opts :op-fn put! :serialized-args serialized-args :result result :queue-key queue-key))
                  result)
                (catch Exception e
                  ;; Clear put-queue entry on error so future calls can retry
                  (swap! put-queue dissoc queue-key)
                  (throw e)))))))
        (get queue-key)
        (deref))))))

(def default-opts
  "Default options for the cache function.

  - db: SQLite database configuration with default path for the cache
  - args-cache-key: Function to create cache key from arguments (default: identity)
  - ttl: Time-to-live for cache entries (default: three days)
  - max-age: Maximum age for cache entries (default: one week)"
  {:db {:dbtype "sqlite" :dbname "sqlite-cache.db"}
   :args-cache-key identity
   :ttl default-ttl
   :max-age default-max-age})

(defn cache
  "Builds a cache with given opts.

  This will ensure the cache is ready to use (has the appropriate schema,
  indexes, et cetera), and returns a function with the same signature as the
  function being cached.

  The returned function has metadata containing the full cache configuration,
  including connections, write queue, and all options. This can be useful for
  introspection, testing, or advanced cache management.

  Options:
  - db: Database configuration (must include :dbtype \"sqlite\" and :dbname)
  - func: The function to cache
  - func-name: The function name for identification in the cache
  - ttl: Time to live in seconds
  - max-age: Maximum age in seconds
  - args-cache-key: Function to transform arguments into cache key"
  [opts]
  (let [{:keys [db] :as opts} (merge default-opts opts)
        read-conn (jdbc/get-connection db)
        write-conn (jdbc/get-connection db)
        write-queue (make-write-queue! write-conn)
        opts (assoc opts
                    :read-conn read-conn
                    :write-conn write-conn
                    :write-queue write-queue)]
    ;; Set up write connection with full schema, WAL mode, and busy timeout
    (ddl/ensure-cache! {:conn write-conn})
    ;; Set up read connection with busy timeout and read-only mode
    (jdbc/execute-one! read-conn ddl/busy-timeout-stmt)
    (jdbc/execute-one! read-conn ddl/read-only-stmt)
    (-> cached (partial opts) (with-meta opts))))

(defn cached-var
  "A helper function for `cache` that configures the cache name based on the
  fully-qualified function name of the given fn-var.

  This takes a fn-var because that's the object with the metadata we need.

  Example:
  ```
  (def my-expensive-fn (cached-var #'original-expensive-fn {:db my-db-config}))
  ```"
  [fn-var opts]
  (let [{func-ns :ns func-name :name} (meta fn-var)
        fqfn (str (-> func-ns ns-name str) "/" func-name)]
    (cache (assoc opts :func fn-var :func-name fqfn))))
