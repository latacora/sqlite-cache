# sqlite-cache

A disk-persistent cache for Clojure functions, backed by SQLite. This library is designed for longer-term caching of expensive function calls, especially those involving third-party services that charge by the request.

## Features

- **Disk persistence**: Cache results survive application restarts
- **Time-based expiry**: Keeps track of both how recently data was requested (TTL) and how old the data is overall (max-age) to ensure data is relevant and current.
- **Rich data support**: Uses Transit to serialize Clojure data structures with high type fidelity
- **Zstd compression**: All cache entries are compressed with zstd for reduced disk usage and high performance
- **Thread-safety**: Handles concurrent access safely, prevents multiple threads from asking the same (expensive) question at the same time
- **Cache maintenance**: Tools for maintaining and surgically cleaning up expired entries
- **Robust error handling**: Functions properly handle and propagate exceptions from cached functions
- **Selective cache invalidation**: Fine-grained cache invalidation with predicate-based, function-based, and call-specific deletion

## Expiry Concepts

This cache uses two expiry concepts:

1. **TTL (Time To Live)**: The time since the last hit after which the cache entry is considered "cold" and may be deleted during maintenance.
2. **Max Age**: The maximum time after which a cache entry is considered "stale" and will never be returned, regardless of how frequently it's been accessed.

Default TTL is 3 days; default max-age is 1 week.

## Installation

Add to your `deps.edn`:

```clojure
io.github.latacora/sqlite-cache {:git/url "https://github.com/latacora/sqlite-cache"
                                  :git/sha "..."}
```

## Usage

### Basic Usage

```clojure
(require '[com.latacora.sqlite-cache.core :as cache])

;; Define an expensive function
(defn expensive-api-call [query-params]
  ;; Call a third-party API, fetch data from web, etc.
  (Thread/sleep 1000) ;; Simulating API call
  {:result "data" :timestamp (java.time.Instant/now)})

;; Create a cached version of the function
(def cached-api-call
  (cache/cache
   {:func expensive-api-call
    :func-name "my-api-call"
    :db {:dbtype "sqlite" :dbname "my-cache.db"}}))

;; Call the cached function - first call will execute the original function
(cached-api-call {:query "something"})
;; => {:result "data", :timestamp #inst"2023-01-01T..."}

;; Second call with the same arguments will return the cached result
(cached-api-call {:query "something"})
;; => same result, returned from cache without calling the expensive function
```

### Using with Vars

```clojure
(require '[com.latacora.sqlite-cache.core :as cache])

(defn expensive-operation [& args]
  ;; Some expensive operation
  )

;; Create a cached version using the function var
(def cached-operation (cache/cached-var #'expensive-operation {}))
```

### Cache Configuration Options

```clojure
(cache/cache
 {:db {:dbtype "sqlite" :dbname "custom-cache.db"} ;; SQLite DB configuration
  :func my-function                                ;; Function to cache
  :func-name "custom-function-name"                ;; Name for the cache (defaults to namespace/name for vars)
  :ttl (* 60 60 24)                                ;; TTL in seconds (default: 3 days)
  :max-age (* 60 60 24 7)                          ;; Max age in seconds (default: 1 week)
  :args-cache-key (fn [args] (first args))})       ;; Function to transform args to cache key (default: identity)
```

### Cache Maintenance and Introspection

Cache maintenance and introspection functions are in the `com.latacora.sqlite-cache.maintenance` namespace:

```clojure
(require '[com.latacora.sqlite-cache.core :as cache]
         '[com.latacora.sqlite-cache.maintenance :as maint]
         '[next.jdbc :as jdbc])

(def db {:dbtype "sqlite" :dbname "my-cache.db"})
(def conn (jdbc/get-connection db))

;; Count entries in cache
(maint/count-entries conn)
;; => 42

;; Count entries by function
(maint/count-entries-by-function conn)
;; => {"my.ns/expensive-fn" 10, "other.ns/api-call" 32}

;; Get size analysis by function
(maint/size-analysis-by-function conn)
;; => {"my.ns/expensive-fn" {:count 10, :sum-args-size 1024, :sum-result-size 2048, ...}}

;; Sample cache entries with a predicate
(maint/sample-entries conn #(> (:hits %) 10) {:limit 5})
;; => [{:id 1, :function "my.ns/expensive-fn", :args [...], :result [...], ...}]

;; Get comprehensive cache summary
(maint/cache-summary conn)
;; => {:total-entries 42, :functions #{"my.ns/expensive-fn" ...}, :total-size 12345, ...}

;; Run maintenance (expire and vacuum)
(maint/maintain! conn)

;; Force maintenance immediately (ignoring delay count)
(maint/maintain-now! conn)
```

#### Delayed Maintenance

For bulk operations that might trigger multiple maintenance runs, you can use `with-delayed-maintenance` to defer all maintenance until the operation completes:

```clojure
;; Delay maintenance during bulk operations
(maint/with-delayed-maintenance conn
  ;; Multiple cache operations that would normally trigger maintenance
  (maint/delete-cache-entries-by-function! conn "function1")
  (maint/delete-cache-entries-by-function! conn "function2")
  (maint/delete-cache-entries-by-function! conn "function3")
  ;; Maintenance runs only once at the end
  )
```

### Error Handling

The cache properly handles exceptions from cached functions. When a cached function throws an exception, it is propagated to the caller without caching the error. This ensures that transient errors don't get permanently cached.

```clojure
(defn failing-function [x]
  (if (= x :fail)
    (throw (RuntimeException. "Something went wrong"))
    (* x 2)))

(def cached-fn (cache/cache {:func failing-function :func-name "my-func" :db db}))

;; This will throw the RuntimeException without caching it
(cached-fn :fail)
;; => RuntimeException: Something went wrong

;; Successful calls are still cached normally
(cached-fn 5)
;; => 10 (cached for subsequent calls)
```

### Selective cache invalidation

The library provides several ways to selectively remove entries from the cache:

#### Delete by function

Remove all cache entries for a specific function:

```clojure
(require '[com.latacora.sqlite-cache.maintenance :as maint])

(maint/delete-cache-entries-by-function! conn "my-namespace/my-function")
;; => 15
```

This returns the number of removed cache entries.

This will automatically maintain the db after, as do all the other related calls. This means the total number of records in the cache may decrease by more than the number of removed entries, because maintenance will remove e.g. stale entries.

#### Delete by specific call

Remove a cache entry for a specific function call with exact arguments:

```clojure
(maint/delete-cache-entry-by-call! conn "my-namespace/my-function" [1 2 3])
;; => 1
```

#### Delete by predicate

Remove entries based on a custom predicate function. The predicate receives a map with deserialized cache entry data:

```clojure
;; Delete all entries older than 1 week
(maint/delete-cache-entries! conn
  (fn [entry]
    (.isBefore (:created-at entry)
               (.minusSeconds (java.time.Instant/now) (* 7 24 60 60)))))

;; Delete entries with specific argument values
(maint/delete-cache-entries! conn
  (fn [entry] (some #{:test-data} (:args entry))))

;; Delete entries with error results
(maint/delete-cache-entries! conn
  (fn [entry] (-> entry :result :error some?)))
```

The predicate function receives a cache entry map with these keys:

- `:id` - Unique cache entry identifier
- `:function` - Function name (e.g., "my.ns/my-fn")
- `:args` - Deserialized function arguments
- `:result` - Deserialized function result
- `:hits` - Number of cache hits
- `:last-hit` - Last access time (java.time.Instant or nil)
- `:ttl` - Time-to-live in seconds
- `:created-at` - Creation time (java.time.Instant)
- `:max-age` - Maximum age in seconds

All deletion functions automatically run cache maintenance (expiring old entries and vacuuming) after deletion.

## Implementation Details

### Serialization

The cache uses Transit with JSON canonicalization and zstd compression for storing Clojure data structures with proper type fidelity. This ensures:

1. Rich Clojure data types are preserved (keywords, symbols, dates, etc.)
2. Cache keys are consistent and comparable
3. Efficient database indexing and lookup
4. Reduced disk usage through compression

All cached data (both arguments and results) are compressed using zstd compression level 3, providing a good balance between compression ratio and performance.

### Concurrency

The cache handles concurrent access through SQLite's transaction model, with special care taken to avoid lock contention when updating hit counts. WAL (Write-Ahead Logging) mode is enabled for better read concurrency.

### Database Schema

The SQLite database uses a simple schema with these key columns:

- `function`: The cached function name
- `args`: The serialized arguments
- `result`: The serialized result
- `hits`: Hit counter
- `last-hit`: Timestamp of the last cache hit
- `created-at`: When the entry was created
- `ttl`: Time-to-live in seconds
- `max-age`: Maximum age in seconds

## License

Copyright (c) Latacora, LLC

This project is licensed under the terms of the MIT license.
