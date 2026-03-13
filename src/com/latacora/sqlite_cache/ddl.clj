(ns com.latacora.sqlite-cache.ddl
  "Database DDL definitions for sqlite-cache.

  This namespace centralizes all table definitions and column metadata
  to avoid duplication across the codebase."
  (:require
   [honey.sql :as hsql]
   [honey.sql.helpers :as h]
   [next.jdbc :as jdbc]))

(def cache-table-columns
  "Column definitions for the cache table.
  Each entry is a vector of [column-name type constraints...]"
  [[:id :integer [:not nil] [:primary-key]]
   [:function :text [:not nil]]
   [:args :blob [:not nil]]
   [:result :blob [:not nil]]
   [:hits :integer [:not nil] [:default 0]]
   [:last-hit :datetime]
   [:ttl :integer [:not nil]]
   [:created-at :datetime [:not nil] [:default [[:unixepoch]]]]
   [:max-age :integer [:not nil]]])

(def cache-column-names
  "Ordered list of column names in the cache table."
  (map first cache-table-columns))

(def create-cache-table
  "DDL query for creating the cache table."
  (let [tbl (h/create-table :cache :if-not-exists)]
    (apply h/with-columns tbl cache-table-columns)))

(def create-cache-index-stmt
  "DDL for creating the unique index on (function, args)."
  ["CREATE UNIQUE INDEX IF NOT EXISTS cache_idx ON cache (function, args);"])

(def enable-wal-stmt
  "Enable WAL mode for better concurrent access."
  ["PRAGMA journal_mode=WAL;"])

(def busy-timeout-stmt
  "Set 5 second timeout for lock contention."
  ["PRAGMA busy_timeout=5000;"])

(def read-only-stmt
  "Make connection read-only."
  ["PRAGMA query_only=true;"])

(defn ensure-cache!
  "Ensures the given db is ready to be used as a cache. Makes sure the cache table
  exists, is indexed, WAL mode is enabled, and busy timeout is set."
  [{:keys [conn]}]
  (let [create-table-stmt (hsql/format create-cache-table)]
    (doseq [stmt [create-table-stmt create-cache-index-stmt enable-wal-stmt busy-timeout-stmt]]
      (jdbc/execute-one! conn stmt))))
