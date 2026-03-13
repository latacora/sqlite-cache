(ns com.latacora.sqlite-cache.db
  "Database helpers for sqlite-cache.

  Provides utility functions for executing honeysql queries
  with sensible defaults (snake_case to kebab-case conversion)."
  (:require
   [honey.sql :as hsql]
   [next.jdbc :as jdbc]))

(defn- exec*
  "Internal helper for executing honeysql queries.
   Automatically converts snake_case columns to kebab-case keywords."
  [exec-fn conn q opts]
  (exec-fn conn (hsql/format q) (merge jdbc/unqualified-snake-kebab-opts opts)))

(defn exec!
  "Execute a honeysql query, returning all results.
   Automatically converts snake_case columns to kebab-case keywords.
   Accepts optional jdbc opts as third argument, which are merged with defaults."
  ([conn q] (exec! conn q {}))
  ([conn q opts]
   (exec* jdbc/execute! conn q opts)))

(defn exec-one!
  "Execute a honeysql query, returning one result.
   Automatically converts snake_case columns to kebab-case keywords.
   Accepts optional jdbc opts as third argument, which are merged with defaults."
  ([conn q] (exec-one! conn q {}))
  ([conn q opts]
   (exec* jdbc/execute-one! conn q opts)))
