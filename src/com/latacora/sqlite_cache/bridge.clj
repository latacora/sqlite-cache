(ns com.latacora.sqlite-cache.bridge
  "Helpers for embedding Clojure into SQLite, providing functionality to add custom functions to SQLite."
  (:import
   (org.sqlite Function)
   (org.sqlite.core Codes)
   (java.lang.reflect Method)
   (java.sql Connection)))

(def ^:private func-methods
  (->>
   Function
   .getDeclaredMethods
   (keep
    (fn [^Method m]
      (when-let [[base val-type]
                 (->> m .getName (re-matches #"args|result|error|value_([a-z]*)"))]
        (Method/.setAccessible m true)
        (let [ks (cond
                   (= base "args") [:args]
                   (= base "error") [:error]
                   (= base "result") [:result (-> m .getParameterTypes first)]
                   ;; at this point, (= base "value") by exclusion
                   (= val-type "type") [:get-arg-type]
                   :else ;; get specific val-type
                   [:get-arg
                    (case val-type
                      ;; SQLite supports 32 and 64 bit integers; we just
                      ;; always get the long (64), but int (32) is
                      ;; accessible here anyway:
                      "int" "int"
                      "double" Codes/SQLITE_FLOAT
                      "text" Codes/SQLITE_TEXT
                      "blob" Codes/SQLITE_BLOB
                      "long" Codes/SQLITE_INTEGER)])]
          [ks m]))))
   (reduce (fn [m [ks method]] (assoc-in m ks method)) nil)))

(defn ^:private resolve-args!
  [^Function func]
  (let [invoke (fn [^Method m & args] (Method/.invoke m func (object-array args)))
        n-args (-> func-methods :args invoke)]
    (for [i (range n-args)
          :let [arg-type (-> func-methods :get-arg-type (invoke (int i)))]]
      (if (= arg-type Codes/SQLITE_NULL)
        nil
        (-> func-methods (get-in [:get-arg arg-type]) (invoke (int i)))))))

(def ^:private unboxed-types
  "Mapping from boxed types (like Double) to unboxed types (like double)."
  {Double Double/TYPE
   Integer Integer/TYPE
   Long Long/TYPE})

(defn ^:private maybe-unbox-type
  "Given a maybe-boxed type, return the unboxed type."
  [maybe-boxed]
  (get unboxed-types maybe-boxed maybe-boxed))

(defn ^:private ->returnable-type
  "Given a return value from a function to be used as a SQLite function, coerce it
  to a value SQLite will understand."
  [return-val]
  (cond
    (boolean? return-val) (if return-val 1 0)
    :else return-val))

(defn ^:private return!
  [^Function func return-val]
  (let [return-val (->returnable-type return-val)
        return-type (-> return-val type maybe-unbox-type)
        method (get-in func-methods [:result return-type])]
    (Method/.invoke method func (object-array [return-val]))))

(defn ->Function
  "Converts a Clojure function to a SQLite Function."
  [f]
  (proxy [Function] []
    (xFunc []
      (try
        (->> this (resolve-args!) (apply f) (return! this))
        (catch Exception e
          (let [error-method (get func-methods :error)]
            (Method/.invoke error-method this (object-array [(str e)]))))))))

(defn add-func!
  "Adds a Clojure function as a SQLite user-defined function."
  [^Connection conn func-name f]
  (Function/create conn func-name (->Function f)))

(defn remove-func!
  "Removes a previously added SQLite user-defined function."
  [^Connection conn func-name]
  (Function/destroy conn func-name))

(defmacro with-func
  "Executes body with a SQLite user-defined function temporarily added to the connection."
  [{:keys [conn func-name func]} & body]
  `(try
     (add-func! ~conn ~func-name ~func)
     ~@body
     (finally
       (remove-func! ~conn ~func-name))))

;; # Example: regexp matching

;; This is a common enough one it's included here.

(defn regexp-matches?
  "Helper function that implements a regexp pattern matcher for SQLite."
  [re-str s]
  (some? (re-matches (re-pattern re-str) s)))

(defn add-regexp!
  "Adds a regexp function to SQLite that can be used with the REGEXP operator."
  [conn]
  (add-func! conn "regexp" regexp-matches?))

(defmacro with-regexp
  "Defines the `regexp` user defined function.

  This enables the `REGEXP` operator: SQLite translates `x REGEXP y` to
  `regexp(y, x)`."
  [{:keys [conn]} & body]
  `(with-func {:conn ~conn :func-name "regexp" :func regexp-matches?}
     ~@body))
