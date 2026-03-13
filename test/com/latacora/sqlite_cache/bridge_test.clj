(ns com.latacora.sqlite-cache.bridge-test
  (:require
   [com.latacora.sqlite-cache.bridge :as bridge]
   [clojure.test :as t]
   [next.jdbc :as jdbc])
  (:import
   (java.sql SQLException)
   (java.util.regex PatternSyntaxException)))

(defn with-test-db
  "Test harness that provides a SQLite connection for testing."
  [test-fn]
  (let [db {:dbtype "sqlite" :dbname ":memory:"}
        conn (jdbc/get-connection db)]
    (test-fn {:conn conn})))

(t/deftest ->Function-test
  (t/testing "->Function works normally for functions that don't throw"
    (with-test-db
      (fn [{:keys [conn]}]
        (bridge/with-func
          {:conn conn :func-name "iddqd" :func +}
          (let [result (jdbc/execute-one! conn ["SELECT iddqd(2, 3) AS result"])]
            (t/is (= 5 (:result result)))))))))

(t/deftest ->Function-error-handling-test
  (with-test-db
    (fn [{:keys [conn]}]
      (doseq [{:keys [pattern err-fn]}
              [{:pattern #".*Iddqd.*"
                :err-fn (fn [_] (throw (RuntimeException. "Iddqd")))}
               {:pattern #".*ArtithmeticException.*"
                :err-fn (fn [_] (/ 1 0))}
               {:pattern #".*NullPointerException.*"
                :err-fn (fn [_] (String/.length nil))}
               {:pattern #".*IllegalArgumentException.*"
                :err-fn (fn [_] (throw (IllegalArgumentException. "Bad arg")))}]]
        (bridge/with-func {:conn conn :func-name "error_func" :func err-fn}
          (t/is
           (thrown?
            SQLException
            (jdbc/execute-one! conn ["SELECT error_func(1) AS result"]))
           (str pattern)))))))

(t/deftest regexp-test
  (t/is (bridge/regexp-matches? ".*foo.*" "foobar"))
  (t/is (not (bridge/regexp-matches? "iddqd" "test")))
  (t/is (thrown? PatternSyntaxException (bridge/regexp-matches? "[invalid" "test")))

  (with-test-db
    (fn [{:keys [conn]}]
      (t/is
       (thrown-with-msg?
        SQLException #".*no such function: REGEXP.*"
        (jdbc/execute-one! conn ["SELECT 'test' REGEXP 'valid' AS result"])))

      (bridge/with-regexp {:conn conn}
        (t/is
         (-> conn
             (jdbc/execute-one! ["SELECT regexp('valid', 'test') AS result"])
             :result
             (= 0))
         "regexp call, function style")

        (t/is
         (-> conn
             (jdbc/execute-one! ["SELECT 'test' REGEXP 'valid' AS result"])
             :result
             (= 0))
         "regexp call, infix style")

        (t/is
         (-> conn
             (jdbc/execute-one! ["SELECT 'valid' REGEXP 'valid' AS result"])
             :result
             (= 1))
         "regexp call, infix style")

        (t/is
         (thrown-with-msg?
          SQLException #".*PatternSyntaxException.*"
          (bridge/with-regexp {:conn conn}
            (jdbc/execute-one! conn ["SELECT 'test' REGEXP '[invalid' AS result"])))))

      (t/is
       (thrown-with-msg?
        SQLException #".*no such function: REGEXP.*"
        (jdbc/execute-one! conn ["SELECT 'test' REGEXP 'valid' AS result"]))))))
