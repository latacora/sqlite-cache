(ns com.latacora.sqlite-cache.db-test
  (:require
   [com.latacora.sqlite-cache.db :as db]
   [honey.sql.helpers :as h]
   [clojure.test :as t]
   [next.jdbc :as jdbc])
  (:import (java.sql Connection)))

(defn with-generic-test-db
  "Creates a simple generic test database with a test table for testing db functions"
  [test-fn]
  (let [db {:dbtype "sqlite" :dbname ":memory:"}
        conn (jdbc/get-connection db)]
    (try
      ;; Create a simple test table with snake_case columns to test conversion
      (jdbc/execute! conn ["CREATE TABLE test_table (
                            id INTEGER PRIMARY KEY,
                            some_name TEXT,
                            created_at INTEGER,
                            snake_case_value TEXT)"])
      (test-fn {:db db :conn conn})
      (finally
        (Connection/.close conn)))))

(t/deftest exec-one!-test
  (with-generic-test-db
    (fn [{:keys [conn]}]
      (t/testing "exec-one! formats and executes honeysql queries"
        (let [query (h/select [[:inline 42] :answer])]
          (t/is (= {:answer 42}
                   (db/exec-one! conn query)))))

      (t/testing "exec-one! converts snake_case to kebab-case"
        ;; Insert test data with snake_case column names
        (jdbc/execute! conn ["INSERT INTO test_table (id, some_name, created_at, snake_case_value)
                              VALUES (1, 'test', 12345, 'value')"])
        (let [query (-> (h/select :id :some_name :created_at :snake_case_value)
                        (h/from :test_table)
                        (h/where [:= :id 1]))]
          (t/is (= {:id 1 :some-name "test" :created-at 12345 :snake-case-value "value"}
                   (db/exec-one! conn query))))))))

(t/deftest exec!-test
  (with-generic-test-db
    (fn [{:keys [conn]}]
      ;; Insert some test data
      (jdbc/execute! conn ["INSERT INTO test_table (id, some_name, created_at, snake_case_value)
                            VALUES (1, 'first', 100, 'val1'),
                                   (2, 'second', 200, 'val2')"])

      (t/testing "exec! formats and executes honeysql queries returning all results"
        (let [query (-> (h/select :id :some_name)
                        (h/from :test_table)
                        (h/order-by :id))]
          (t/is (= [{:id 1 :some-name "first"}
                    {:id 2 :some-name "second"}]
                   (db/exec! conn query)))))

      (t/testing "exec! converts snake_case to kebab-case"
        (let [query (-> (h/select :id :created_at :snake_case_value)
                        (h/from :test_table)
                        (h/order-by :id))]
          (t/is (= [{:id 1 :created-at 100 :snake-case-value "val1"}
                    {:id 2 :created-at 200 :snake-case-value "val2"}]
                   (db/exec! conn query)))))

      (t/testing "exec! returns empty vector for no results"
        (let [query (-> (h/select :id)
                        (h/from :test_table)
                        (h/where [:= :some_name "nonexistent"]))]
          (t/is (= [] (db/exec! conn query))))))))
