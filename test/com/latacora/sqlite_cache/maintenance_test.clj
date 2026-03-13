(ns com.latacora.sqlite-cache.maintenance-test
  (:require
   [com.latacora.sqlite-cache.maintenance :as maint]
   [com.latacora.sqlite-cache.serialization :as ser]
   [com.latacora.sqlite-cache.test-utils :as tu]
   [clojure.string :as str]
   [clojure.test :as t]
   [next.jdbc :as jdbc])
  (:import
   (java.time Instant)))

;; ============================================================================
;; Test Data Setup
;; ============================================================================

(def test-entries
  "Standard test data used across all tests.
   Includes:
   - Multiple functions (fn1 appears twice, fn2 and fn3 once each)
   - Different arg/result sizes for size analysis
   - Various hit counts and timestamps for filtering"
  (let [small-data {:small "data"}
        large-data {:large (str/join (repeat 100 "data"))}
        ;; Use current time minus a day so entries aren't expired
        base-time (- (quot (System/currentTimeMillis) 1000) 86400)]
    [{:id 1 :function "my.ns/fn1" :args [1 2] :result small-data
      :hits 10 :last-hit (+ base-time 5000) :ttl 1000000 :created-at base-time :max-age 2000000}
     {:id 2 :function "my.ns/fn1" :args [3 4] :result large-data
      :hits 20 :last-hit (+ base-time 6000) :ttl 1000000 :created-at (+ base-time 100) :max-age 2000000}
     {:id 3 :function "other.ns/fn2" :args small-data :result [1 2 3]
      :hits 30 :last-hit (+ base-time 7000) :ttl 1000000 :created-at (+ base-time 200) :max-age 2000000}
     {:id 4 :function "third.ns/fn3" :args large-data :result "result"
      :hits 0 :last-hit nil :ttl 1000000 :created-at (+ base-time 300) :max-age 2000000}]))

(defn insert-test-entries!
  "Insert test entries into the cache table."
  ([conn] (insert-test-entries! conn test-entries))
  ([conn entries]
   (doseq [{:keys [id function args result hits last-hit ttl created-at max-age]} entries]
     (jdbc/execute! conn
                    ["INSERT INTO cache (id, function, args, result, hits, last_hit, ttl, created_at, max_age)
                      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
                     id function
                     (ser/serialize args)
                     (ser/serialize result)
                     hits last-hit ttl created-at max-age]))))

;; ============================================================================
;; Introspection Tests
;; ============================================================================

(t/deftest count-entries-test
  (tu/with-test-db
    (fn [{:keys [conn]}]
      (t/testing "count-entries returns 0 for empty cache"
        (t/is (= 0 (maint/count-entries conn))))

      (insert-test-entries! conn)

      (t/testing "count-entries returns correct count"
        (t/is (= 4 (maint/count-entries conn)))))))

(t/deftest count-entries-by-function-test
  (tu/with-test-db
    (fn [{:keys [conn]}]
      (insert-test-entries! conn)

      (t/testing "count-entries-by-function groups correctly"
        (t/is (= {"my.ns/fn1" 2
                  "other.ns/fn2" 1
                  "third.ns/fn3" 1}
                 (maint/count-entries-by-function conn)))))))

(t/deftest list-functions-test
  (tu/with-test-db
    (fn [{:keys [conn]}]
      (t/testing "list-functions returns empty set for empty cache"
        (t/is (= #{} (maint/list-functions conn))))

      (insert-test-entries! conn)

      (t/testing "list-functions returns unique function names"
        (t/is (= #{"my.ns/fn1" "other.ns/fn2" "third.ns/fn3"}
                 (maint/list-functions conn)))))))

(t/deftest size-analysis-by-function-test
  (tu/with-test-db
    (fn [{:keys [conn]}]
      (insert-test-entries! conn)

      (let [analysis (maint/size-analysis-by-function conn)]
        (t/testing "size-analysis includes all functions"
          (t/is (= #{"my.ns/fn1" "other.ns/fn2" "third.ns/fn3"}
                   (set (keys analysis)))))

        (t/testing "size-analysis has correct counts"
          (t/is (= 2 (get-in analysis ["my.ns/fn1" :count])))
          (t/is (= 1 (get-in analysis ["other.ns/fn2" :count])))
          (t/is (= 1 (get-in analysis ["third.ns/fn3" :count]))))

        (t/testing "size-analysis includes size statistics"
          ;; Just verify the keys exist and values are reasonable
          (t/is (pos? (get-in analysis ["my.ns/fn1" :sum-args-size])))
          (t/is (pos? (get-in analysis ["my.ns/fn1" :sum-result-size])))
          (t/is (pos? (get-in analysis ["my.ns/fn1" :sum-total-size])))
          (t/is (number? (get-in analysis ["my.ns/fn1" :avg-args-size])))
          (t/is (number? (get-in analysis ["my.ns/fn1" :avg-result-size]))))

        (t/testing "fn1 has both small and large results"
          ;; fn1 has one small result and one large result
          (t/is (< (get-in analysis ["my.ns/fn1" :min-result-size])
                   (get-in analysis ["my.ns/fn1" :max-result-size]))))))))

;; ============================================================================
;; Sampling Tests
;; ============================================================================

(t/deftest sample-entries-test
  (tu/with-test-db
    (fn [{:keys [conn]}]
      (insert-test-entries! conn)

      (t/testing "sample-entries without predicate returns all entries"
        (let [entries (maint/sample-entries conn)]
          (t/is (= 4 (count entries)))
          (t/is (every? #(contains? % :id) entries))
          (t/is (every? #(contains? % :function) entries))
          (t/is (every? #(contains? % :args) entries))
          (t/is (every? #(contains? % :result) entries))))

      (t/testing "sample-entries with predicate filters correctly"
        (let [entries (maint/sample-entries conn {:predicate #(= (:function %) "my.ns/fn1")
                                                  :limit 10})]
          (t/is (= 2 (count entries)))
          (t/is (every? #(= "my.ns/fn1" (:function %)) entries))))

      (t/testing "sample-entries with limit"
        (let [entries (maint/sample-entries conn {:limit 2})]
          (t/is (= 2 (count entries)))))

      (t/testing "sample-entries deserializes args and results"
        (let [entries (maint/sample-entries conn {:predicate #(= (:id %) 1)
                                                  :limit 10})]
          (t/is (= 1 (count entries)))
          (let [entry (first entries)]
            (t/is (= [1 2] (:args entry)))
            (t/is (= {:small "data"} (:result entry))))))

      (t/testing "sample-entries converts timestamps to Instant"
        (let [entries (maint/sample-entries conn {:predicate #(= (:id %) 1)
                                                  :limit 10})
              entry (first entries)]
          (t/is (instance? Instant (:last-hit entry)))
          (t/is (instance? Instant (:created-at entry))))))))

;; ============================================================================
;; Deletion Tests
;; ============================================================================

(t/deftest delete-cache-entries!-test
  (tu/with-test-db
    (fn [{:keys [conn]}]
      (insert-test-entries! conn)

      (t/testing "deletion removes entries"
        (let [count (maint/delete-cache-entries! conn
                                                 #(= (:function %) "my.ns/fn1"))]
          (t/is (= 2 count))
          (t/is (= 2 (maint/count-entries conn)))
          (t/is (= #{"other.ns/fn2" "third.ns/fn3"} (maint/list-functions conn)))))

      (t/testing "predicate errors don't delete entries"
        ;; Reset data
        (jdbc/execute! conn ["DELETE FROM cache"])
        (insert-test-entries! conn)

        (let [count (maint/delete-cache-entries! conn
                                                 (fn [entry]
                                                   (if (= (:function entry) "third.ns/fn3")
                                                     (throw (Exception. "Predicate error"))
                                                     false)))]
          (t/is (= 0 count))
          (t/is (= 4 (maint/count-entries conn))))))))

(t/deftest delete-cache-entries-by-function!-test
  (tu/with-test-db
    (fn [{:keys [conn]}]
      (insert-test-entries! conn)

      (t/testing "deletes all entries for function"
        (let [count (maint/delete-cache-entries-by-function! conn "my.ns/fn1")]
          (t/is (= 2 count))
          (t/is (= 2 (maint/count-entries conn)))
          (t/is (= #{"other.ns/fn2" "third.ns/fn3"} (maint/list-functions conn))))))))

(t/deftest delete-cache-entry-by-call!-test
  (tu/with-test-db
    (fn [{:keys [conn]}]
      (insert-test-entries! conn)

      (t/testing "deletes specific entry by function and args"
        (let [deleted-count (maint/delete-cache-entry-by-call! conn "my.ns/fn1" [1 2])]
          (t/is (= 1 deleted-count))
          (t/is (= 3 (maint/count-entries conn)))
          ;; Verify the right entry was deleted
          (let [remaining (maint/sample-entries conn {:predicate #(= (:function %) "my.ns/fn1")
                                                      :limit 10})]
            (t/is (= 1 (count remaining)))
            (t/is (= [3 4] (:args (first remaining)))))))

      (t/testing "returns 0 for non-matching args"
        (let [deleted-count (maint/delete-cache-entry-by-call! conn "my.ns/fn1" [99 99])]
          (t/is (= 0 deleted-count))
          (t/is (= 3 (maint/count-entries conn))))))))

(t/deftest cache-summary-test
  (tu/with-test-db
    (fn [{:keys [conn]}]
      (t/testing "empty cache summary"
        (t/is (= {:total-entries 0
                  :functions #{}
                  :by-function {}
                  :by-function-quartiles {}
                  :total-size 0}
                 (maint/cache-summary conn))))

      (insert-test-entries! conn)

      (t/testing "cache summary with entries"
        (let [summary (maint/cache-summary conn)
              ;; Extract just the structure we care about, sizes will vary
              summary-structure {:total-entries (:total-entries summary)
                                 :functions (:functions summary)
                                 :by-function-keys (set (keys (:by-function summary)))
                                 :fn1-count (get-in summary [:by-function "my.ns/fn1" :count])
                                 :fn2-count (get-in summary [:by-function "other.ns/fn2" :count])
                                 :fn3-count (get-in summary [:by-function "third.ns/fn3" :count])}]
          (t/is (= {:total-entries 4
                    :functions #{"my.ns/fn1" "other.ns/fn2" "third.ns/fn3"}
                    :by-function-keys #{"my.ns/fn1" "other.ns/fn2" "third.ns/fn3"}
                    :fn1-count 2
                    :fn2-count 1
                    :fn3-count 1}
                   summary-structure))
          ;; Just verify total-size is positive
          (t/is (pos? (:total-size summary))))))))

(t/deftest size-quartiles-by-function-test
  (tu/with-test-db
    (fn [{:keys [conn]}]
      (t/testing "empty cache returns empty map"
        (t/is (= {} (maint/size-quartiles-by-function conn))))

      ;; Insert test data with known size distributions
      ;; Function 1: 10 entries with args sizes [10, 20, 30, ..., 100]
      (doseq [i (range 1 11)]
        (let [args (pr-str (repeat i :x))  ; Args of increasing size
              result (pr-str (repeat (* i 10) :y))]  ; Results 10x larger
          (jdbc/execute! conn ["INSERT INTO cache (function, args, result, created_at, last_hit, ttl, max_age)
                                VALUES (?, ?, ?, ?, ?, ?, ?)"
                               "test-fn-1" args result i nil 86400 604800])))

      ;; Function 2: 4 entries with specific sizes for easy quartile verification
      (doseq [[i size] [[1 10] [2 20] [3 30] [4 40]]]
        (let [args (str/join (repeat size "a"))
              result (str/join (repeat (* size 2) "b"))]
          (jdbc/execute! conn ["INSERT INTO cache (function, args, result, created_at, last_hit, ttl, max_age)
                                VALUES (?, ?, ?, ?, ?, ?, ?)"
                               "test-fn-2" args result i nil 86400 604800])))

      (t/testing "quartiles calculated correctly for multiple functions"
        (let [quartiles (maint/size-quartiles-by-function conn)
              fn1-stats (get quartiles "test-fn-1")
              fn2-stats (get quartiles "test-fn-2")]

          (t/testing "test-fn-1 quartiles"
            (t/is (some? fn1-stats))
            ;; With 10 entries, quartiles divide as: [1-3], [4-5], [6-8], [9-10]
            ;; Args sizes are based on serialized (repeat i :x) expressions
            (t/is (= 10 (:count fn1-stats)))
            (t/is (pos? (:args-min fn1-stats)))
            (t/is (< (:args-min fn1-stats) (:args-q-1 fn1-stats)))
            (t/is (<= (:args-q-1 fn1-stats) (:args-median fn1-stats)))
            (t/is (<= (:args-median fn1-stats) (:args-q-3 fn1-stats)))
            (t/is (<= (:args-q-3 fn1-stats) (:args-max fn1-stats))))

          (t/testing "test-fn-2 quartiles with known values"
            (t/is (some? fn2-stats))
            (t/is (= 4 (:count fn2-stats)))
            ;; With 4 entries of size 10,20,30,40:
            ;; Q1 should be 10, median 20, Q3 30, max 40
            (t/is (= 10 (:args-min fn2-stats)))
            (t/is (= 10 (:args-q-1 fn2-stats)))
            (t/is (= 20 (:args-median fn2-stats)))
            (t/is (= 30 (:args-q-3 fn2-stats)))
            (t/is (= 40 (:args-max fn2-stats)))
            ;; Results are 2x args
            (t/is (= 20 (:result-min fn2-stats)))
            (t/is (= 80 (:result-max fn2-stats)))))))))

(t/deftest enhanced-cache-summary-test
  (tu/with-test-db
    (fn [{:keys [conn]}]
      ;; Insert simple test data
      (jdbc/execute! conn ["INSERT INTO cache (function, args, result, created_at, last_hit, ttl, max_age)
                            VALUES (?, ?, ?, ?, ?, ?, ?)"
                           "test-fn" "small-args" "small-result" 1 nil 86400 604800])
      (jdbc/execute! conn ["INSERT INTO cache (function, args, result, created_at, last_hit, ttl, max_age)
                            VALUES (?, ?, ?, ?, ?, ?, ?)"
                           "test-fn" "larger-args-here" "larger-result-here" 2 nil 86400 604800])

      (t/testing "enhanced cache-summary includes quartile data"
        (let [summary (maint/cache-summary conn)]
          (t/is (contains? summary :by-function-quartiles))
          (t/is (contains? summary :total-entries))
          (t/is (= 2 (:total-entries summary)))
          (t/is (contains? summary :by-function))
          (t/is (contains? (get-in summary [:by-function-quartiles "test-fn"]) :args-median)))))))
