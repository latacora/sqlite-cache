(ns com.latacora.sqlite-cache.core-test
  (:require
   [com.latacora.sqlite-cache.core :as c]
   [com.latacora.sqlite-cache.maintenance :as maint]
   [com.latacora.sqlite-cache.test-utils :as tu]
   [clojure.test :as t]
   [next.jdbc :as jdbc]
   [honey.sql.helpers :as h]
   [honey.sql :as hsql])
  (:import
   (java.time Instant)))

(defn ^:private ->bool
  [x]
  (case x
    (nil 0 false) false
    true))


(defn get-cache-state!
  [conn]
  (let [conn (jdbc/with-options conn jdbc/unqualified-snake-kebab-opts)]
    (->>
     (-> (h/select
          [:id]
          [:hits]
          [[:unixepoch] :clock]

          [:last-hit]
          [:ttl]
          [maint/goes-cold-at :goes-cold-at]
          [[:- maint/goes-cold-at [:unixepoch]] :time-until-cold]
          [maint/cold? :cold?]

          [:created-at]
          [:max-age]
          [maint/goes-stale-at :goes-stale-at]
          [[:- maint/goes-stale-at [:unixepoch]] :time-until-stale]
          [maint/stale? :stale?])
         (h/from :cache))
     (hsql/format)
     (jdbc/execute! conn)
     (map #(-> % (update :cold? ->bool) (update :stale? ->bool))))))

(t/deftest cache-test
  (tu/with-harness
    (fn [{:keys [cached-fn n-calls]}]
      (t/is (= 0 @n-calls))
      (t/is (= 2 (cached-fn 1 1)))
      (t/is (= 1 @n-calls))
      (t/is (= 2 (cached-fn 1 1)))
      (t/is (= 1 @n-calls))
      (t/is (= 3 (cached-fn 1 2)))
      (t/is (= 2 @n-calls)))))

(t/deftest cache-with-args-cache-key-test
  (tu/with-harness
    {:cache-opts {:args-cache-key sort}}
    (fn [{:keys [cached-fn n-calls]}]
      (t/is (= 0 @n-calls))
      (t/is (= 3 (cached-fn 1 2)))
      (t/is (= 1 @n-calls))
      (t/is (= 3 (cached-fn 1 2)))
      (t/is (= 1 @n-calls))
      (t/is (= 3 (cached-fn 2 1)))
      (t/is (= 1 @n-calls)))))

(t/deftest default-expiry-times-test
  (t/is (= c/default-ttl c/three-days 259200))
  (t/is (= c/default-max-age c/one-week 604800))
  (t/is (< c/default-ttl c/default-max-age)))

(t/deftest cache-with-warm-entries-test
  (tu/with-harness
    (fn [{:keys [cached-fn conn n-calls advance-clock! clock]}]
      (t/is (= 2 (cached-fn 1 1)))
      (t/is (= 1 @n-calls))

      (t/is (= 1 (maint/count-entries conn)))
      (let [[{:keys [hits last-hit]}] (get-cache-state! conn)]
        (t/is (= hits 0))
        (t/is (nil? last-hit)))

      (advance-clock! c/one-day)
      (t/is (= 2 (cached-fn 1 1)))
      (t/is (= 1 @n-calls) "got cached result")

      (t/is (= (maint/count-entries conn) 1) "still only one entry in cache")
      (let [[{:keys [hits last-hit]}] (get-cache-state! conn)]
        (t/is (= 1 hits))
        (t/is (= last-hit @clock (+ tu/clock-start c/one-day)))))))

(t/deftest cache-ttl-test
  (tu/with-harness
    (fn [{:keys [cached-fn conn n-calls advance-clock! assert-n-entries!]}]
      (t/is (= 2 (cached-fn 1 1)))
      (t/is (= 1 @n-calls))
      (assert-n-entries! 1)

      (advance-clock! c/default-ttl)
      ;; entry is now cold, but not yet max-age, so it can still be returned and it
      ;; would not be proactively cleaned up.
      (let [state (get-cache-state! conn)]
        #_(t/is (= {} (get-cache-state! conn)))
        (t/is (-> state count (= 1)))
        (t/is (-> state first :cold?))
        (t/is (not (-> state first :stale?))))

      (t/is (= 2 (cached-fn 1 1)))
      (t/is (= 1 @n-calls))
      (t/is (= (maint/count-entries conn) 1))

      ;; Note that as a side effect of calling the cached function, we've
      ;; bumped the most recent hit time, and so the entry is no longer cold.
      (let [state (get-cache-state! conn)]
        (t/is (-> state count (= 1)))
        (t/is (not (-> state first :cold?)))
        (t/is (not (-> state first :stale?))))

      (advance-clock! c/default-ttl)
      (let [state (get-cache-state! conn)]
        (t/is (-> state count (= 1)))
        (t/is (-> state first :cold?))
        ;; Not stale mostly by accident, because max-age (7d) > 2 * ttl (6d)
        (t/is (not (-> state first :stale?))))

      (t/testing  "cache entry is removed after maintenance"
        (maint/maintain! conn)
        (t/is (= (maint/count-entries conn) 0))))))

(t/deftest cache-with-expired-entries-test
  (tu/with-harness
    (fn [{:keys [cached-fn conn n-calls advance-clock! assert-n-entries!]}]
      (t/is (= 2 (cached-fn 1 1)))
      (t/is (= 1 @n-calls))
      (assert-n-entries! 1)

      (advance-clock! (+ c/default-max-age c/one-day))

      (t/testing "entries > max-age are never returned but still present in cache"
        (let [state (get-cache-state! conn)]
          (t/is (-> state count (= 1)))
          (t/is (-> state first :stale?))))

      (maint/maintain! conn)
      (t/is (empty? (get-cache-state! conn))))))

(t/deftest can-call-cached-fn-with-stale-entries-in-conn-test
  ;; Because we update the cache lazily, there's a weird edge case where data is
  ;; stale but still present in the cache. The database enforces a uniqueness
  ;; constraint on (function, args) as a sanity check. That means we either need
  ;; to remove the stale cache entry or teach the cache code how to do upserts
  ;; instead. (The actual code uses upserts, though this test doesn't specify.)
  (tu/with-harness
    (fn [{:keys [cached-fn conn n-calls advance-clock!]}]
      (let [expected-cache-line
            (atom {:id 1
                   :created-at tu/clock-start
                   :hits 0
                   :last-hit nil
                   :ttl c/three-days
                   :cold? false
                   :max-age c/one-week
                   :stale? false})
            assert-state-change!
            (fn [changes]
              (let [expected (swap! expected-cache-line merge changes)
                    ;; we project onto the keys in `expected` so the test doesn't
                    ;; break when the debugging test adds extra keys.
                    actual (-> conn get-cache-state! first (select-keys (keys expected)))]
                (t/is (= expected actual))))]

        (t/is (= 2 (cached-fn 1 1)))
        (t/is (= 1 @n-calls))
        (t/is (= 1 (maint/count-entries conn)))
        (assert-state-change! nil)

        (t/is (= 2 (cached-fn 1 1)))
        (t/is (= 1 @n-calls))
        (assert-state-change! {:hits 1 :last-hit tu/clock-start})

        (advance-clock! (+ c/default-max-age c/one-day))
        (t/is (assert-state-change! {:cold? true :stale? true}))

        (t/is (= 2 (cached-fn 1 1)))
        (t/is (= 2 @n-calls))
        (assert-state-change!
         {:created-at (+ tu/clock-start c/default-max-age c/one-day)
          :hits 0
          :last-hit nil
          :cold? false
          :stale? false})))))

(t/deftest repeat-put-test
  ;; Our usual test harness uses a pure function (sum). This is usually fine,
  ;; except that it means that it doesn't really test that the cache upsert also
  ;; updates the result correctly, which is critical for correctness.
  (let [counter (atom 0)
        f! (partial swap! counter inc)]

    (tu/with-harness
      {:f f!}
      (fn [{:keys [cached-fn n-calls advance-clock! assert-n-entries!]}]
        (t/is (= 1 (cached-fn)))
        (t/is (= 1 @n-calls))
        (assert-n-entries! 1)

        (t/testing "get the cached result"
          (t/is (= 1 (cached-fn)))
          (t/is (= 1 @n-calls)))

        (advance-clock! (+ c/default-max-age c/one-day))

        (t/is (= 2 (cached-fn)))
        (t/is (= 2 @n-calls))
        (assert-n-entries! 1)))))

;; # Tests for cache invalidation functionality

(t/deftest delete-cache-entries-test
  (tu/with-harness
    (fn [{:keys [cached-fn conn assert-n-entries!]}]
      ;; Populate cache with some entries
      (t/is (= 1 (cached-fn 1)))  ; add entry
      (t/is (= 3 (cached-fn 3)))  ; add entry
      (t/is (= 6 (cached-fn 2 4))) ; add entry
      (assert-n-entries! 3)

      ;; Delete entries where args contain 1
      (let [deleted-count (maint/delete-cache-entries!
                           conn
                           (fn [entry]
                             (some #{1} (:args entry))))]
        (t/is (= 1 deleted-count))
        (t/is (= 2 (maint/count-entries conn))))

      ;; Delete entries where result is even
      (let [deleted-count (maint/delete-cache-entries!
                           conn
                           (comp even? :result))]
        (t/is (= 1 deleted-count))  ; Should delete the entry with result 6
        (t/is (= 1 (maint/count-entries conn))))

      ;; Verify the remaining entry
      (let [remaining (jdbc/execute! conn ["SELECT * FROM cache"])]
        (t/is (= 1 (count remaining)))))))

(t/deftest delete-cache-entries-maintenance-test
  (t/testing "All delete functions run maintenance and clean up cold/stale entries"
    (tu/with-harness
      (fn [{:keys [cached-fn conn advance-clock!]}]
        ;; Add some entries
        (t/is (= 1 (cached-fn 1)))
        (t/is (= 2 (cached-fn 2)))

        ;; Advance time to make entries cold/stale
        (advance-clock! (+ c/default-max-age c/one-day))

        ;; Add a fresh entry
        (t/is (= 3 (cached-fn 3)))

        ;; Verify we have 3 entries (2 stale + 1 fresh)
        (t/is (= 3 (maint/count-entries conn)))

        ;; Delete using predicate - should trigger maintenance
        (let [deleted-count (maint/delete-cache-entries! conn (fn [entry] (= (:result entry) 3)))]
          (t/is (= 1 deleted-count) "Should delete 1 entry matching predicate")
          (t/is (= 0 (maint/count-entries conn)) "All entries should be gone after maintenance"))

        ;; Test delete-cache-entry! also runs maintenance
        (t/is (= 4 (cached-fn 4))) ; Add fresh entry

        (advance-clock! (+ c/default-max-age c/one-day)) ; Make it stale

        (t/is (= 5 (cached-fn 5))) ; Add another fresh entry

        (t/is (= 2 (maint/count-entries conn))) ; 1 stale + 1 fresh

        ;; Delete specific entry - should trigger maintenance
        ;; Note: args are stored as a list, not a vector
        (let [deleted-count (maint/delete-cache-entry-by-call! conn "my-func" '(5))]
          (t/is (= 1 deleted-count) "Should delete 1 specific entry")
          (t/is (= 0 (maint/count-entries conn)) "All entries should be gone after maintenance"))))))


(t/deftest delete-cache-entries-by-function-test
  (tu/with-harness
    {:f (fn [x] (* x 2))} ; Different function for this test
    (fn [{:keys [cached-fn conn assert-n-entries!]}]
      ;; Add some entries with the multiply function
      (t/is (= 4 (cached-fn 2)))
      (t/is (= 6 (cached-fn 3)))
      (assert-n-entries! 2)

      ;; Delete all entries for this specific function
      (let [deleted-count (maint/delete-cache-entries!
                           conn
                           (fn [entry]
                             ;; This will match our cached function name
                             (= (:function entry) "my-func")))]
        (t/is (pos? deleted-count))  ; Should delete entries
        (t/is (= 0 (maint/count-entries conn)))))))

(t/deftest delete-cache-entries-empty-predicate-test
  (tu/with-harness
    (fn [{:keys [cached-fn conn assert-n-entries!]}]
      ;; Add an entry
      (t/is (= 5 (cached-fn 5)))
      (assert-n-entries! 1)

      ;; Delete with predicate that matches nothing
      (let [deleted-count (maint/delete-cache-entries!
                           conn
                           (fn [_entry] false))]
        (t/is (= 0 deleted-count))
        (t/is (= 1 (maint/count-entries conn))))

      ;; Delete with predicate that matches everything
      (let [deleted-count (maint/delete-cache-entries!
                           conn
                           (fn [_entry] true))]
        (t/is (= 1 deleted-count))
        (t/is (= 0 (maint/count-entries conn)))))))

(t/deftest delete-cache-entries-datetime-test
  (tu/with-harness
    (fn [{:keys [cached-fn conn advance-clock! assert-n-entries!]}]
      ;; Add an entry
      (t/is (= 2 (cached-fn 1 1)))
      (assert-n-entries! 1)

      ;; Access the entry to set :last-hit
      (advance-clock! c/one-hour)
      (t/is (= 2 (cached-fn 1 1)))
      ;; Wait for the async hit update to complete
      (Thread/sleep 50)

      ;; Test that datetime fields are java.time.Instant instances
      (let [the-entry (atom nil)
            deleted-count (maint/delete-cache-entries!
                           conn
                           (fn [entry]
                             ;; Capture the entry for inspection but don't delete
                             (reset! the-entry entry)
                             false))] ; Don't delete anything
        (t/is (= 0 deleted-count) "Should not delete any entries")
        (t/is (= 1 (maint/count-entries conn)) "Entry should still exist")

        ;; Verify the datetime types
        (let [{:keys [created-at last-hit]} @the-entry]
          (t/is (instance? Instant created-at))
          (t/is (instance? Instant last-hit))))

      ;; Test date-based deletion: delete entries created in the future (should be none)
      (let [future-date (-> (Instant/now) (Instant/.plusSeconds (* 24 60 60))) ; 1 day from now
            deleted-count (maint/delete-cache-entries!
                           conn
                           (fn [entry] (-> entry :created-at (Instant/.isAfter future-date))))]
        (t/is (= 0 deleted-count) "Should not delete entries created before future date")
        (t/is (= 1 (maint/count-entries conn))))

      ;; Test date-based deletion: delete entries created in the past (should be all)
      (let [past-date (Instant/ofEpochSecond 0)
            deleted-count (maint/delete-cache-entries!
                           conn
                           (fn [entry] (-> entry :created-at (Instant/.isAfter past-date))))]
        (t/is (= 1 deleted-count) "Should delete entries created after epoch")
        (t/is (= 0 (maint/count-entries conn)))))))

(t/deftest delete-cache-entries-by-function-efficient-test
  (t/testing "Delete all cache entries for a specific function efficiently"
    (tu/with-harness
      (fn [{:keys [cached-fn conn assert-n-entries!]}]
        ;; Add some entries
        (t/is (= 2 (cached-fn 1 1)))
        (t/is (= 3 (cached-fn 3)))
        (t/is (= 6 (cached-fn 2 4)))
        (assert-n-entries! 3)

        ;; Delete all entries for "my-func"
        (let [deleted-count (maint/delete-cache-entries-by-function! conn "my-func")]
          (t/is (= 3 deleted-count) "Should delete all 3 entries")
          (t/is (= 0 (maint/count-entries conn)) "Cache should be empty"))

        ;; Test with non-existent function
        (let [deleted-count (maint/delete-cache-entries-by-function! conn "non-existent")]
          (t/is (= 0 deleted-count) "Should delete 0 entries for non-existent function"))))))

(t/deftest delete-cache-entries-by-function-maintenance-test
  (t/testing "delete-cache-entries-by-function! runs maintenance and cleans cold/stale entries"
    (tu/with-harness
      (fn [{:keys [cached-fn conn advance-clock! assert-n-entries!]}]
        ;; Add entries for our function
        (t/is (= 2 (cached-fn 1 1)))
        (t/is (= 3 (cached-fn 3)))
        (assert-n-entries! 2)

        ;; Advance clock to make existing entries stale
        (advance-clock! (+ c/default-max-age c/one-day))

        ;; Add a new entry at current (future) time
        (t/is (= 5 (cached-fn 5)))
        (assert-n-entries! 3)

        ;; Delete entries by function - this should also run maintenance
        (let [deleted-count (maint/delete-cache-entries-by-function! conn "my-func")]
          (t/is (= 3 deleted-count) "Should delete all entries including stale ones")
          (t/is (= 0 (maint/count-entries conn)) "All entries should be gone after deletion and maintenance"))))))

;;; Delayed Maintenance Tests

(t/deftest basic-delayed-maintenance-test
  (tu/with-harness-and-maintenance
    (fn [{:keys [cached-fn conn assert-n-entries! maintenance-calls]}]
      ;; Create some cache entries
      (cached-fn 1 1)
      (cached-fn 1 2)
      (assert-n-entries! 2)

      (maint/with-delayed-maintenance conn
        ;; These operations would normally trigger maintenance
        (maint/delete-cache-entries-by-function! conn "my-func")
        (maint/delete-cache-entries-by-function! conn "my-func")
        ;; Verify no maintenance has run yet
        (t/is (= @maintenance-calls 0) "No maintenance during delayed block"))

      ;; Verify maintenance ran once at the end
      (t/is (= @maintenance-calls 1) "Maintenance ran once after delayed block"))))

(t/deftest nested-delayed-maintenance-test
  (tu/with-harness-and-maintenance
    (fn [{:keys [conn cached-fn maintenance-calls]}]
      ;; Create some cache entries
      (cached-fn 1 1)
      (cached-fn 1 2)

      (maint/with-delayed-maintenance conn
        (maint/delete-cache-entries-by-function! conn "my-func")
        (t/is (= @maintenance-calls 0) "No maintenance in outer block")

        (maint/with-delayed-maintenance conn
          (maint/delete-cache-entries-by-function! conn "my-func")
          (t/is (= @maintenance-calls 0) "No maintenance in inner block"))

        (t/is (= @maintenance-calls 0) "No maintenance after inner block")
        (maint/delete-cache-entries-by-function! conn "my-func"))

      (t/is (= @maintenance-calls 1) "Maintenance ran once after all blocks"))))

(t/deftest maintain-vs-maintain-now-test
  (tu/with-harness-and-maintenance
    (fn [{:keys [conn cached-fn maintenance-calls]}]
      ;; Create some cache entries
      (cached-fn 1 1)
      (cached-fn 1 2)

      (maint/with-delayed-maintenance conn
        ;; maintain! should be blocked
        (maint/maintain! conn)
        (t/is (= @maintenance-calls 0) "maintain! blocked during delayed maintenance")

        ;; maintain-now! should always run
        (@#'maint/maintain-now! conn)
        (t/is (= @maintenance-calls 1) "maintain-now! always runs"))

      (t/is (= @maintenance-calls 2) "Final maintenance ran after delayed block"))))

(t/deftest delayed-maintenance-exception-handling-test
  (tu/with-harness-and-maintenance
    (fn [{:keys [conn cached-fn maintenance-calls]}]
      ;; Create some cache entries
      (cached-fn 1 1)

      ;; Exception should be propagated but maintenance should still run
      (t/is (thrown? RuntimeException
                     (maint/with-delayed-maintenance conn
                       (maint/delete-cache-entries-by-function! conn "my-func")
                       (throw (RuntimeException. "Test exception")))))

      ;; Maintenance should have run in finally block
      (t/is (= @maintenance-calls 1) "Maintenance ran despite exception"))))

(t/deftest bulk-operations-efficiency-test
  (tu/with-harness-and-maintenance
    (fn [{:keys [conn cached-fn assert-n-entries! maintenance-calls]}]
      ;; Create multiple cache entries
      (doseq [i (range 10)]
        (cached-fn i i))
      (assert-n-entries! 10)

      ;; Without delayed maintenance - each operation triggers maintenance
      ;; Note: args are stored as lists when called with multiple arguments
      (t/is (= 1 (maint/delete-cache-entry-by-call! conn "my-func" '(0 0))))
      (t/is (= 1 (maint/delete-cache-entry-by-call! conn "my-func" '(1 1))))
      (t/is (= 1 (maint/delete-cache-entry-by-call! conn "my-func" '(2 2))))
      (let [calls-without-delay @maintenance-calls]
        (reset! maintenance-calls 0)

        ;; With delayed maintenance - only one maintenance call
        (maint/with-delayed-maintenance conn
          (t/is (= 1 (maint/delete-cache-entry-by-call! conn "my-func" '(3 3))))
          (t/is (= 1 (maint/delete-cache-entry-by-call! conn "my-func" '(4 4))))
          (t/is (= 1 (maint/delete-cache-entry-by-call! conn "my-func" '(5 5)))))

        (let [calls-with-delay @maintenance-calls]
          (t/is (= calls-without-delay 3) "3 maintenance calls without delay")
          (t/is (= calls-with-delay 1) "1 maintenance call with delay"))))))

(t/deftest concurrent-delayed-maintenance-test
  (tu/with-harness-and-maintenance
    (fn [{:keys [conn cached-fn assert-n-entries! maintenance-calls]}]
      ;; Create multiple cache entries
      (doseq [i (range 20)]
        (cached-fn i i))
      (assert-n-entries! 20)

      ;; Launch multiple threads that each use delayed maintenance
      (let [threads (doall
                     (for [i (range 4)]
                       (future
                         (maint/with-delayed-maintenance conn
                           ;; Each thread deletes some entries
                           (doseq [j (range (* i 3) (+ (* i 3) 3))]
                             (when (< j 20)
                               (maint/delete-cache-entry-by-call! conn "my-func" (list j j))))))))]

        ;; Wait for all threads to complete
        (doseq [thread threads] @thread)

        ;; All threads are done, so all delayed maintenance should have completed
        ;; We expect exactly 4 maintenance calls (one per thread)
        (t/is (<= @maintenance-calls 4) "At most 4 maintenance calls (one per thread)")
        (t/is (>= @maintenance-calls 1) "At least 1 maintenance call occurred")))))

(t/deftest delayed-maintenance-delay-count-test
  (tu/with-harness-and-maintenance
    (fn [{:keys [conn cached-fn maintenance-calls]}]
      (cached-fn 1 1)

      ;; Test that maintenance is properly delayed and only runs once at the end
      (maint/with-delayed-maintenance conn
        ;; Call maintain! multiple times - should not run
        (maint/maintain! conn)
        (maint/maintain! conn)
        (t/is (= @maintenance-calls 0) "No maintenance during delayed block")

        ;; Test nested behavior
        (maint/with-delayed-maintenance conn
          (maint/maintain! conn)
          (t/is (= @maintenance-calls 0) "No maintenance during nested block"))

        (maint/maintain! conn)
        (t/is (= @maintenance-calls 0) "Still no maintenance in outer block"))

      ;; Should run exactly once after all blocks
      (t/is (= @maintenance-calls 1) "Maintenance ran exactly once after all blocks"))))

(t/deftest function-error-caching-bug-test
  "Test that demonstrates the critical bug where function exceptions get cached forever.
   When a cached function throws an exception, the put-queue entry should be cleared
   so that subsequent calls can retry and potentially succeed."
  (let [should-fail (atom true)
        error-fn (fn [x]
                   (if @should-fail
                     (throw (ex-info "Function failed" {:x x}))
                     (* x 2)))]

    (tu/with-harness
      {:f error-fn}
      (fn [{:keys [cached-fn]}]
        ;; First call should throw
        (t/is (thrown-with-msg? clojure.lang.ExceptionInfo #"Function failed"
                                (cached-fn 5)))

        ;; Fix the underlying condition
        (reset! should-fail false)

        ;; Second call should succeed with the same arguments
        (t/is (= 10 (cached-fn 5)))))))
