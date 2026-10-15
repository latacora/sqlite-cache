(ns com.latacora.sqlite-cache.refresh-test
  (:require
   [clojure.test :as t]
   [com.latacora.sqlite-cache.core :as c]
   [com.latacora.sqlite-cache.test-utils :as tu])
  (:import (clojure.lang ExceptionInfo)))

(defn- with-refresh-jobs [test-fn]
  (let [jobs (atom [])
        run-next! (fn []
                    (let [[before _] (swap-vals! jobs #(subvec % 1))]
                      @(first before)))]
    (with-redefs [clojure.core/future-call
                  (fn [f]
                    (let [run (bound-fn [] (f))
                          job (delay (run))]
                      (swap! jobs conj job)
                      job))]
      (test-fn jobs run-next!))))

(defn- in-flight [cached-fn & args]
  (let [{:keys [db func-name func args-cache-key]} (meta cached-fn)]
    (get @@#'c/put-queue [db func-name func (args-cache-key args)])))

(t/deftest refresh-option-normalization-test
  (doseq [[option expected] [[nil nil] [1 1] [9 9]
                             [{:before-max-age 1} 9]
                             [{:before-max-age 9} 1]]]
    (tu/with-harness
      {:cache-opts {:max-age 10 :refresh-after option}}
      (fn [{:keys [base-cached-fn]}]
        (t/is (= expected (:refresh-after (meta base-cached-fn))))))))

(t/deftest refresh-boundaries-test
  (doseq [refresh-after [5 {:before-max-age 5}]]
    (with-refresh-jobs
      (fn [jobs run-next!]
        (let [calls (atom 0)]
          (tu/with-harness
            {:cache-opts {:max-age 10 :refresh-after refresh-after}
             :f (fn [& _] (swap! calls inc))}
            (fn [{:keys [cached-fn base-cached-fn advance-clock! sync-write-queue!]}]
              (t/is (= 1 (cached-fn :key)))
              (advance-clock! 4)
              (t/is (= 1 (cached-fn :key)))
              (t/is (empty? @jobs))
              (advance-clock! 1)
              (t/is (= 1 (cached-fn :key)))
              (dotimes [_ 10] (t/is (= 1 (cached-fn :key))))
              (t/is (= 1 @calls) "refresh is queued, not evaluated on the caller")
              (t/is (= 1 (count @jobs)) "one background job per key")
              (run-next!)
              (sync-write-queue!)
              (t/is (= 2 (cached-fn :key)))
              (t/is (= 2 @calls))
              (t/is (false? (:stale? (c/entry-status base-cached-fn :key)))))))))))

(t/deftest expired-read-evaluates-queued-refresh-test
  (with-refresh-jobs
    (fn [jobs run-next!]
      (let [calls (atom 0)]
        (tu/with-harness
          {:cache-opts {:max-age 10 :refresh-after 5}
           :f (fn [& _] (swap! calls inc))}
          (fn [{:keys [cached-fn advance-clock!]}]
            (t/is (= 1 (cached-fn :key)))
            (advance-clock! 5)
            (t/is (= 1 (cached-fn :key)))
            (t/is (= 1 (count @jobs)))
            (advance-clock! 5)
            (t/is (= 2 (cached-fn :key)) "expired caller evaluates the queued shared delay")
            (run-next!)
            (t/is (= 2 @calls) "background job reuses the caller's computation")))))))

(t/deftest expired-read-joins-running-refresh-test
  (let [calls (atom 0)
        started (promise)
        release (promise)]
    (tu/with-harness
      {:cache-opts {:max-age 10 :refresh-after 5}
       :f (fn [& _]
            (let [n (swap! calls inc)]
              (when (= n 2)
                (deliver started true)
                @release)
              n))}
      (fn [{:keys [cached-fn base-cached-fn advance-clock! sync-write-queue!]}]
        (let [waiter (atom nil)]
          (try
            (t/is (= 1 (cached-fn :key)))
            (advance-clock! 5)
            (t/is (= 1 (cached-fn :key)))
            @started
            (let [pending (in-flight base-cached-fn :key)
                  joined (promise)
                  original-computation! @#'c/computation!]
              (advance-clock! 5)
              (with-redefs [c/computation!
                            (fn [& args]
                              (let [result (apply original-computation! args)]
                                (deliver joined (first result))
                                result))]
                (reset! waiter (future (cached-fn :key)))
                (t/is (identical? pending @joined))
                (t/is (not (realized? pending)) "refresh remains blocked until explicitly released")
                (deliver release true)
                (t/is (= 2 @@waiter)))
              (t/is (= 2 @calls)))
            (finally
              (deliver release true)
              (when @waiter @@waiter)
              (sync-write-queue!))))))))

(t/deftest refresh-failure-retains-value-and-retries-test
  (with-refresh-jobs
    (fn [_ run-next!]
      (let [calls (atom 0)]
        (tu/with-harness
          {:cache-opts {:max-age 10 :refresh-after 5}
           :f (fn [& _]
                (let [n (swap! calls inc)]
                  (when (= n 2) (throw (ex-info "Refresh failed" {})))
                  n))}
          (fn [{:keys [cached-fn base-cached-fn advance-clock! sync-write-queue!]}]
            (t/is (= 1 (cached-fn :key)))
            (let [created-at (:created-at (c/entry-status base-cached-fn :key))]
              (advance-clock! 5)
              (t/is (= 1 (cached-fn :key)))
              (run-next!)
              (t/is (= created-at (:created-at (c/entry-status base-cached-fn :key))))
              (t/is (= 1 (cached-fn :key)) "failed refresh leaves the previous value available")
              (run-next!)
              (sync-write-queue!)
              (t/is (= 3 (cached-fn :key)))
              (t/is (= 3 @calls)))))))))

(t/deftest falsey-values-are-cache-hits-test
  (doseq [refresh-after [nil 5]
          value [nil false]]
    (with-refresh-jobs
      (fn [jobs run-next!]
        (tu/with-harness
          {:cache-opts {:max-age 10 :refresh-after refresh-after}
           :f (constantly value)}
          (fn [{:keys [cached-fn n-calls advance-clock! sync-write-queue!]}]
            (t/is (= value (cached-fn :key)))
            (t/is (= value (cached-fn :key)))
            (t/is (= 1 @n-calls))
            (advance-clock! 5)
            (t/is (= value (cached-fn :key)))
            (when refresh-after
              (t/is (= 1 (count @jobs)))
              (run-next!)
              (sync-write-queue!)
              (t/is (= value (cached-fn :key)))
              (t/is (= 2 @n-calls)))))))))

(t/deftest refresh-remains-shared-until-stored-test
  (with-refresh-jobs
    (fn [_ run-next!]
      (let [calls (atom 0)]
        (tu/with-harness
          {:cache-opts {:max-age 10 :refresh-after 5}
           :f (fn [& _] (swap! calls inc))}
          (fn [{:keys [cached-fn base-cached-fn advance-clock! sync-write-queue!]}]
            (t/is (= 1 (cached-fn :key)))
            (let [writes (atom [])
                  queue-write! @#'c/queue-write!]
              (with-redefs [c/queue-write!
                            (fn [op]
                              (if (= (:op-fn op) @#'c/put!)
                                (swap! writes conj op)
                                (queue-write! op)))]
                (advance-clock! 5)
                (t/is (= 1 (cached-fn :key)))
                (let [pending (in-flight base-cached-fn :key)]
                  (run-next!)
                  (dotimes [_ 10] (t/is (= 1 (cached-fn :key))))
                  (t/is (identical? pending (in-flight base-cached-fn :key)))
                  (advance-clock! 5)
                  (t/is (= 2 (cached-fn :key)))
                  (t/is (= 2 @calls))))
              (doseq [op @writes] (queue-write! op))
              (sync-write-queue!)
              (t/is (= 2 (cached-fn :key))))))))))

(t/deftest refresh-shares-transformed-keys-without-blocking-other-keys-test
  (with-refresh-jobs
    (fn [jobs run-next!]
      (let [calls (atom {})]
        (tu/with-harness
          {:cache-opts {:max-age 10 :refresh-after 5 :args-cache-key sort}
           :f (fn [& args]
                (let [key (sort args)
                      n (get (swap! calls update key (fnil inc 0)) key)]
                  {:key key :generation n :print-length *print-length*}))}
          (fn [{:keys [cached-fn base-cached-fn advance-clock! sync-write-queue!]}]
            (t/is (= 1 (:generation (cached-fn 1 2))))
            (advance-clock! 5)
            (binding [*print-length* 7]
              (t/is (= 1 (:generation (cached-fn 2 1)))))
            (t/is (= 1 (:generation (cached-fn 1 2))))
            (t/is (= 1 (count @jobs)))
            (t/is (identical? (in-flight base-cached-fn 1 2) (in-flight base-cached-fn 2 1)))
            (t/is (= 1 (:generation (cached-fn 3 4))) "another key can compute before the refresh runs")
            (run-next!)
            (sync-write-queue!)
            (t/is (= 7 (:print-length (cached-fn 2 1))))
            (t/is (= {'(1 2) 2 '(3 4) 1} @calls))))))))

(t/deftest existing-entries-can-opt-into-refresh-test
  (with-refresh-jobs
    (fn [_ run-next!]
      (let [calls (atom 0)]
        (tu/with-harness
          {:cache-opts {:max-age 10}
           :f (fn [& _] (swap! calls inc))}
          (fn [{:keys [cached-fn base-cached-fn db advance-clock!]}]
            (t/is (= 1 (cached-fn :key)))
            (advance-clock! 5)
            (t/is (= 1 (cached-fn :key)))
            (t/is (= 1 @calls))
            (let [refreshing (c/cache {:db db
                                     :func (:func (meta base-cached-fn))
                                     :func-name "my-func"
                                     :max-age 10
                                     :refresh-after {:before-max-age 5}})]
              (t/is (= 1 (refreshing :key)))
              (run-next!)
              (tu/sync-write-queue! refreshing)
              (t/is (= 2 (cached-fn :key))))))))))

(t/deftest blocking-computation-failure-after-expiry-test
  (let [fail? (atom false)]
    (tu/with-harness
      {:cache-opts {:max-age 10 :refresh-after 5}
       :f (fn [& _]
            (if @fail? (throw (ex-info "Upstream unavailable" {})) :value))}
      (fn [{:keys [cached-fn base-cached-fn advance-clock!]}]
        (t/is (= :value (cached-fn :key)))
        (reset! fail? true)
        (advance-clock! 10)
        (t/is (thrown-with-msg? ExceptionInfo #"Upstream unavailable" (cached-fn :key)))
        (t/is (:stale? (c/entry-status base-cached-fn :key)))
        (reset! fail? false)
        (t/is (= :value (cached-fn :key)))))))

(t/deftest named-caches-do-not-share-refresh-computations-test
  (with-refresh-jobs
    (fn [jobs run-next!]
      (let [calls (atom 0)]
        (tu/with-harness
          {:cache-opts {:max-age 10 :refresh-after 5}
           :f (fn [& _] (swap! calls inc))}
          (fn [{:keys [cached-fn base-cached-fn db advance-clock! sync-write-queue!]}]
            (let [other (c/cache {:db db :func (:func (meta base-cached-fn))
                                 :func-name "other" :max-age 10 :refresh-after 5})]
              (t/is (= 1 (cached-fn :key)))
              (t/is (= 2 (other :key)))
              (tu/sync-write-queue! other)
              (advance-clock! 5)
              (t/is (= 1 (cached-fn :key)))
              (t/is (= 2 (other :key)))
              (t/is (= 2 (count @jobs)) "separate database entries each need a refresh")
              (run-next!)
              (sync-write-queue!)
              (run-next!)
              (tu/sync-write-queue! other)
              (t/is (= 3 (cached-fn :key)))
              (t/is (= 4 (other :key))))))))))
