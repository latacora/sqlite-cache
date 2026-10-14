(ns com.latacora.sqlite-cache.refresh-test
  (:require
   [clojure.test :as t]
   [com.latacora.sqlite-cache.core :as c]
   [com.latacora.sqlite-cache.test-utils :as tu]
   [next.jdbc :as jdbc])
  (:import (java.util.concurrent LinkedBlockingQueue ExecutionException)))

(defn- await! [pending]
  (let [pending (if (delay? pending) (future @pending) pending)]
    (try
      (let [result (deref pending 5000 ::timeout)]
        (when (= ::timeout result)
          (throw (ex-info "Timed out waiting for test computation" {})))
        result)
      (catch ExecutionException e
        (throw (.getCause e))))))

(defn- in-flight [cached-fn & args]
  (let [{:keys [db func args-cache-key]} (meta cached-fn)]
    (get @@#'c/put-queue [db func (args-cache-key args)])))

(t/deftest refresh-option-validation-test
  (doseq [option [0 -1 10 11 "5" false {} {:before-max-age 0}
                  {:before-max-age -1} {:before-max-age 10}
                  {:before-max-age "2"} {:before-max-age 2 :extra true}
                  Double/NaN Double/POSITIVE_INFINITY]]
    (with-redefs [jdbc/get-connection (fn [_] (throw (AssertionError. "Opened database before validation")))]
      (t/is (thrown? clojure.lang.ExceptionInfo
                     (c/cache {:max-age 10 :refresh-after option}))))))

(t/deftest refresh-boundaries-and-expired-join-test
  (doseq [refresh-after [5 {:before-max-age 5}]]
    (let [calls (atom 0)
          started (promise)
          release (promise)]
      (tu/with-harness
        {:auto-sync false
         :cache-opts {:max-age 10 :refresh-after refresh-after}
         :f (fn [& _]
              (let [n (swap! calls inc)]
                (when (= n 2)
                  (deliver started true)
                  (await! release))
                n))}
        (fn [{:keys [base-cached-fn advance-clock! sync-write-queue!]}]
          (let [cached base-cached-fn
                waiter (atom nil)]
            (try
              (t/is (= 1 (cached :key)))
              (sync-write-queue!)
              (advance-clock! 4)
              (t/is (= 1 (cached :key)))
              (t/is (= 1 @calls))
              (advance-clock! 1)
              (t/is (= 1 (await! (future (cached :key)))) "threshold hit returns while refresh is blocked")
              (await! started)
              (let [readers (doall (repeatedly 10 #(future (cached :key))))]
                (doseq [reader readers] (t/is (= 1 (await! reader)))))
              (t/is (= 2 @calls) "one refresh per key")
              (advance-clock! 5)
              (reset! waiter (future (cached :key)))
              (t/is (= ::waiting (deref @waiter 100 ::waiting)) "hard-expired hit waits")
              (deliver release true)
              (t/is (= 2 (await! @waiter)))
              (sync-write-queue!)
              (t/is (= 2 (cached :key)))
              (t/is (= 2 @calls))
              (t/is (false? (:stale? (c/entry-status cached :key))) "replacement resets age")
              (finally
                (deliver release true)
                (when @waiter (await! @waiter))
                (when-let [pending (in-flight cached :key)] (await! pending))
                (sync-write-queue!)))))))))

(t/deftest refresh-failure-retains-value-and-retries-test
  (let [calls (atom 0)
        started (promise)
        release (promise)]
    (tu/with-harness
      {:auto-sync false
       :cache-opts {:max-age 10 :refresh-after 5}
       :f (fn [& _]
            (let [n (swap! calls inc)]
              (when (= n 2)
                (deliver started true)
                (await! release)
                (throw (ex-info "Refresh failed" {})))
              n))}
      (fn [{:keys [base-cached-fn advance-clock! sync-write-queue!]}]
        (let [cached base-cached-fn]
          (try
            (t/is (= 1 (cached :key)))
            (sync-write-queue!)
            (let [created-at (:created-at (c/entry-status cached :key))]
              (advance-clock! 5)
              (t/is (= 1 (cached :key)))
              (await! started)
              (let [pending (in-flight cached :key)]
                (deliver release true)
                (t/is (thrown-with-msg? clojure.lang.ExceptionInfo #"Refresh failed" (await! pending))))
              (t/is (= created-at (:created-at (c/entry-status cached :key))) "failure does not extend age")
              (advance-clock! 5)
              (t/is (= 3 (cached :key)) "expired call retries instead of serving failed refresh")
              (sync-write-queue!)
              (t/is (= 3 @calls)))
            (finally
              (deliver release true)
              (sync-write-queue!))))))))

(t/deftest falsey-values-are-cache-hits-test
  (doseq [value [nil false]]
    (tu/with-harness
      {:f (constantly value)}
      (fn [{:keys [cached-fn n-calls]}]
        (t/is (= value (cached-fn :key)))
        (t/is (= value (cached-fn :key)))
        (t/is (= 1 @n-calls))))
    (let [started (promise)
          release (promise)
          calls (atom 0)]
      (tu/with-harness
        {:auto-sync false
         :cache-opts {:max-age 10 :refresh-after 5}
         :f (fn [& _]
              (when (= 2 (swap! calls inc))
                (deliver started true)
                (await! release))
              value)}
        (fn [{:keys [base-cached-fn advance-clock! sync-write-queue!]}]
          (let [cached base-cached-fn]
            (try
              (t/is (= value (cached :key)))
              (sync-write-queue!)
              (advance-clock! 5)
              (t/is (= value (await! (future (cached :key)))))
              (await! started)
              (let [pending (in-flight cached :key)]
                (deliver release true)
                (t/is (= value (await! pending))))
              (sync-write-queue!)
              (t/is (= value (cached :key)))
              (t/is (= 2 @calls))
              (finally
                (deliver release true)
                (when-let [pending (in-flight cached :key)] (await! pending))
                (sync-write-queue!)))))))))

(t/deftest refresh-remains-shared-until-stored-test
  (let [calls (atom 0)
        writer-started (promise)
        writer-release (promise)
        refresh-started (promise)
        refresh-release (promise)]
    (tu/with-harness
      {:auto-sync false
       :cache-opts {:max-age 10 :refresh-after 5}
       :f (fn [& _]
            (let [n (swap! calls inc)]
              (when (= n 2)
                (deliver refresh-started true)
                (await! refresh-release))
              n))}
      (fn [{:keys [base-cached-fn advance-clock! sync-write-queue!]}]
        (let [cached base-cached-fn
              queue (:write-queue (meta cached))]
          (try
            (t/is (= 1 (cached :key)))
            (sync-write-queue!)
            (.put ^LinkedBlockingQueue queue
                  {:op-fn (fn [_] (deliver writer-started true) (await! writer-release))})
            (await! writer-started)
            (advance-clock! 5)
            (t/is (= 1 (cached :key)))
            (await! refresh-started)
            (let [pending (in-flight cached :key)]
              (deliver refresh-release true)
              (t/is (= 2 (await! pending)))
              (dotimes [_ 10] (t/is (= 1 (cached :key))))
              (t/is (identical? pending (in-flight cached :key)))
              (advance-clock! 5)
              (t/is (= 2 (cached :key)) "expired reads use computed result pending storage")
              (t/is (= 2 @calls)))
            (finally
              (deliver refresh-release true)
              (deliver writer-release true)
              (when-let [pending (in-flight cached :key)] (await! pending))
              (sync-write-queue!))))))))

(t/deftest refresh-shares-transformed-keys-without-blocking-other-keys-test
  (let [calls (atom {})
        started (promise)
        release (promise)]
    (tu/with-harness
      {:auto-sync false
       :cache-opts {:max-age 10 :refresh-after 5 :args-cache-key sort}
       :f (fn [& args]
            (let [key (sort args)
                  n (get (swap! calls update key (fnil inc 0)) key)]
              (when (and (= key '(1 2)) (= n 2))
                (deliver started true)
                (await! release))
              {:key key :generation n :print-length *print-length*}))}
      (fn [{:keys [base-cached-fn advance-clock! sync-write-queue!]}]
        (let [cached base-cached-fn]
          (try
            (t/is (= 1 (:generation (cached 1 2))))
            (sync-write-queue!)
            (advance-clock! 5)
            (binding [*print-length* 7]
              (t/is (= 1 (:generation (cached 2 1)))))
            (await! started)
            (t/is (= 1 (:generation (cached 1 2))))
            (t/is (= 1 (:generation (await! (future (cached 3 4))))) "another key can compute during refresh")
            (let [pending (in-flight cached 1 2)]
              (t/is (identical? pending (in-flight cached 2 1)))
              (deliver release true)
              (t/is (= 7 (:print-length (await! pending))) "background refresh conveys caller bindings"))
            (sync-write-queue!)
            (t/is (= 2 (:generation (cached 2 1))))
            (t/is (= {'(1 2) 2 '(3 4) 1} @calls))
            (finally
              (deliver release true)
              (when-let [pending (in-flight cached 1 2)] (await! pending))
              (sync-write-queue!))))))))

(t/deftest existing-entries-can-opt-into-refresh-test
  (let [calls (atom 0)
        started (promise)
        release (promise)
        func (fn [& _]
               (let [n (swap! calls inc)]
                 (when (= n 2)
                   (deliver started true)
                   (await! release))
                 n))]
    (tu/with-harness
      {:f func :cache-opts {:max-age 10}}
      (fn [{:keys [cached-fn base-cached-fn db advance-clock!]}]
        (t/is (= 1 (cached-fn :key)))
        (advance-clock! 5)
        (t/is (= 1 (cached-fn :key)))
        (t/is (= 1 @calls) "omitting refresh-after preserves behavior")
        (let [refreshing (c/cache {:db db
                                 :func (:func (meta base-cached-fn))
                                 :func-name "my-func"
                                 :max-age 10
                                 :refresh-after {:before-max-age 5}})]
          (try
            (t/is (= 1 (refreshing :key)) "reads the existing persisted entry")
            (await! started)
            (let [pending (in-flight refreshing :key)]
              (deliver release true)
              (t/is (= 2 (await! pending))))
            (tu/sync-write-queue! refreshing)
            (t/is (= 2 (cached-fn :key)) "old wrapper sees the replacement")
            (finally
              (deliver release true)
              (when-let [pending (in-flight refreshing :key)] (await! pending))
              (tu/sync-write-queue! refreshing))))))))

(t/deftest blocking-computation-failure-after-expiry-test
  (let [fail? (atom false)]
    (tu/with-harness
      {:cache-opts {:max-age 10 :refresh-after 5}
       :f (fn [& _]
            (if @fail?
              (throw (ex-info "Upstream unavailable" {}))
              :value))}
      (fn [{:keys [cached-fn base-cached-fn advance-clock!]}]
        (t/is (= :value (cached-fn :key)))
        (reset! fail? true)
        (advance-clock! 10)
        (t/is (thrown-with-msg? clojure.lang.ExceptionInfo #"Upstream unavailable" (cached-fn :key)))
        (t/is (:stale? (c/entry-status base-cached-fn :key)))
        (reset! fail? false)
        (t/is (= :value (cached-fn :key)) "failed computation can be retried")))))
