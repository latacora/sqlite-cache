(ns com.latacora.sqlite-cache.stress-test
  (:require
   [com.latacora.sqlite-cache.test-utils :as tu]
   [clojure.test :as t]))

;; Helper functions for test setup and execution

(defn select-cache
  "Select a cache based on thread ID modulo number of caches"
  [thread-id caches]
  (nth caches (mod thread-id (count caches))))


;; High contention test - many threads hitting the same cache

(t/deftest ^:stress high-contention-test
  (tu/with-stress-harness
    (fn [{:keys [db assert-n-entries!]}]
      (let [call-counter (atom 0)
            n-threads 20
            ops-per-thread 20
            expected-entries (* n-threads ops-per-thread)

            cached-fn (tu/create-test-cache db call-counter "contention-test")]

        ;; Launch all threads simultaneously
        ;; Each thread writes unique keys to force cache misses
        (->>
         (fn []
           (let [thread-name (Thread/.getName (Thread/currentThread))]
             (dotimes [i ops-per-thread]
               (cached-fn (str thread-name "-" i)))))
         (tu/run-parallel! n-threads))

        ;; Function calls should be done immediately
        (t/is (= expected-entries @call-counter)
              "Each operation executed exactly once")

        ;; Cache writes are async - sync with write queue
        (assert-n-entries! cached-fn expected-entries)))))

;; Multiple caches test - different cache instances sharing write queue

(t/deftest ^:stress multiple-caches-same-db-test
  (tu/with-stress-harness
    (fn [{:keys [db assert-n-entries! with-exception-tracking]}]
      (let [call-counter (atom 0)
            n-threads 5
            ops-per-thread 5

            ;; Create multiple cache instances that share the same write queue
            cached-fns (for [i (range 1 4)]
                         (tu/create-test-cache db call-counter (str "cache" i)))]

        ;; Pre-populate each cache
        (doseq [cache-fn cached-fns
                i (range 3)]
          (cache-fn i))
        ;; Sync with all caches since each has its own write queue
        (tu/sync-all-caches! cached-fns)
        (assert-n-entries! (first cached-fns) 9)

        ;; Launch threads that use different caches simultaneously
        (->>
         (with-exception-tracking
           (fn []
             (let [thread-id (Thread/.getId (Thread/currentThread))
                   selected-cache (select-cache thread-id cached-fns)]

               ;; First hit existing entries
               (dotimes [i 3]
                 (selected-cache i))

               ;; Then create new entries
               (dotimes [i ops-per-thread]
                 (let [key (+ 1000 thread-id i)]
                   (selected-cache key))))))
         (tu/run-parallel! n-threads))

        ;; Sync with all cache write queues
        (tu/sync-all-caches! cached-fns)))))

;; Burst pattern test - cache hits while new writes are happening

(t/deftest ^:stress burst-pattern-test
  (tu/with-stress-harness
    (fn [{:keys [db assert-n-entries! with-exception-tracking]}]
      (let [call-counter (atom 0)

            ;; Create cache instances like a real app might
            primary-cache (tu/create-test-cache db call-counter "primary")
            secondary-cache (tu/create-test-cache db call-counter "secondary")]

        ;; Pre-populate primary cache with many more entries
        (dotimes [i 50]  ; Reasonable number of entries to hit
          (primary-cache i))
        (tu/sync-write-queue! primary-cache)
        (assert-n-entries! primary-cache 50)

        ;; Simulate the problematic burst pattern with much higher concurrency
        ;; Hit threads repeatedly access cached entries to create many read transactions
        (let [burst-threads 8  ; High concurrency to stress test
              hit-thread-fn (with-exception-tracking
                              (fn []
                                (dotimes [i 200]  ; Many hits per thread
                                  ;; Cache hit -> queues hit! update
                                  (primary-cache (mod i 50)))))

              ;; Write threads try to create new entries while hits are happening
              write-threads 4  ; Multiple concurrent writers
              write-thread-fn (with-exception-tracking
                                (fn []
                                  ;; Very small delay to let hit threads start
                                  (Thread/sleep 10)

                                  ;; Try to write new entries
                                  (dotimes [i 25]  ; Multiple writes per thread
                                    (secondary-cache (+ 1000 (Thread/.getId (Thread/currentThread)) i)))))]

          ;; Run hit threads and write threads concurrently, then wait for completion
          (doseq [f (concat (doall (repeatedly burst-threads #(future (hit-thread-fn))))
                            (doall (repeatedly write-threads #(future (write-thread-fn)))))]
            @f))

        ;; Sync with both cache write queues to ensure all writes complete
        (tu/sync-all-caches! [primary-cache secondary-cache])))))
