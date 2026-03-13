(ns com.latacora.sqlite-cache.serialization-test
  (:require
   [com.latacora.sqlite-cache.serialization :as ser]
   [clojure.string :as str]
   [clojure.test :as t]
   [com.gfredericks.test.chuck.clojure-test :refer [checking]]
   [clojure.test.check.generators :as gen]))

;; Move the serializable-gen from core-test to here since it's really testing serialization
(def serializable-gen
  "Generator for values that can be serialized/deserialized correctly."
  (gen/recursive-gen
   (fn [inner-gen]
     (gen/one-of
      [(gen/list inner-gen)
       (gen/vector inner-gen)
       (gen/set inner-gen)
       (gen/map gen/string inner-gen)]))
   (gen/one-of
    [;; We avoid generating doubles with zero fractional parts (like 1.0, 2.0, etc.)
     ;; because of a serialization round-trip issue involving Transit and JSON canonicalization:
     ;;
     ;; Transit's encoding rules for values (not keys):
     ;; - Integers < 2^53: plain JSON number (no tagging)
     ;; - Doubles: plain JSON number (no tagging)
     ;; - Transit can't distinguish between 1.0 and 1 because JSON can't either
     ;;
     ;; The problem:
     ;; 1. Transit writes both 1.0 (Double) and 1 (Long) as JSON "1.0"
     ;; 2. JSON canonicalization (RFC 8785) converts "1.0" → "1" (shorter form)
     ;; 3. Transit reads "1" back and chooses Long since integers are preferred
     ;; Result: 1.0 (Double) → 1 (Long), breaking equality and failing round-trip tests
     (->>
      (gen/double* {:infinite? false :NaN? false})
      (gen/fmap (fn [x]
                  ;; Limit to range where 0.01 precision is representable
                  (let [bounded (mod x 1e15)]  ; ~2^50, leaves room for 0.01 precision
                    (+ bounded 0.01 (* 0.99 (rand))))))
      ;; This such-that verifies our fmap actually produces non-zero fractional parts
      (gen/such-that (fn [x] (-> x (mod 1) (not= 0.0)))))
     gen/small-integer
     gen/large-integer
     gen/boolean
     gen/keyword
     gen/keyword-ns
     gen/symbol
     gen/symbol-ns])))

(t/deftest serialization-roundtrip-generative-test
  (checking 1000 [x serializable-gen]
    (let [y (-> x ser/serialize ser/deserialize)]
      (t/is (= x y)))))

(t/deftest compression-basic-test
  (t/testing "Test that compression/decompression works for basic data types."
    (let [test-data [{:a 1 :b "hello"}
                     [1 2 3 4 5]
                     {:large-string (str/join (repeat 1000 "abcdef"))}
                     #{"keyword" :key 'symbol}
                     42
                     "simple string"]]
      (doseq [data test-data]
        (let [serialized (ser/serialize data)
              deserialized (ser/deserialize serialized)]
          (t/is (= data deserialized)))))))

(t/deftest compression-ratio-test
  (t/testing "Test that compression provides space savings for large, repetitive data."
    (let [large-data {:entries (repeat 100 {:same "data" :repeated "structure" :numbers (range 50)})}
          serialized (ser/serialize large-data)
          compressed-size (alength serialized)]
      (t/is (< compressed-size 50000) "Compressed data should be reasonably sized")
      (t/is (= large-data (ser/deserialize serialized)) "Roundtrip should preserve data"))))

(t/deftest edge-cases-test
  (t/testing "Serialization handles edge cases correctly"
    (t/are [data] (= data (-> data ser/serialize ser/deserialize))
      nil
      []
      {}
      #{}
      ""
      {:nested {:deeply {:nested {:value 42}}}}
      (vec (range 1000)))))

(t/deftest canonical-map-ordering-test
  (t/testing "Maps with same data serialize identically regardless of construction order"
    ;; This is the bug from issue #1145 - maps constructed in different orders
    ;; should serialize to identical bytes for cache key consistency
    (let [m1 (zipmap [:a :b :c] [1 2 3])
          m2 (zipmap [:c :b :a] [3 2 1])
          m3 (-> {} (assoc :a 1) (assoc :b 2) (assoc :c 3))
          m4 (-> {} (assoc :c 3) (assoc :b 2) (assoc :a 1))]
      (t/is (= m1 m2 m3 m4) "All maps should be logically equal")
      (t/is (java.util.Arrays/equals (ser/serialize m1) (ser/serialize m2))
            "Different construction order should produce same bytes")
      (t/is (java.util.Arrays/equals (ser/serialize m1) (ser/serialize m3))
            "assoc order shouldn't affect serialization")
      (t/is (java.util.Arrays/equals (ser/serialize m3) (ser/serialize m4))
            "Reverse assoc order should produce same bytes"))))

(t/deftest canonical-set-ordering-test
  (t/testing "Sets with same data serialize identically regardless of construction order"
    (let [s1 (into #{} [:a :b :c])
          s2 (into #{} [:c :b :a])
          s3 (conj (conj (conj #{} :a) :b) :c)
          s4 (conj (conj (conj #{} :c) :b) :a)]
      (t/is (= s1 s2 s3 s4) "All sets should be logically equal")
      (t/is (java.util.Arrays/equals (ser/serialize s1) (ser/serialize s2))
            "Different insertion order should produce same bytes")
      (t/is (java.util.Arrays/equals (ser/serialize s1) (ser/serialize s3))
            "conj order shouldn't affect serialization"))))
