;; # sqlite-cache visual cache analysis

^{:nextjournal.clerk/visibility {:code :hide}}
(ns cache-analysis
  {:nextjournal.clerk/toc true}
  (:require
   [nextjournal.clerk :as clerk]
   [next.jdbc :as jdbc]
   [com.latacora.sqlite-cache.maintenance :as maint]
   [com.latacora.sqlite-cache.serialization :as ser]
   [clj-commons.humanize :as humanize])
  (:import [java.time LocalDateTime ZoneId]))

^{::clerk/sync true ::clerk/visibility {:code :hide :result :hide}}
(defonce cache-db-path
  (atom (or (System/getenv "CACHE_DB_PATH") "sqlite-cache.db")))

^{::clerk/visibility {:code :hide :result :hide}}
(def conn (jdbc/get-connection {:dbtype "sqlite" :dbname @cache-db-path}))

;; ## Cache overview

^{:nextjournal.clerk/visibility {:code :hide :result :hide}}
(def cache-summary (maint/cache-summary conn))

^{:nextjournal.clerk/visibility {:code :hide}}
(clerk/html
 [:ul
  [:li "Database path: " @cache-db-path]
  [:li "Total entries: " (humanize/intcomma (:total-entries cache-summary))]
  [:li "Total size: " (humanize/filesize (:total-size cache-summary) :binary false)]
  [:li "Functions cached: " (count (:functions cache-summary))]])

^{:nextjournal.clerk/visibility {:code :hide :result :hide}}
(def cache-entries-base
  "Comprehensive cache data fetched once and enriched with calculated fields"
  (let [query "SELECT function, args, result, hits, created_at, last_hit,
                      ttl, max_age,
                      LENGTH(args) as compressed_args_size,
                      LENGTH(result) as compressed_result_size
               FROM cache"
        results (jdbc/execute! conn [query])
        now (.getEpochSecond (java.time.Instant/now))]
    (map (fn [row]
           (let [args (ser/deserialize (:cache/args row))
                 result (ser/deserialize (:cache/result row))
                 uncompressed-args-size (count (pr-str args))
                 uncompressed-result-size (count (pr-str result))
                 compressed-args (:compressed_args_size row)
                 compressed-result (:compressed_result_size row)]
             {:function (:cache/function row)
              :args args
              :result result
              :hits (:cache/hits row)
              :created-at (:cache/created_at row)
              :last-hit (:cache/last_hit row)
              :ttl (:cache/ttl row)
              :max-age (:cache/max_age row)
              :compressed-args-size compressed-args
              :compressed-result-size compressed-result
              :uncompressed-args-size uncompressed-args-size
              :uncompressed-result-size uncompressed-result-size
              :total-compressed-size (+ compressed-args compressed-result)
              :total-uncompressed-size (+ uncompressed-args-size uncompressed-result-size)
              :created-days-ago (when-let [created (:cache/created_at row)]
                                  (/ (- now created) 86400.0))
              :last-hit-days-ago (when-let [last-hit (:cache/last_hit row)]
                                   (/ (- now last-hit) 86400.0))
              :never-hit? (nil? (:cache/last_hit row))}))
         results)))

;; ## Size distribution by function

^{:nextjournal.clerk/visibility {:code :hide :result :hide}}
(def function-sizes
  "Cache sizes grouped by function"
  (let [by-func (:by-function cache-summary)]
    (map (fn [[func data]]
           {:function func
            :entries (:count data)
            :total-size (:sum-total-size data)
            :total-size-formatted (humanize/filesize (:sum-total-size data) :binary false)
            :avg-size (long (:avg-total-size data))
            :avg-size-formatted (humanize/filesize (long (:avg-total-size data)) :binary false)
            :args-size (long (:avg-args-size data))
            :result-size (long (:avg-result-size data))})
         by-func)))

;; ### Stacked bar chart of cache sizes

;; Shows the relative size of cache entries by function, split by args/results.

^{:nextjournal.clerk/visibility {:code :hide :result :hide}}
(def stacked-size-data
  (mapcat (fn [f]
            (let [args-total (* (:entries f) (:args-size f))
                  results-total (* (:entries f) (:result-size f))]
              [{:function (:function f)
                :type "args"
                :size args-total
                :size-mb (/ args-total 1000000.0)  ; Convert to MB for display
                :size-formatted (humanize/filesize args-total :binary false)
                :total-size (:total-size f)
                :total-size-formatted (:total-size-formatted f)
                :entries (:entries f)}
               {:function (:function f)
                :type "results"
                :size results-total
                :size-mb (/ results-total 1000000.0)  ; Convert to MB for display
                :size-formatted (humanize/filesize results-total :binary false)
                :total-size (:total-size f)
                :total-size-formatted (:total-size-formatted f)
                :entries (:entries f)}]))
          function-sizes))

^{:nextjournal.clerk/visibility {:code :hide}}
(clerk/vl
 {:$schema "https://vega.github.io/schema/vega-lite/v5.json"
  :title "Cache size contribution of functions"
  :data {:values stacked-size-data}
  :mark "bar"
  :encoding {:y
             {:field "function"
              :type "nominal"}
             :x
             {:field "size-mb"
              :type "quantitative"
              :stack true
              :title "Total size (MB)"
              :axis {:format ".1f"}}
             :color
             {:field "type"
              :type "nominal"
              :scale {:domain ["args" "results"]}
              :legend {:title "Type"}}
             :tooltip
             [{:field "function" :type "nominal" :title "Function"}
              {:field "type" :type "nominal" :title "Type"}
              {:field "size-formatted" :type "nominal" :title "Size"}
              {:field "entries" :type "quantitative" :title "Entries" :format ","}
              {:field "total-size-formatted" :type "nominal" :title "Total size"}]}})

;; ## Size distribution analysis

^{:nextjournal.clerk/visibility {:code :hide :result :hide}}
(def quartile-data
  "Quartile statistics for each function"
  (:by-function-quartiles cache-summary))

^{:nextjournal.clerk/visibility {:code :hide :result :hide}}
(def raw-size-data
  "Raw size measurements for each function"
  (mapcat (fn [entry]
            [{:function (:function entry)
              :category "args"
              :value (:compressed-args-size entry)}
             {:function (:function entry)
              :category "result"
              :value (:compressed-result-size entry)}])
          cache-entries-base))

^{:nextjournal.clerk/visibility {:code :hide}}
(clerk/vl
 {:$schema "https://vega.github.io/schema/vega-lite/v5.json"
  :data {:values raw-size-data}
  :mark {:type "boxplot" :extent 1.5}
  :encoding {:y {:field "function" :type "nominal"}
             :x {:field "value"
                 :type "quantitative"
                 :scale {:type "log" :base 10}
                 :title "Size (bytes, log scale)"}
             :color {:field "category"
                     :type "nominal"
                     :scale {:domain ["args" "result"]}
                     :legend {:title "Type"}}
             :yOffset {:field "category"}}
  :title "Stored size contribution of functions"})

;; ## Compression analysis

^{:nextjournal.clerk/visibility {:code :hide :result :hide}}
(defn format-timestamp
  "Formats an Instant timestamp with both absolute and relative time.
   Returns 'Never' if timestamp is nil."
  [instant]
  (if instant
    (let [now (LocalDateTime/now)
          local-dt (LocalDateTime/ofInstant instant (ZoneId/systemDefault))
          relative (humanize/datetime local-dt :now now)]
      (str instant " (" relative ")"))
    "Never"))

;; ### Storage efficiency by function

;; Compares stored size (Transit+zstd compressed) vs EDN representation (pr-str).
;; The stored format uses: Transit → JSON canonicalization → zstd compression.
;; Small data shows negative efficiency due to format overhead (Transit tags, zstd headers),
;; while large repetitive data compresses extremely well (70-95% reduction).

;; Note that just because this appears to show that a substantial number of uses of
;; compression have counterproductive impact, that doesn't mean compression overall is
;; ineffective: it just means that *when* it's effective, it's *highly* effective in
;; absolute terms. Additionally, generally, *when* it's ineffective, while it may
;; substantially increase the size of the item (args or result) being stored, that
;; tends to happen when the item is so small to begin with that the absolute contribution
;; is still minimal (if indeed negative).

^{:nextjournal.clerk/visibility {:code :hide :result :hide}}
(def compression-data
  "Compression ratios for each cache entry"
  (map (fn [entry]
         (let [args-ratio (if (pos? (:uncompressed-args-size entry))
                            (/ (double (:compressed-args-size entry)) (:uncompressed-args-size entry))
                            1.0)
               result-ratio (if (pos? (:uncompressed-result-size entry))
                              (/ (double (:compressed-result-size entry)) (:uncompressed-result-size entry))
                              1.0)]
           {:function (:function entry)
            :args-compression-ratio args-ratio
            :result-compression-ratio result-ratio
            :args-compression-pct (* 100 (- 1 args-ratio))
            :result-compression-pct (* 100 (- 1 result-ratio))
            :total-compression-ratio (if (pos? (:total-uncompressed-size entry))
                                       (/ (double (:total-compressed-size entry))
                                          (:total-uncompressed-size entry))
                                       1.0)}))
       cache-entries-base))

^{:nextjournal.clerk/visibility {:code :hide :result :hide}}
(def compression-boxplot-data
  "Data formatted for compression ratio boxplot"
  (mapcat (fn [entry]
            [{:function (:function entry)
              :type "args"
              :compression-pct (:args-compression-pct entry)}
             {:function (:function entry)
              :type "result"
              :compression-pct (:result-compression-pct entry)}])
          compression-data))

^{:nextjournal.clerk/visibility {:code :hide}}
(clerk/vl
 {:$schema "https://vega.github.io/schema/vega-lite/v5.json"
  :layer [{:data {:values compression-boxplot-data}
           :mark {:type "boxplot" :extent 1.5}
           :encoding {:y {:field "function" :type "nominal"}
                      :x {:field "compression-pct"
                          :type "quantitative"
                          :title "Efficiency %"
                          :scale {:domain [-200 100]}}  ; Allow negative values for overhead
                      :color {:field "type"
                              :type "nominal"
                              :scale {:domain ["args" "result"]}
                              :legend {:title "Type"}}
                      :yOffset {:field "type"}}}
          {:data {:values [{:x 0}]}
           :mark {:type "rule" :strokeWidth 2 :stroke "#888" :strokeDash [5 5]}
           :encoding {:x {:field "x" :type "quantitative"}}}]
  :title {:text "Storage efficiency by function (Transit+zstd vs EDN size)"
          :subtitle "Negative values indicate overhead from format headers, positive values show compression savings"}})

;; ### Absolute compression impact by function
;; Shows total sizes before and after compression to demonstrate actual space savings

^{:nextjournal.clerk/visibility {:code :hide :result :hide}}
(def compression-summary
  "Aggregate compression statistics by function"
  (let [by-function (group-by :function cache-entries-base)]
    (map (fn [[func entries]]
           (let [totals (reduce (fn [acc entry]
                                  {:uncompressed-args (+ (:uncompressed-args acc)
                                                         (:uncompressed-args-size entry))
                                   :uncompressed-results (+ (:uncompressed-results acc)
                                                            (:uncompressed-result-size entry))
                                   :compressed-args (+ (:compressed-args acc)
                                                       (:compressed-args-size entry))
                                   :compressed-results (+ (:compressed-results acc)
                                                          (:compressed-result-size entry))})
                                {:uncompressed-args 0
                                 :uncompressed-results 0
                                 :compressed-args 0
                                 :compressed-results 0}
                                entries)
                 total-uncompressed (+ (:uncompressed-args totals)
                                       (:uncompressed-results totals))
                 total-compressed (+ (:compressed-args totals)
                                     (:compressed-results totals))
                 savings (- total-uncompressed total-compressed)]
             {:function func
              :entries (count entries)
              :uncompressed-args (:uncompressed-args totals)
              :uncompressed-results (:uncompressed-results totals)
              :uncompressed-total total-uncompressed
              :compressed-args (:compressed-args totals)
              :compressed-results (:compressed-results totals)
              :compressed-total total-compressed
              :savings savings
              :efficiency-pct (if (pos? total-uncompressed)
                                (* 100.0 (/ (double savings) total-uncompressed))
                                0.0)}))
         by-function)))

^{:nextjournal.clerk/visibility {:code :hide :result :hide}}
(def compression-viz-data
  "Data formatted for stacked compression comparison"
  (let [sorted-summary (sort-by :uncompressed-total > compression-summary)]
    (mapcat (fn [row]
              [{:function (:function row)
                :state "Uncompressed"
                :type "Arguments"
                :size (:uncompressed-args row)
                :size-mb (/ (:uncompressed-args row) 1000000.0)
                :total-mb (/ (:uncompressed-total row) 1000000.0)
                :savings-mb (/ (:savings row) 1000000.0)
                :efficiency-pct (:efficiency-pct row)}
               {:function (:function row)
                :state "Uncompressed"
                :type "Results"
                :size (:uncompressed-results row)
                :size-mb (/ (:uncompressed-results row) 1000000.0)
                :total-mb (/ (:uncompressed-total row) 1000000.0)
                :savings-mb (/ (:savings row) 1000000.0)
                :efficiency-pct (:efficiency-pct row)}
               {:function (:function row)
                :state "Compressed"
                :type "Arguments"
                :size (:compressed-args row)
                :size-mb (/ (:compressed-args row) 1000000.0)
                :total-mb (/ (:compressed-total row) 1000000.0)
                :savings-mb (/ (:savings row) 1000000.0)
                :efficiency-pct (:efficiency-pct row)}
               {:function (:function row)
                :state "Compressed"
                :type "Results"
                :size (:compressed-results row)
                :size-mb (/ (:compressed-results row) 1000000.0)
                :total-mb (/ (:compressed-total row) 1000000.0)
                :savings-mb (/ (:savings row) 1000000.0)
                :efficiency-pct (:efficiency-pct row)}])
            sorted-summary)))

^{:nextjournal.clerk/visibility {:code :hide}}
(clerk/vl
 {:$schema "https://vega.github.io/schema/vega-lite/v5.json"
  :title "Compression impact by function (in bytes)"
  :data {:values compression-viz-data}
  :mark "bar"
  :encoding {:y {:field "function"
                 :type "nominal"
                 :sort {:field "total-mb" :op "max" :order "descending"}
                 :title "Function"}
             :x {:field "size-mb"
                 :type "quantitative"
                 :stack true
                 :title "Size (MB)"}
             :yOffset {:field "state"
                       :type "nominal"
                       :sort ["Uncompressed" "Compressed"]}
             :color {:field "type"
                     :type "nominal"
                     :scale {:domain ["Arguments" "Results"]}
                     :legend {:title "Type"}}
             :opacity {:field "state"
                       :type "nominal"
                       :scale {:domain ["Uncompressed" "Compressed"]
                               :range [0.6 1.0]}
                       :legend {:title "State"}}
             :tooltip [{:field "function" :type "nominal" :title "Function"}
                       {:field "state" :type "nominal" :title "State"}
                       {:field "type" :type "nominal" :title "Type"}
                       {:field "size-mb" :type "quantitative" :title "Size (MB)" :format ".2f"}
                       {:field "total-mb" :type "quantitative" :title "Total (MB)" :format ".2f"}
                       {:field "savings-mb" :type "quantitative" :title "Savings (MB)" :format ".2f"}
                       {:field "efficiency-pct" :type "quantitative" :title "Efficiency %" :format ".1f"}]}})

;; ## Temporal analysis

;; ### Cache entry age distribution
;; Shows when entries were created and last accessed. "Never accessed again" indicates entries that were populated by the initial cache miss but never subsequently accessed.

^{:nextjournal.clerk/visibility {:code :hide :result :hide}}
(def temporal-data
  "Temporal data for cache entries with y-offset for grouping"
  (mapcat (fn [entry]
            (concat
             (when (:created-days-ago entry)
               [{:function (:function entry)
                 :type "created"
                 :days-ago (:created-days-ago entry)
                 :never-hit (:never-hit? entry)
                 :y-offset -0.3}])  ; Negative offset for created
             (when (:last-hit-days-ago entry)
               [{:function (:function entry)
                 :type "last-hit"
                 :days-ago (:last-hit-days-ago entry)
                 :never-hit false
                 :y-offset 0.3}])))  ; Positive offset for accessed
          (filter :created-at cache-entries-base)))

^{:nextjournal.clerk/visibility {:code :hide :result :hide}}
(def expiry-boundaries
  "TTL and max_age boundaries per function with y-offset"
  (let [distinct-by-func (into {} (map (fn [entry]
                                         [(:function entry)
                                          {:ttl (:ttl entry)
                                           :max-age (:max-age entry)}])
                                       cache-entries-base))]
    (mapcat (fn [[func data]]
              [{:function func
                :ttl-days (/ (:ttl data) 86400.0)
                :y-offset 0.3}  ; TTL aligns with last-hit (accessed)
               {:function func
                :max-age-days (/ (:max-age data) 86400.0)
                :y-offset -0.3}])  ; Max age aligns with created
            distinct-by-func)))

^{:nextjournal.clerk/visibility {:code :hide}}
(clerk/vl
 {:$schema "https://vega.github.io/schema/vega-lite/v5.json"
  :data {:values (let [grouped (group-by #(cond
                                            (and (= (:type %) "created") (:never-hit %)) :never-accessed
                                            (= (:type %) "created") :created
                                            (= (:type %) "last-hit") :last-hit)
                                         temporal-data)
                       ttl-markers (filter #(:ttl-days %) expiry-boundaries)
                       max-age-markers (filter #(:max-age-days %) expiry-boundaries)]
                   (concat
                    (map #(assoc % :marker-type "Created (accessed again)" :y-offset (:y-offset %)) (:created grouped))
                    (map #(assoc % :marker-type "Created (never accessed again)" :y-offset (:y-offset %)) (:never-accessed grouped))
                    (map #(assoc % :marker-type "Last accessed" :y-offset (:y-offset %)) (:last-hit grouped))
                    (map #(assoc % :marker-type "TTL expiry" :days-ago (:ttl-days %) :y-offset (:y-offset %)) ttl-markers)
                    (map #(assoc % :marker-type "Max age expiry" :days-ago (:max-age-days %) :y-offset (:y-offset %)) max-age-markers)))}
  :mark {:type "point" :size 50}
  :encoding {:y {:field "function"
                 :type "nominal"
                 :title "Function"
                 :sort {:order "ascending"}}
             :yOffset {:field "y-offset"
                       :type "quantitative"
                       :scale {:domain [-0.5 0.5]}}
             :x {:field "days-ago"
                 :type "quantitative"
                 :title "Days ago (log scale)"
                 :scale {:type "log" :base 10 :nice true}}
             :shape {:field "marker-type"
                     :type "nominal"
                     :scale {:domain ["Created (accessed again)" "Created (never accessed again)" "Last accessed" "TTL expiry" "Max age expiry"]
                             :range ["circle" "circle" "square" "triangle-down" "triangle-down"]}}
             :color {:field "marker-type"
                     :type "nominal"
                     :scale {:domain ["Created (accessed again)" "Created (never accessed again)" "Last accessed" "TTL expiry" "Max age expiry"]
                             :range ["#9467bd" "#dc2626" "#22c55e" "#22c55e" "#9467bd"]}
                     :legend {:title "Timeline markers" :orient "right"}}
             :fill {:field "marker-type"
                    :type "nominal"
                    :scale {:domain ["Created (accessed again)" "Created (never accessed again)" "Last accessed" "TTL expiry" "Max age expiry"]
                            :range ["#9467bd" "#dc262680" "#22c55e" "#22c55e" "#9467bd"]}
                    :legend nil}
             :opacity {:value 0.8}
             :tooltip [{:field "function" :type "nominal" :title "Function"}
                       {:field "days-ago" :type "quantitative" :title "Days ago" :format ".1f"}
                       {:field "marker-type" :type "nominal" :title "Type"}]}
  :title "Cache entry timeline by function & expiry"})

;; ### Hit count distribution
;; Shows the distribution of cache hit counts across entries to understand usage patterns

^{:nextjournal.clerk/visibility {:code :hide :result :hide}}
(def hit-count-data
  "Hit count data for cache entries"
  (map #(select-keys % [:function :hits]) cache-entries-base))

^{:nextjournal.clerk/visibility {:code :hide}}
(clerk/vl
 {:$schema "https://vega.github.io/schema/vega-lite/v5.json"
  :data {:values hit-count-data}
  :facet {:row {:field "function"
                :type "nominal"
                :title nil
                :header {:labelAngle 0
                         :labelAlign "left"
                         :labelFontSize 12}}}
  :spec {:mark {:type "bar"}
         :encoding {:x {:field "hits"
                        :type "quantitative"
                        :bin {:maxbins 20}
                        :title "Number of hits"}
                    :y {:aggregate "count"
                        :type "quantitative"
                        :title "Cache entries"}
                    :tooltip [{:aggregate "count" :type "quantitative" :title "Entries"}
                              {:field "hits" :bin {:maxbins 20} :title "Hit range"}]}}
  :resolve {:scale {:x "shared"
                    :y "independent"}}
  :title "Distribution of cache hit counts by function"})
