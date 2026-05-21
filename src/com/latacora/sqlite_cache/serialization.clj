(ns com.latacora.sqlite-cache.serialization
  "Serialization and deserialization for cache entries.

  Uses transit-canon for canonical Transit serialization and zstd compression
  for space efficiency.

  ## A note on serialization

  In order for this system to work, you want to canonicalize arguments. That
  way, a cache hit is an exact match, made fast thanks to the database index.
  Without canonicalization, equal arguments might serialize to different byte
  sequences, so you might generate cache misses even though the argument is
  actually in the cache (unless you have some other mechanism to find matches,
  see below).

  At first we used [CBOR] for this, because CBOR has a builtin canonicalization
  mode. This worked fine but had the downside you could only express whatever
  CBOR allows (roughly: JSON plus byte strings). We want to do fancier data
  types like timestamps, keywords, symbols, et cetera. We considered Transit
  next since it essentially solves the problem of \"express Clojure richness in
  fast JSON-compatible data types\". Transit doesn't have canonicalization,
  though, and it doesn't expose the its tagging logic outside of the context of
  a concrete serializer (JSON or msgpack). JSON has a few canonicalization
  schemes including an ostensibly modern and well-supported one in [RFC 8785].

  [RFC 8785]: https://www.rfc-editor.org/rfc/rfc8785

  This _almost_ worked without a hitch, and we found the specific way it didn't
  thanks to generative testing. The JSON canonicalization scheme we're using
  uses ECMAScript float canonicalization rules, which writes `1.0` as `1`: it's
  shorter, and in ECMAScript, where all numbers are IEEE 754 doubles, those two
  are the same anyway. Transit, by contrast, assumes the JSON implementation
  will honor that difference: Clojure, after all, has real integers. This
  problem only happens for plain ints; things like bigint, BigInteger,
  BigDecimal et cetera have sufficient additional tagging information to
  prevent the issue.

  To solve this, we now use [transit-canon] which provides truly canonical
  serialization that handles maps with the same logical data consistently
  (regardless of construction order) and preserves the distinction between
  integers and floats.

  [transit-canon]: https://github.com/latacora/transit-canon

  ## Type fidelity beyond transit-canon's defaults

  transit-canon has built-in handlers for Clojure primitives, collections, and
  numeric types, but not for `java.time.*`, `java.util.regex.Pattern`, or
  `clojure.lang.TaggedLiteral`. We register handlers for these so common
  values round-trip through the cache without any per-call configuration:

  - `java.time.*` — all 15 types covered by [time-literals]. Tags follow
    the time-literals convention (`time/date`, `time/duration`, ...). The
    exception is `Instant`, which writes with the bare tag `\"instant\"`
    (matching pre-existing `com.latacora.formats.transit` data);
    `\"time/instant\"` is also registered as a read-only alias so values
    written by other tooling still deserialize.
  - `Pattern` — tag `\"pattern\"`, rep `[pattern-string flags-int]` to
    match `com.latacora.formats.transit`. Storing the flags separately
    preserves any flags that aren't expressible as inline pattern syntax.
  - `TaggedLiteral` — tag is the literal's own tag name, rep is the form,
    same as `com.latacora.formats.transit`. Write-only: if the reader
    knows the tag (e.g. `\"pattern\"`) it materializes the live value;
    if not, transit returns a `cognitect.transit.TaggedValue` rather
    than re-creating the original `TaggedLiteral`. Use `:handlers` to
    register custom read handlers for any tags you care about.

  Callers can register handlers for additional types — `java.nio.file.Path`,
  `java.util.Locale`, custom records, etc. — via the cache's `:handlers`
  opt; see `com.latacora.sqlite-cache.core/cache`.

  [time-literals]: https://github.com/henryw374/time-literals

  ### Alternative: hashing

  As an alternative to all of this, we could have instead hashed the arguments.
  Equal objects are guaranteed to hash equal, but equal hashes don't imply
  equality. We'd then deserialize all of the arguments in the cache with the
  same hash value. Assuming the hash function is ideal and maps objects
  randomly onto its 32-bit domain, the collision chance can be computed as a
  birthday paradox problem.

  For a chance of 1 in 1 million of a collision in a 32 bit space, you'd need
  ~93 or more elements. So, these collisions would be unlikely but not so
  unlikely we wouldn't ever see them during extensive use. Keep in mind: a
  collision is not actually that bad: it just means we have to deserialize
  2 (or more) arguments to confirm we have a cache hit."
  (:require
   [cognitect.transit :as transit]
   [com.latacora.transit-canon.core :as transit-canon]
   [time-literals.data-readers :as tldr])
  (:import
   (clojure.lang TaggedLiteral)
   (java.time DayOfWeek Duration Instant LocalDate LocalDateTime LocalTime
              Month MonthDay OffsetDateTime OffsetTime Period Year YearMonth
              ZoneId ZonedDateTime)
   (java.util.regex Pattern)))

(defn ^:private str-write-handler
  "Transit write handler that tags via `tag` and writes the value's `toString`."
  [tag]
  (transit/write-handler (constantly tag) str))

(defn ^:private parse-read-handler
  "Transit read handler that builds a value by applying `parse-fn` to the
  string representation."
  [parse-fn]
  (transit/read-handler parse-fn))

(def ^:private pattern-write-handler
  (transit/write-handler
   (constantly "pattern")
   (fn [^Pattern p] [(str p) (Pattern/.flags p)])))

(def ^:private pattern-read-handler
  (transit/read-handler (fn [[s flags]] (Pattern/compile s flags))))

(def ^:private tagged-literal-write-handler
  "Same shape as com.latacora.formats.transit's TaggedLiteralWriteHandler:
  the transit tag is the literal's own tag, the rep is its form. Lets cached
  values carry tagged literals through serialization with no surprises."
  (transit/write-handler
   (fn [^TaggedLiteral tl] (-> tl :tag str))
   (fn [^TaggedLiteral tl] (:form tl))))

(def ^:private java-time-write-handlers
  "Write handlers for the 15 java.time types covered by time-literals.
  Tags follow the time-literals convention; Instant uses the bare \"instant\"
  tag for compatibility with com.latacora.formats.transit."
  {Period         (str-write-handler "time/period")
   LocalDate      (str-write-handler "time/date")
   LocalDateTime  (str-write-handler "time/date-time")
   ZonedDateTime  (str-write-handler "time/zoned-date-time")
   OffsetTime     (str-write-handler "time/offset-time")
   Instant        (str-write-handler "instant")
   OffsetDateTime (str-write-handler "time/offset-date-time")
   ZoneId         (str-write-handler "time/zone")
   DayOfWeek      (str-write-handler "time/day-of-week")
   LocalTime      (str-write-handler "time/time")
   Month          (str-write-handler "time/month")
   MonthDay       (str-write-handler "time/month-day")
   Duration       (str-write-handler "time/duration")
   Year           (str-write-handler "time/year")
   YearMonth      (str-write-handler "time/year-month")})

(def ^:private default-write-handlers
  (assoc java-time-write-handlers
         Pattern       pattern-write-handler
         TaggedLiteral tagged-literal-write-handler))

(def ^:private java-time-read-handlers
  "Read handlers paired with `java-time-write-handlers`. Both \"instant\" and
  \"time/instant\" deserialize to `Instant` so values produced by other
  tooling using the time-literals tag also load."
  {"time/period"           (parse-read-handler tldr/period)
   "time/date"             (parse-read-handler tldr/date)
   "time/date-time"        (parse-read-handler tldr/date-time)
   "time/zoned-date-time"  (parse-read-handler tldr/zoned-date-time)
   "time/offset-time"      (parse-read-handler tldr/offset-time)
   "instant"               (parse-read-handler tldr/instant)
   "time/instant"          (parse-read-handler tldr/instant)
   "time/offset-date-time" (parse-read-handler tldr/offset-date-time)
   "time/zone"             (parse-read-handler tldr/zone)
   "time/day-of-week"      (parse-read-handler tldr/day-of-week)
   "time/time"             (parse-read-handler tldr/time)
   "time/month"            (parse-read-handler tldr/month)
   "time/month-day"        (parse-read-handler tldr/month-day)
   "time/duration"         (parse-read-handler tldr/duration)
   "time/year"             (parse-read-handler tldr/year)
   "time/year-month"       (parse-read-handler tldr/year-month)})

(def ^:private default-read-handlers
  (assoc java-time-read-handlers
         "pattern" pattern-read-handler))

(defn serialize
  "Serializes a Clojure value to a compressed canonical Transit value.

  Uses transit-canon for canonical serialization to ensure maps with the same
  logical data always serialize identically, regardless of construction order.
  Compression is handled by transit-canon (zstd level 3 by default).

  Options:
  - :handlers - map of class -> transit write-handler. Merged on top of the
                built-in handlers for `java.time.*`, `Pattern`, and
                `TaggedLiteral`; user entries override defaults for the same
                class. transit-canon's own canonical handlers (maps, sets,
                integer types) always win."
  (^bytes [obj] (serialize obj nil))
  (^bytes [obj {:keys [handlers]}]
   (transit-canon/serialize obj {:handlers (merge default-write-handlers handlers)})))

(defn deserialize
  "Deserializes a compressed byte sequence representing one canonical Transit-encoded object.

  Options:
  - :handlers - map of tag-string -> transit read-handler. Merged on top of
                the built-in handlers for `java.time.*` and `Pattern`, and
                the transit reader's defaults."
  ([^bytes bs] (deserialize bs nil))
  ([^bytes bs {:keys [handlers]}]
   (transit-canon/deserialize bs {:handlers (merge default-read-handlers handlers)})))
