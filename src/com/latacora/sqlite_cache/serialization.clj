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
   [com.latacora.transit-canon.core :as transit-canon]))

(defn serialize
  "Serializes a Clojure value to a compressed canonical Transit value.

  Uses transit-canon for canonical serialization to ensure maps with the same
  logical data always serialize identically, regardless of construction order.
  Compression is handled by transit-canon (zstd level 3 by default)."
  ^bytes [obj]
  (transit-canon/serialize obj))

(defn deserialize
  "Deserializes a compressed byte sequence representing one canonical Transit-encoded object."
  [^bytes bs]
  (transit-canon/deserialize bs))
