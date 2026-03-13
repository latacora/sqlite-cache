# sqlite-cache limitations

## High concurrency write contention

### Problem

sqlite-cache was originally designed for single-threaded or low-concurrency use cases. Under high concurrency (15+ concurrent writers), you may encounter `SQLITE_BUSY_SNAPSHOT` errors due to multiple threads attempting to write cache entries simultaneously.

This is particularly problematic during **cache seeding** scenarios where:

- All concurrent threads are cache-missing and writing new entries
- No existing cache hits reduce write pressure
- Large serialized values (e.g., LLM responses, API payloads) create longer transactions
- High concurrency amplifies lock contention

### Root cause

- SQLite supports only one writer at a time, regardless of the number of connections
- The `put!` operation uses `INSERT ... ON CONFLICT DO UPDATE` which requires exclusive locks
- Multiple threads calling cached functions simultaneously can create lock contention
- WAL mode helps with read concurrency but doesn't eliminate write serialization issues
- Large transactions hold locks longer, increasing contention window

### Current solutions

sqlite-cache implements two complementary approaches to handle write contention:

#### 1. Busy timeout pragma

Sets `PRAGMA busy_timeout=5000` (5 seconds) to allow threads to wait for locks rather than failing immediately. This resolves most contention issues for typical use cases where cache writes complete quickly.

#### 2. Single-writer queue per database

The cache now implements a serialized writer queue that completely eliminates write contention:

1. When a cache miss occurs, create a `delay` that computes the result
2. Return the computed result immediately to the caller
3. Queue the write operation to a dedicated writer thread via core.async channel
4. Each database connection gets its own single-writer go-loop that processes writes sequentially in FIFO order
5. Future cache hits are served from the `put-queue` delay until the async write completes
6. Multiple threads can safely call cached functions without blocking

This ensures:

- Only one thread ever writes to SQLite per database
- No blocking on cache operations - results returned immediately
- Correct cache semantics during pending writes via the put-queue
- Complete elimination of `SQLITE_BUSY_SNAPSHOT` errors by serializing ALL writes (cache entries and hit counters)
- Maintains all existing API compatibility with zero performance impact

### Error Types and Detection

#### SQLITE_BUSY vs SQLITE_BUSY_SNAPSHOT

Both are lock contention errors, but occurring at different points in transaction lifecycle:

**SQLITE_BUSY (Write Lock Contention)**

- Cause: Thread trying to START a write transaction when another thread is already writing
- Timing: "I want to write but someone else is already writing"
- Solution: `PRAGMA busy_timeout` allows waiting for the other writer to finish
- Handled by: Our 5-second busy timeout configuration

**SQLITE_BUSY_SNAPSHOT (Read Snapshot Invalidation)**

- Cause: Thread trying to UPGRADE from read to write, but read snapshot is now invalid
- Timing: Thread A reads → Thread B writes → Thread A tries to write → SNAPSHOT error
- Key insight: Your original read snapshot became invalid while you were using it
- Solution: Avoid read-then-write patterns, serialize all writes from the start
- Not helped by: `busy_timeout` (can't wait - must restart with fresh snapshot)

#### Watch for these error patterns:

- `SQLITE_BUSY_SNAPSHOT: Another database connection has already written to the database`
- `SQLITE_BUSY: database is locked` errors under high load
- Performance degradation with increased concurrency
- Large WAL files (indicate snapshot isolation pressure)

### Monitoring

Add logging around cache operations to track:

- Cache hit/miss ratios under load
- Write timeout frequencies
- Average cache operation latencies
