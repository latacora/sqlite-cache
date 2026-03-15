# Agent Instructions

## Build & Test

- Run tests: `bb test`
- Run linting: `bb lint`
- See `bb.edn` for all available tasks

## Project Overview

Clojure library providing a SQLite-backed cache with automatic expiry, maintenance, and async writes.

- **Namespace**: `com.latacora.sqlite-cache.*`
- **Build tool**: deps.edn (use Babashka `bb` task runner for common tasks)
- **Test runner**: Kaocha (via `bb test`)

## Key Dependencies

- `next.jdbc` / `honeysql` — database access and query building
- `org.xerial/sqlite-jdbc` — SQLite driver
- `io.github.latacora/clj-sqlite-bridge` — SQLite UDFs, listeners, busy/progress handlers
- `io.github.latacora/transit-canon` — canonical serialization
