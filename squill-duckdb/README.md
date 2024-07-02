> The `squill` driver for DuckDB

```rust
use squill_core::connection::Connection;

let conn = Connection::open("duckdb:///:in-memory:?threads=4&max_memory=2GB");
```
