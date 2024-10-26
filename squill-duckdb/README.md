> The `squill` driver for DuckDB

```rust
use squill_core::factory::Factory;

let mut conn = Factory::open("duckdb:///:in-memory:?threads=4&max_memory=2GB");
```

# Supported data types

| DuckDB Data Type           | Arrow             | Binding      | Description                                                                                            |
| -------------------------- | ----------------- | ------------ | ------------------------------------------------------------------------------------------------------ |
| `BOOLEAN`                  | bool              | Bool         | Logical boolean (true/false)                                                                           |
| `TINYINT`                  | int8              | Int8         | Signed 8 bits integer                                                                                  |
| `SMALLINT`                 | int16             | Int16        | signed 16 bits integer                                                                                 |
| `INTEGER`                  | int32             | Int32        | Signed 32 bits integer                                                                                 |
| `BIGINT`                   | int64             | Int64        | Signed 64 bits integer                                                                                 |
| `HUGEINT`                  | decimal128(38, 0) | Int128       | Signed 128 bits integer                                                                                |
| `UTINYINT`                 | unit8             | UInt8        | Unsigned 8 bits integer                                                                                |
| `USMALLINT`                | unit16            | UInt16       | Unsigned 16 bits integer                                                                               |
| `UBIGINT`                  | uint64            | UInt32       | Unsigned 64 bits integer                                                                               |
| `UINTEGER`                 | unit32            | UInt64       | Unsigned 32 bits integer                                                                               |
| `UHUGEINT`                 | uint128           | Unit128 [^1] | Unsigned 128 bits integer                                                                              |
| `REAL`                     | float32           | Float32      | Single precision floating-point number (4 bytes)                                                       |
| `DOUBLE`                   | float64           | Float64      | Double precision floating-point number (8 bytes)                                                       |
| `VARCHAR`                  | string            | String       | variable-length character string                                                                       |
| `BLOB`                     | binary            | Blob         | Variable-length binary data                                                                            |
| `BIT`                      | binary            | n/a [^1]     | String of 1s and 0s                                                                                    |
| `DATE`                     | date32            | Date32       | Calendar date (year, month day)                                                                        |
| `TIMESTAMP`                | date64            | Timestamp    | Combination of time and date                                                                           |
| `TIMESTAMP WITH TIME ZONE` | timestamp         | Timestamp    | Combination of time and date that uses the current time zone                                           |
| `TIME`                     | time32            | Time64       | Time of day (no time zone)                                                                             |
| `DECIMAL(prec, scale)`     | decimal128        | Decimal [^1] | Fixed-precision number with the given width (precision) and scale, defaults to prec = 18 and scale = 3 |
| `INTERVAL`                 | duration          | Interval     | Date / Time delta                                                                                      |
| `UUID`                     | string            | String       | UUID data type                                                                                         |

[^1]:
    This driver is implemented on top of [duckdb-rs](https://github.com/duckdb/duckdb-rs) version 0.10.2 which do not
    support yet parameters bindings for `UHUGEINT`, `BIT` and `DECIMAL(prec, scale)` but a workaround as been
    implemented by binding the string representation of the value. This will typically not work when DuckDB doesn't
    know that the text value should be cast to the proper type (like `SELECT 2 + ?` will generate an error when
    binding a `Unit128`), but in some cases (like `INSERT INTO test(ui128) VALUES (?)` or `... WHERE ui128=?`) the cast
    will make the statement execution work.

## Alternative bindings

- `chrono::NaiveDate` can be used to bind a `DATE`.
- `chrono::NaiveDateTime` and `chrono::DateTime` can be used to bind a `TIMESTAMP`.
- `uuid::Uuid` can be used to bind a `UUID`.
