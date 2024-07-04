use squill_core::values::{TimeUnit, Value};

/// Converting a `Value` to a `duckdb::ToSql`.
///
/// Using an adapter is necessary to work around
/// [E0117](https://doc.rust-lang.org/error_codes/E0117.html) that prevents
/// implementing `duckdb::ToSql` for `Value` directly.
pub(crate) struct Adapter<'a>(pub &'a Value);

impl<'a> duckdb::ToSql for Adapter<'a> {
    fn to_sql(&self) -> std::result::Result<duckdb::types::ToSqlOutput<'_>, duckdb::Error> {
        match &self.0 {
            Value::Null => Ok(duckdb::types::ToSqlOutput::Owned(duckdb::types::Value::Null)),
            Value::Bool(value) => Ok(duckdb::types::ToSqlOutput::Owned(duckdb::types::Value::Boolean(*value))),
            Value::Int8(value) => Ok(duckdb::types::ToSqlOutput::Owned(duckdb::types::Value::TinyInt(*value))),
            Value::Int16(value) => Ok(duckdb::types::ToSqlOutput::Owned(duckdb::types::Value::SmallInt(*value))),
            Value::Int32(value) => Ok(duckdb::types::ToSqlOutput::Owned(duckdb::types::Value::Int(*value))),
            Value::Int64(value) => Ok(duckdb::types::ToSqlOutput::Owned(duckdb::types::Value::BigInt(*value))),
            Value::Int128(value) => Ok(duckdb::types::ToSqlOutput::Owned(duckdb::types::Value::HugeInt(*value))),
            Value::UInt8(value) => Ok(duckdb::types::ToSqlOutput::Owned(duckdb::types::Value::UTinyInt(*value))),
            Value::UInt16(value) => Ok(duckdb::types::ToSqlOutput::Owned(duckdb::types::Value::USmallInt(*value))),
            Value::UInt32(value) => Ok(duckdb::types::ToSqlOutput::Owned(duckdb::types::Value::UInt(*value))),
            Value::UInt64(value) => Ok(duckdb::types::ToSqlOutput::Owned(duckdb::types::Value::UBigInt(*value))),
            Value::UInt128(_value) => {
                // As of duckdb-rs 0.10.2, there no duckdb::types::Value::UInt128 but we can use a
                // duckdb::types::Value::Text.
                // This will not work on a binding such as `SELECT 2 + ?` but a when duckdb known the type of the column
                // it will automatically cast the text to the correct type.
                // See // https://github.com/duckdb/duckdb-rs/issues/273
                Ok(duckdb::types::ToSqlOutput::Owned(duckdb::types::Value::Text(self.0.to_string())))
            }
            Value::Float32(value) => Ok(duckdb::types::ToSqlOutput::Owned(duckdb::types::Value::Float(*value))),
            Value::Float64(value) => Ok(duckdb::types::ToSqlOutput::Owned(duckdb::types::Value::Double(*value))),
            Value::String(value) => Ok(duckdb::types::ToSqlOutput::Owned(duckdb::types::Value::Text(value.clone()))),
            Value::Blob(value) => Ok(duckdb::types::ToSqlOutput::Owned(duckdb::types::Value::Blob(value.clone()))),

            // Date32
            Value::Date32(value) => {
                // As of duckdb-rs 0.10.2, there is no support Date32 for binding parameters but we can use a Timestamp.
                Ok(duckdb::types::ToSqlOutput::Owned(duckdb::types::Value::Timestamp(
                    duckdb::types::TimeUnit::Second,
                    *value as i64 * 86400,
                )))
            }

            // Timestamp
            Value::Timestamp(TimeUnit::Second, value) => Ok(duckdb::types::ToSqlOutput::Owned(
                duckdb::types::Value::Timestamp(duckdb::types::TimeUnit::Second, *value),
            )),
            Value::Timestamp(TimeUnit::Millisecond, value) => Ok(duckdb::types::ToSqlOutput::Owned(
                duckdb::types::Value::Timestamp(duckdb::types::TimeUnit::Millisecond, *value),
            )),
            Value::Timestamp(TimeUnit::Microsecond, value) => Ok(duckdb::types::ToSqlOutput::Owned(
                duckdb::types::Value::Timestamp(duckdb::types::TimeUnit::Microsecond, *value),
            )),
            Value::Timestamp(TimeUnit::Nanosecond, value) => Ok(duckdb::types::ToSqlOutput::Owned(
                duckdb::types::Value::Timestamp(duckdb::types::TimeUnit::Nanosecond, *value),
            )),

            // Time64
            // duckdb::types::Value::Time64 is not supported by duckdb-rs 0.10.2 for binding parameters but we can use a
            // duckdb::types::Value::Text.
            Value::Time64(_unit, _value) => {
                Ok(duckdb::types::ToSqlOutput::Owned(duckdb::types::Value::Text(self.0.to_string())))
            }

            // Interval
            Value::Interval { months, days, nanos } => {
                Ok(duckdb::types::ToSqlOutput::Owned(duckdb::types::Value::Interval {
                    months: *months,
                    days: *days,
                    nanos: *nanos,
                }))
            }

            // Decimal
            // duckdb::types::Value::Decimal is not supported by duckdb-rs 0.10.2 for binding parameters but we can use
            // a duckdb::types::Value::Text.
            Value::Decimal(_value) => {
                Ok(duckdb::types::ToSqlOutput::Owned(duckdb::types::Value::Text(self.0.to_string())))
            }
        }
    }
}
