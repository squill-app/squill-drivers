use rusqlite::types::ToSqlOutput;
use squill_core::values::Value;

/// Converting a `Value` to a `duckdb::ToSql`.
///
/// Using an adapter is necessary to work around
/// [E0117](https://doc.rust-lang.org/error_codes/E0117.html) that prevents
/// implementing `rusqlite::ToSql` for `Value` directly.
pub(crate) struct Adapter<'a>(pub &'a Value);

impl<'a> rusqlite::ToSql for Adapter<'a> {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        match &self.0 {
            Value::Null => Ok(ToSqlOutput::Owned(rusqlite::types::Value::Null)),
            Value::Bool(value) => Ok(ToSqlOutput::Owned(rusqlite::types::Value::Integer(*value as i64))),
            Value::Int8(value) => Ok(ToSqlOutput::Owned(rusqlite::types::Value::Integer(*value as i64))),
            Value::Int16(value) => Ok(ToSqlOutput::Owned(rusqlite::types::Value::Integer(*value as i64))),
            Value::Int32(value) => Ok(ToSqlOutput::Owned(rusqlite::types::Value::Integer(*value as i64))),
            Value::Int64(value) => Ok(ToSqlOutput::Owned(rusqlite::types::Value::Integer(*value))),
            Value::Int128(_) => Err(rusqlite::Error::ToSqlConversionFailure("128 bit integers not supported".into())),
            Value::UInt8(value) => Ok(ToSqlOutput::Owned(rusqlite::types::Value::Integer(*value as i64))),
            Value::UInt16(value) => Ok(ToSqlOutput::Owned(rusqlite::types::Value::Integer(*value as i64))),
            Value::UInt32(value) => Ok(ToSqlOutput::Owned(rusqlite::types::Value::Integer(*value as i64))),
            Value::UInt64(_) => {
                Err(rusqlite::Error::ToSqlConversionFailure("Unsigned 64 bit integers not supported".into()))
            }
            Value::UInt128(_) => {
                Err(rusqlite::Error::ToSqlConversionFailure("Unsigned 128 bit integers not supported".into()))
            }
            Value::Float32(value) => Ok(ToSqlOutput::Owned(rusqlite::types::Value::Real(*value as f64))),
            Value::Float64(value) => Ok(ToSqlOutput::Owned(rusqlite::types::Value::Real(*value))),
            Value::String(value) => Ok(ToSqlOutput::Owned(rusqlite::types::Value::Text(value.clone()))),
            _ => Err(rusqlite::Error::ToSqlConversionFailure("Unsupported value type".into())),
        }
    }
}
