use squill_core::parameters::Parameter;

/// Converting a `Parameter` to a `duckdb::ToSql`.
/// 
/// Using an adapter is necessary to work around 
/// [E0117](https://doc.rust-lang.org/error_codes/E0117.html) that prevents
/// implementing `duckdb::ToSql` for `Parameter` directly.
pub(crate) struct Adapter<'a>(pub &'a Parameter);

impl<'a> duckdb::ToSql for Adapter<'a> {
    fn to_sql(&self) -> std::result::Result<duckdb::types::ToSqlOutput<'_>, duckdb::Error> {
        match &self.0 {
            Parameter::Null => Ok(duckdb::types::ToSqlOutput::Owned(duckdb::types::Value::Null)),
            Parameter::Bool(value) => Ok(duckdb::types::ToSqlOutput::Owned(duckdb::types::Value::Boolean(*value))),
            Parameter::Int8(value) => Ok(duckdb::types::ToSqlOutput::Owned(duckdb::types::Value::TinyInt(*value))),
            Parameter::Int16(value) => Ok(duckdb::types::ToSqlOutput::Owned(duckdb::types::Value::SmallInt(*value))),
            Parameter::Int32(value) => Ok(duckdb::types::ToSqlOutput::Owned(duckdb::types::Value::Int(*value))),
            Parameter::Int64(value) => Ok(duckdb::types::ToSqlOutput::Owned(duckdb::types::Value::BigInt(*value))),
            Parameter::UInt8(value) => Ok(duckdb::types::ToSqlOutput::Owned(duckdb::types::Value::UTinyInt(*value))),
            Parameter::UInt16(value) => Ok(duckdb::types::ToSqlOutput::Owned(duckdb::types::Value::USmallInt(*value))),
            Parameter::UInt32(value) => Ok(duckdb::types::ToSqlOutput::Owned(duckdb::types::Value::UInt(*value))),
            Parameter::UInt64(value) => Ok(duckdb::types::ToSqlOutput::Owned(duckdb::types::Value::UBigInt(*value))),
            Parameter::Float32(value) => Ok(duckdb::types::ToSqlOutput::Owned(duckdb::types::Value::Float(*value))),
            Parameter::Float64(value) => Ok(duckdb::types::ToSqlOutput::Owned(duckdb::types::Value::Double(*value))),
            Parameter::String(value) =>
                Ok(duckdb::types::ToSqlOutput::Owned(duckdb::types::Value::Text(value.clone()))),
        }
    }
}
