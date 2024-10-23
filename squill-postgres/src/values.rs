use squill_core::parameters::Parameters;
use squill_core::values::{TimeUnit, Value};
use squill_core::Error;
use std::any::type_name;
use std::fmt::Debug;

/// The number of microseconds since midnight, January 1st, 2000.
const EPOCH_2000_IN_MICRO_SEC: i64 = 946684800 * 1_000_000;

/// The number of days since January 1st, 2000.
const EPOCH_2000_IN_DAYS: i32 = 10957;

fn get_unsupported_data_type_error<T>() -> Error {
    Error::UnsupportedDataType { data_type: type_name::<T>().to_string() }
}

pub struct Adapter<'a>(pub &'a Value);

impl Debug for Adapter<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl<'a> postgres::types::ToSql for Adapter<'a> {
    fn to_sql(
        &self,
        ty: &postgres::types::Type,
        out: &mut bytes::BytesMut,
    ) -> std::result::Result<postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        match &self.0 {
            Value::Null => Ok(postgres::types::IsNull::Yes),
            Value::Bool(value) => value.to_sql(ty, out),
            Value::Int8(value) => value.to_sql(ty, out),
            Value::Int16(value) => value.to_sql(ty, out),
            Value::Int32(value) => value.to_sql(ty, out),
            Value::Int64(value) => value.to_sql(ty, out),
            Value::Int128(_value) => Err(get_unsupported_data_type_error::<i128>().into()),
            Value::UInt8(_value) => Err(get_unsupported_data_type_error::<u8>().into()),
            Value::UInt16(_value) => Err(get_unsupported_data_type_error::<u16>().into()),
            Value::UInt32(_value) => Err(get_unsupported_data_type_error::<u32>().into()),
            Value::UInt64(_value) => Err(get_unsupported_data_type_error::<u64>().into()),
            Value::UInt128(_value) => Err(get_unsupported_data_type_error::<u128>().into()),
            Value::Float32(value) => value.to_sql(ty, out),
            Value::Float64(value) => value.to_sql(ty, out),
            Value::String(value) => value.to_sql(ty, out),
            Value::Blob(value) => value.to_sql(ty, out),
            Value::Date32(value) => {
                // Serializes a `DATE` value.
                // The value should represent the number of days since January 1st, 2000.
                postgres_protocol::types::date_to_sql(EPOCH_2000_IN_DAYS - *value, out);
                Ok(postgres_types::IsNull::No)
            }
            Value::Timestamp(unit, value) => {
                // Serializes a `TIMESTAMP` or `TIMESTAMPTZ` value.
                // The value should represent the number of microseconds since midnight, January 1st, 2000.
                let micro_secs = match unit {
                    TimeUnit::Second => *value * 1_000_000 - EPOCH_2000_IN_MICRO_SEC, // FIXME: This could overflow
                    TimeUnit::Millisecond => *value * 1_000 - EPOCH_2000_IN_MICRO_SEC, // FIXME: This could overflow
                    TimeUnit::Microsecond => *value,
                    TimeUnit::Nanosecond => *value / 1_000 - EPOCH_2000_IN_MICRO_SEC,
                };
                postgres_protocol::types::timestamp_to_sql(micro_secs, out);
                Ok(postgres_types::IsNull::No)
            }
            Value::Time64(unit, value) => {
                // Serializes a `TIME` or `TIMETZ` value.
                // The value should represent the number of microseconds since midnight.
                let micro_secs = match unit {
                    TimeUnit::Second => *value * 1_000_000,  // FIXME: This could overflow
                    TimeUnit::Millisecond => *value * 1_000, // FIXME: This could overflow
                    TimeUnit::Microsecond => *value,
                    TimeUnit::Nanosecond => *value / 1_000,
                };
                postgres_protocol::types::time_to_sql(micro_secs, out);
                Ok(postgres_types::IsNull::No)
            }
            Value::Interval { months, days, nanos } => {
                // Serializes an `INTERVAL` value.
                // The value should represent the number of microseconds.
                todo!(
                    "Interval serialization is not implemented yet: {} months, {} days, {} nanos",
                    months,
                    days,
                    nanos
                );
            }
            Value::Decimal(value) => {
                todo!("Decimal serialization is not implemented yet: {}", value);
            }
        }
    }

    fn accepts(_ty: &postgres::types::Type) -> bool {
        todo!()
    }

    fn to_sql_checked(
        &self,
        ty: &postgres::types::Type,
        out: &mut bytes::BytesMut,
    ) -> std::result::Result<postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        self.to_sql(ty, out)
    }
}

pub struct ParametersIterator<'p> {
    parameters: &'p Option<Parameters>,
    index: usize,
}

impl<'p> ParametersIterator<'p> {
    pub fn new(parameters: &'p Option<Parameters>) -> Self {
        ParametersIterator { parameters, index: 0 }
    }
}

impl<'p> Iterator for ParametersIterator<'p> {
    type Item = Adapter<'p>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.parameters {
            None => None,
            Some(Parameters::Positional(values)) => {
                if self.index < values.len() {
                    let value = &values[self.index];
                    self.index += 1;
                    Some(Adapter(value))
                } else {
                    None
                }
            }
        }
    }
}

impl<'p> ExactSizeIterator for ParametersIterator<'p> {
    fn len(&self) -> usize {
        match self.parameters {
            None => 0,
            Some(Parameters::Positional(values)) => values.len() - self.index,
        }
    }
}
