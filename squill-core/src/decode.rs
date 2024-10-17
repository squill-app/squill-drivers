use crate::{Error, Result};
use arrow_array::array::Array;
use arrow_schema::{DataType, TimeUnit};
use chrono::{DateTime, Utc};

/// A trait to decode values from an Arrow array.
pub trait Decode: Sized {
    fn decode(array: &dyn Array, index: usize) -> Self;
    fn try_decode(array: &dyn Array, index: usize) -> Result<Self>;
}

/// Returns whether the value at the given index is null.
///
/// This is a helper function to work around the surprising behavior `is_null` method in the Arrow `Array` trait which
/// will always return `false` for a [arrow_array::NullArray].
pub fn is_null(array: &dyn Array, index: usize) -> bool {
    if array.is_null(index) {
        true
    } else {
        array.as_any().downcast_ref::<arrow_array::NullArray>().is_some()
    }
}

macro_rules! impl_decode {
    ($type:ty, $array_type:ident) => {
        impl Decode for $type {
            fn decode(array: &dyn Array, index: usize) -> Self {
                array.as_any().downcast_ref::<arrow_array::$array_type>().unwrap().value(index).into()
            }
            fn try_decode(array: &dyn Array, index: usize) -> Result<Self> {
                if index >= array.len() {
                    return Err(Error::OutOfBounds { index });
                }
                match array.as_any().downcast_ref::<arrow_array::$array_type>() {
                    Some(array) => Ok(array.value(index).into()),
                    None => Err(Error::InvalidType {
                        expected: stringify!($array_type).to_string(),
                        actual: array.data_type().to_string(),
                    }),
                }
            }
        }
    };
}

impl_decode!(i8, Int8Array);
impl_decode!(i16, Int16Array);
impl_decode!(i32, Int32Array);
impl_decode!(i64, Int64Array);
impl_decode!(i128, Decimal128Array);
impl_decode!(u8, UInt8Array);
impl_decode!(u16, UInt16Array);
impl_decode!(u32, UInt32Array);
impl_decode!(u64, UInt64Array);
impl_decode!(f32, Float32Array);
impl_decode!(f64, Float64Array);
impl_decode!(String, StringArray);
impl_decode!(Vec<u8>, BinaryArray);

/// Decoding a boolean from a {{arrow_array::Array}}
///
/// This implementation will try to decode a boolean from a {{arrow_array::BooleanArray}} or an
/// {{arrow_array::Int64Array}} in order to support databases that store booleans as integers such as SQLite.
impl Decode for bool {
    fn decode(array: &dyn Array, index: usize) -> Self {
        match Self::try_decode(array, index) {
            Ok(value) => value,
            Err(_) => panic!("Unable to decode a boolean (index: {})", index),
        }
    }

    fn try_decode(array: &dyn Array, index: usize) -> Result<Self> {
        if index >= array.len() {
            return Err(Error::OutOfBounds { index });
        }
        match array.data_type() {
            DataType::Boolean => Ok(array.as_any().downcast_ref::<arrow_array::BooleanArray>().unwrap().value(index)),
            DataType::Int64 => Ok(array.as_any().downcast_ref::<arrow_array::Int64Array>().unwrap().value(index) != 0),
            _ => Err(Error::InvalidType { expected: "Boolean".to_string(), actual: array.data_type().to_string() }),
        }
    }
}

/// Decoding a UUID from a {{arrow_array::Array}}
impl Decode for uuid::Uuid {
    fn decode(array: &dyn Array, index: usize) -> Self {
        match Self::try_decode(array, index) {
            Ok(uuid) => uuid,
            Err(_) => panic!("Unable to decode a UUID"),
        }
    }

    fn try_decode(array: &dyn Array, index: usize) -> Result<Self> {
        if index >= array.len() {
            return Err(Error::OutOfBounds { index });
        }
        match array.as_any().downcast_ref::<arrow_array::StringArray>() {
            Some(array) => {
                let value = array.value(index);
                uuid::Uuid::parse_str(value).map_err(|e| Error::InternalError { error: e.into() })
            }
            None => {
                Err(Error::InvalidType { expected: "StringArray".to_string(), actual: array.data_type().to_string() })
            }
        }
    }
}

/// Decoding a Decimal from {{arrow_array::Array}}
impl Decode for rust_decimal::Decimal {
    fn decode(array: &dyn Array, index: usize) -> Self {
        match Self::try_decode(array, index) {
            Ok(decimal) => decimal,
            Err(_) => panic!("Unable to decode a Decimal"),
        }
    }

    fn try_decode(array: &dyn Array, index: usize) -> Result<Self> {
        if index >= array.len() {
            return Err(Error::OutOfBounds { index });
        }
        match array.as_any().downcast_ref::<arrow_array::Decimal128Array>() {
            Some(array) => Ok(rust_decimal::Decimal::from_i128_with_scale(array.value(index), array.scale() as u32)),
            None => Err(Error::InvalidType {
                expected: "Decimal128Array".to_string(), // FIXME:
                actual: array.data_type().to_string(),
            }),
        }
    }
}

/// Decoding a DateTime from {{arrow_array::Array}}
impl Decode for chrono::DateTime<chrono::Utc> {
    fn decode(array: &dyn Array, index: usize) -> Self {
        match Self::try_decode(array, index) {
            Ok(datetime) => datetime,
            Err(e) => panic!("Unable to decode DateTime (reason: {:?})", e),
        }
    }

    fn try_decode(array: &dyn Array, index: usize) -> Result<Self> {
        match array.data_type() {
            DataType::Timestamp(TimeUnit::Second, _) => {
                let secs = array.as_any().downcast_ref::<arrow_array::TimestampSecondArray>().unwrap().value(index);
                match chrono::DateTime::<Utc>::from_timestamp(secs, 0) {
                    Some(datetime) => Ok(datetime),
                    None => Err(Error::InternalError { error: format!("Out of range datetime: {}s.", secs).into() }),
                }
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                let ms = array.as_any().downcast_ref::<arrow_array::TimestampMillisecondArray>().unwrap().value(index);
                match chrono::DateTime::<Utc>::from_timestamp_millis(ms) {
                    Some(datetime) => Ok(datetime),
                    None => Err(Error::InternalError { error: format!("Out of range datetime: {}ms.", ms).into() }),
                }
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                let micro =
                    array.as_any().downcast_ref::<arrow_array::TimestampMicrosecondArray>().unwrap().value(index);
                match chrono::DateTime::<Utc>::from_timestamp_micros(micro) {
                    Some(datetime) => Ok(datetime),
                    None => Err(Error::InternalError { error: format!("Out of range datetime: {}μs.", micro).into() }),
                }
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                let nano = array.as_any().downcast_ref::<arrow_array::TimestampNanosecondArray>().unwrap().value(index);
                Ok(chrono::DateTime::<Utc>::from_timestamp_nanos(nano))
            }
            DataType::Int64 => {
                let secs = array.as_any().downcast_ref::<arrow_array::Int64Array>().unwrap().value(index);
                match chrono::DateTime::<Utc>::from_timestamp(secs, 0) {
                    Some(datetime) => Ok(datetime),
                    None => Err(Error::InternalError { error: format!("Out of range datetime: {}s.", secs).into() }),
                }
            }
            DataType::Utf8 => {
                let str = array.as_any().downcast_ref::<arrow_array::StringArray>().unwrap().value(index);
                if str.len() == 19 {
                    // This may be a date such as '2024-09-05 05:04:47'. sqlite returns dates in this format.
                    match chrono::NaiveDateTime::parse_from_str(str, "%Y-%m-%d %H:%M:%S") {
                        Ok(datetime) => Ok(DateTime::from_naive_utc_and_offset(datetime, Utc)),
                        Err(e) => Err(Error::InternalError { error: e.into() }),
                    }
                } else {
                    match chrono::DateTime::parse_from_rfc3339(str) {
                        Ok(datetime) => Ok(datetime.with_timezone(&Utc)),
                        Err(e) => Err(Error::InternalError { error: e.into() }),
                    }
                }
            }
            _ => {
                return Err(Error::InvalidType {
                    expected: "Timestamp".to_string(),
                    actual: array.data_type().to_string(),
                });
            }
        }
    }
}

impl Decode for chrono::NaiveTime {
    fn decode(array: &dyn Array, index: usize) -> Self {
        match Self::try_decode(array, index) {
            Ok(time) => time,
            Err(e) => panic!("Unable to decode NaiveTime (reason: {:?})", e),
        }
    }

    fn try_decode(array: &dyn Array, index: usize) -> Result<Self> {
        match array.as_any().downcast_ref::<arrow_array::Time64MicrosecondArray>() {
            Some(array) => {
                let time_micros = array.value(index);
                let time_secs = time_micros / 1_000_000;
                match chrono::NaiveTime::from_num_seconds_from_midnight_opt(
                    time_secs as u32,
                    (time_micros % 1_000_000) as u32 * 1_000,
                ) {
                    Some(time) => Ok(time),
                    None => Err(Error::InternalError { error: format!("Out of range time: {time_micros}.").into() }),
                }
            }
            None => Err(Error::InvalidType {
                expected: "Time64MicrosecondArray".to_string(), // FIXME:
                actual: array.data_type().to_string(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::*;
    use chrono::Timelike;
    use rust_decimal::Decimal;

    #[test]
    fn test_decode_primitive_types() {
        let int8_array = Int8Array::from(vec![i8::MIN, i8::MAX]);
        assert_eq!(i8::decode(&int8_array, 0), i8::MIN);
        assert_eq!(i8::decode(&int8_array, 1), i8::MAX);
        assert_eq!(i16::decode(&Int16Array::from(vec![i16::MAX]), 0), i16::MAX);
        assert_eq!(i32::decode(&Int32Array::from(vec![i32::MAX]), 0), i32::MAX);
        assert_eq!(i64::decode(&Int64Array::from(vec![i64::MAX]), 0), i64::MAX);
        assert_eq!(i128::decode(&Decimal128Array::from(vec![i128::MAX]), 0), i128::MAX);
        assert_eq!(u8::decode(&UInt8Array::from(vec![u8::MAX]), 0), u8::MAX);
        assert_eq!(u16::decode(&UInt16Array::from(vec![u16::MAX]), 0), u16::MAX);
        assert_eq!(u32::decode(&UInt32Array::from(vec![u32::MAX]), 0), u32::MAX);
        assert_eq!(u64::decode(&UInt64Array::from(vec![u64::MAX]), 0), u64::MAX);
        assert_eq!(f32::decode(&Float32Array::from(vec![f32::MAX]), 0), f32::MAX);
        assert_eq!(f64::decode(&Float64Array::from(vec![f64::MAX]), 0), f64::MAX);
        assert!(bool::decode(&BooleanArray::from(vec![true]), 0));
        assert!(bool::decode(&Int64Array::from(vec![1]), 0));
        assert_eq!(String::decode(&StringArray::from(vec!["test".to_string()]), 0), "test");
    }

    #[test]
    fn test_decode_uuid() {
        assert_eq!(
            uuid::Uuid::decode(&StringArray::from(vec!["5d94967a-ee15-4f60-9677-1a959fab2982"]), 0),
            uuid::Uuid::parse_str("5d94967a-ee15-4f60-9677-1a959fab2982").unwrap()
        );
    }

    #[test]
    fn test_decode_decimal() {
        assert_eq!(
            rust_decimal::Decimal::decode(
                &Decimal128Array::from(vec![1999]).with_precision_and_scale(10, 2).unwrap(),
                0
            ),
            Decimal::from_i128_with_scale(1999, 2)
        );
    }

    #[test]
    fn test_decode_chrono() {
        use chrono::{DateTime, NaiveTime, Utc};

        let datetime = DateTime::parse_from_rfc3339("2024-07-03T08:56:05.001002003Z").unwrap();
        assert_eq!(
            DateTime::<Utc>::decode(&TimestampSecondArray::from(vec![datetime.timestamp()]), 0),
            DateTime::parse_from_rfc3339("2024-07-03T08:56:05Z").unwrap()
        );
        assert_eq!(
            DateTime::<Utc>::decode(&TimestampMillisecondArray::from(vec![datetime.timestamp_millis()]), 0),
            DateTime::parse_from_rfc3339("2024-07-03T08:56:05.001Z").unwrap()
        );
        assert_eq!(
            DateTime::<Utc>::decode(&TimestampMicrosecondArray::from(vec![datetime.timestamp_micros()]), 0),
            DateTime::parse_from_rfc3339("2024-07-03T08:56:05.001002Z").unwrap()
        );
        assert_eq!(
            DateTime::<Utc>::decode(&TimestampNanosecondArray::from(vec![datetime.timestamp_nanos_opt().unwrap()]), 0),
            datetime
        );
        assert_eq!(
            DateTime::<Utc>::decode(&Int64Array::from(vec![datetime.timestamp()]), 0),
            DateTime::parse_from_rfc3339("2024-07-03T08:56:05Z").unwrap()
        );
        assert_eq!(DateTime::<Utc>::decode(&StringArray::from(vec!["2024-07-03T08:56:05.001002003Z"]), 0), datetime);

        let expected_time = NaiveTime::parse_from_str("11:30:00.123456", "%H:%M:%S%.f").unwrap();
        assert_eq!(
            NaiveTime::decode(
                &arrow_array::Time64MicrosecondArray::from(vec![
                    expected_time.num_seconds_from_midnight() as i64 * 1_000_000
                        + expected_time.nanosecond() as i64 / 1_000
                ]),
                0
            ),
            expected_time
        );
    }
}
