use chrono::{DateTime, Datelike, TimeZone};
use rust_decimal::Decimal;
use std::fmt;
use uuid::Uuid;

// The number of days between the UNIX epoch and the CE epoch.
// Same as `chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap().num_days_from_ce()` but faster...
const UNIX_EPOCH_NUM_DAYS_FROM_CE: i32 = 719163;

#[derive(Debug, PartialEq, Clone)]
pub enum TimeUnit {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

impl TimeUnit {
    pub fn to_nanos(&self, value: i64) -> i64 {
        match self {
            TimeUnit::Second => value * 1_000_000_000,
            TimeUnit::Millisecond => value * 1_000_000,
            TimeUnit::Microsecond => value * 1_000,
            TimeUnit::Nanosecond => value,
        }
    }
}

// See {@link https://arrow.apache.org/docs/python/api/datatypes.html}
#[derive(Debug, Clone)]
pub enum Value {
    Null,
    Bool(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Int128(i128),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    UInt128(u128),
    Float32(f32),
    Float64(f64),
    String(String),
    Blob(Vec<u8>),

    /// A 32-bit date type representing the elapsed time since UNIX epoch in days.
    Date32(i32),

    /// A 64-bit date type representing the elapsed time since UNIX epoch (UTC).
    /// The precision depends on the {TimeUnit} used.
    Timestamp(TimeUnit, i64),

    /// A 64-bit time type representing the elapsed time since midnight in the unit of {TimeUnit}.
    Time64(TimeUnit, i64),

    /// An interval type representing the elapsed time in months, days, and nanoseconds between two timestamps.
    Interval {
        /// months
        months: i32,
        /// days
        days: i32,
        /// nanos
        nanos: i64,
    },

    /// Decimal type with precision and scale and 128-bit width
    Decimal(Decimal),
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Int8(a), Value::Int8(b)) => a == b,
            (Value::Int16(a), Value::Int16(b)) => a == b,
            (Value::Int32(a), Value::Int32(b)) => a == b,
            (Value::Int64(a), Value::Int64(b)) => a == b,
            (Value::Int128(a), Value::Int128(b)) => a == b,
            (Value::UInt8(a), Value::UInt8(b)) => a == b,
            (Value::UInt16(a), Value::UInt16(b)) => a == b,
            (Value::UInt32(a), Value::UInt32(b)) => a == b,
            (Value::UInt64(a), Value::UInt64(b)) => a == b,
            (Value::UInt128(a), Value::UInt128(b)) => a == b,
            (Value::Float32(a), Value::Float32(b)) => a == b,
            (Value::Float64(a), Value::Float64(b)) => a == b,
            (Value::String(a), Value::String(b)) => a == b,
            (Value::Blob(a), Value::Blob(b)) => a == b,
            (Value::Date32(a), Value::Date32(b)) => a == b,
            (Value::Timestamp(unit_a, a), Value::Timestamp(unit_b, b)) => {
                if unit_a == unit_b && a == b {
                    true
                } else {
                    unit_a.to_nanos(*a) == unit_b.to_nanos(*b)
                }
            }
            (Value::Time64(unit_a, a), Value::Time64(unit_b, b)) => {
                if unit_a == unit_b && a == b {
                    true
                } else {
                    unit_a.to_nanos(*a) == unit_b.to_nanos(*b)
                }
            }
            (Value::Interval { months, days, nanos }, Value::Interval { months: m, days: d, nanos: n }) => {
                months == m && days == d && nanos == n
            }
            (Value::Decimal(a), Value::Decimal(b)) => a == b,
            _ => false,
        }
    }
}

impl From<chrono::NaiveDate> for Value {
    #[inline]
    fn from(value: chrono::NaiveDate) -> Self {
        Value::Date32(value.num_days_from_ce() - UNIX_EPOCH_NUM_DAYS_FROM_CE)
    }
}

impl From<chrono::NaiveDateTime> for Value {
    #[inline]
    fn from(value: chrono::NaiveDateTime) -> Self {
        Value::Timestamp(TimeUnit::Microsecond, value.and_utc().timestamp_micros())
    }
}

impl<T: TimeZone> From<DateTime<T>> for Value {
    fn from(value: DateTime<T>) -> Self {
        Value::Timestamp(TimeUnit::Microsecond, value.timestamp_micros())
    }
}

/// Convert a Uuid into a Value::String or Value::Null if the Uuid is nil.
///
/// If you explicitly want to convert an Uuid into a u128, you can use `Uuid::as_u128()` when binding the value.
impl From<Uuid> for Value {
    #[inline]
    fn from(value: Uuid) -> Self {
        if value.is_nil() {
            Value::Null
        } else {
            Value::String(value.to_string())
        }
    }
}

/// Convert an Option<T> into a Value::Null if None, otherwise convert the value into a Value.
impl<T> From<Option<T>> for Value
where
    T: Into<Value>,
{
    #[inline]
    fn from(v: Option<T>) -> Value {
        match v {
            Some(x) => x.into(),
            None => Value::Null,
        }
    }
}

impl<T> From<&T> for Value
where
    T: Into<Value> + Clone,
{
    #[inline]
    fn from(v: &T) -> Value {
        v.clone().into()
    }
}

macro_rules! impl_from_for_value {
    ($t:ty, $variant:ident) => {
        impl From<$t> for Value {
            #[inline]
            fn from(value: $t) -> Self {
                Value::$variant(value.into())
            }
        }
    };
}

impl_from_for_value!(i8, Int8);
impl_from_for_value!(i16, Int16);
impl_from_for_value!(i32, Int32);
impl_from_for_value!(i64, Int64);
impl_from_for_value!(i128, Int128);
impl_from_for_value!(u8, UInt8);
impl_from_for_value!(u16, UInt16);
impl_from_for_value!(u32, UInt32);
impl_from_for_value!(u64, UInt64);
impl_from_for_value!(u128, UInt128);
impl_from_for_value!(bool, Bool);
impl_from_for_value!(f32, Float32);
impl_from_for_value!(f64, Float64);
impl_from_for_value!(String, String);
impl_from_for_value!(&str, String);
impl_from_for_value!(Vec<u8>, Blob);
impl_from_for_value!(Decimal, Decimal);

/// Display implementation for Value.
///
/// This is also use to cast a Value into a string when a driver would not support the Value type. For instance DuckDB
/// RUST crate does not provide a way to bind a `Value::Decimal` but binding a string representation of a decimal is
/// working.
impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "null"),
            Value::Bool(value) => write!(f, "{}", value),
            Value::Int8(value) => write!(f, "{}", value),
            Value::Int16(value) => write!(f, "{}", value),
            Value::Int32(value) => write!(f, "{}", value),
            Value::Int64(value) => write!(f, "{}", value),
            Value::Int128(value) => write!(f, "{}", value),
            Value::UInt8(value) => write!(f, "{}", value),
            Value::UInt16(value) => write!(f, "{}", value),
            Value::UInt32(value) => write!(f, "{}", value),
            Value::UInt64(value) => write!(f, "{}", value),
            Value::UInt128(value) => write!(f, "{}", value),
            Value::Float32(value) => write!(f, "{}", value),
            Value::Float64(value) => write!(f, "{}", value),
            Value::String(value) => write!(f, "{}", value),
            Value::Blob(value) => write!(f, "{:?}", value),

            // Date32
            Value::Date32(value) => write!(f, "{}", value),

            // Timestamp
            Value::Timestamp(TimeUnit::Second, value) => match DateTime::from_timestamp(*value, 0) {
                Some(datetime) => datetime.to_rfc3339_opts(chrono::SecondsFormat::Secs, true).fmt(f),
                None => Err(fmt::Error),
            },
            Value::Timestamp(TimeUnit::Millisecond, value) => match DateTime::from_timestamp_millis(*value) {
                Some(datetime) => datetime.to_rfc3339_opts(chrono::SecondsFormat::Millis, true).fmt(f),
                None => Err(fmt::Error),
            },
            Value::Timestamp(TimeUnit::Microsecond, value) => match DateTime::from_timestamp_micros(*value) {
                Some(datetime) => datetime.to_rfc3339_opts(chrono::SecondsFormat::Micros, true).fmt(f),
                None => Err(fmt::Error),
            },
            Value::Timestamp(TimeUnit::Nanosecond, value) => {
                DateTime::from_timestamp_nanos(*value).to_rfc3339_opts(chrono::SecondsFormat::Nanos, true).fmt(f)
            }

            // Time64
            Value::Time64(TimeUnit::Second, value) => {
                write!(f, "{:02}:{:02}:{:02}", value / 3600, value / 60 % 60, value % 60)
            }
            Value::Time64(TimeUnit::Millisecond, value) => {
                let secs = value / 1000;
                write!(f, "{:02}:{:02}:{:02}.{:03}", secs / 3600, secs / 60 % 60, secs % 60, value % 1000)
            }
            Value::Time64(TimeUnit::Microsecond, value) => {
                let secs = value / 1_000_000;
                write!(f, "{:02}:{:02}:{:02}.{:06}", secs / 3600, secs / 60 % 60, secs % 60, value % 1_000_000)
            }
            Value::Time64(TimeUnit::Nanosecond, value) => {
                let secs = value / 1_000_000_000;
                write!(f, "{:02}:{:02}:{:02}.{:06}", secs / 3600, secs / 60 % 60, secs % 60, value % 1_000_000_000)
            }

            // Interval
            Value::Interval { months, days, nanos } => {
                let mut space_prefix = false;
                fmt_unit(f, ("month", "months"), *months as i64, &mut space_prefix)?;
                fmt_unit(f, ("day", "days"), *days as i64, &mut space_prefix)?;
                if *nanos > 0 {
                    // Splitting the nanoseconds into seconds, milliseconds, microseconds, and nanoseconds
                    let nanos_in_second = 1_000_000_000;
                    let nanos_in_minute = 60 * nanos_in_second;
                    let nanos_in_hour = 60 * nanos_in_minute;
                    let nanos_in_millisecond = 1_000_000;
                    let nanos_in_microsecond = 1_000;

                    let hours = *nanos / nanos_in_hour;
                    let remaining_nanos = *nanos % nanos_in_hour;
                    fmt_unit(f, ("hour", "hours"), hours, &mut space_prefix)?;

                    let minutes = remaining_nanos / nanos_in_minute;
                    let remaining_nanos = remaining_nanos % nanos_in_minute;
                    fmt_unit(f, ("minute", "minutes"), minutes, &mut space_prefix)?;

                    let seconds = remaining_nanos / nanos_in_second;
                    let remaining_nanos = remaining_nanos % nanos_in_second;
                    fmt_unit(f, ("second", "seconds"), seconds, &mut space_prefix)?;

                    let milliseconds = remaining_nanos / nanos_in_millisecond;
                    let remaining_nanos = remaining_nanos % nanos_in_millisecond;
                    fmt_unit(f, ("millisecond", "milliseconds"), milliseconds, &mut space_prefix)?;

                    let microseconds = remaining_nanos / nanos_in_microsecond;
                    let remaining_nanos = remaining_nanos % nanos_in_microsecond;
                    fmt_unit(f, ("microsecond", "microseconds"), microseconds, &mut space_prefix)?;

                    let nanoseconds = remaining_nanos;
                    fmt_unit(f, ("nanosecond", "nanoseconds"), nanoseconds, &mut space_prefix)?;
                }
                Ok(())
            }

            // Decimal
            Value::Decimal(value) => write!(f, "{}", value),
        }
    }
}

pub trait ToValue: Send + Sync {
    fn to_value(&self) -> Value;
}

impl<T> ToValue for T
where
    T: Into<Value> + Clone + Send + Sync,
{
    fn to_value(&self) -> Value {
        self.clone().into()
    }
}

// Helper function to format a unit value with its singular and plural form.
//
// - The value is only printed if it is greater than 0.
// - The space_prefix is used to add a space before the value if it is not the first unit.
//
// Ex: fmt_unit(f, ("hour", "hours"), 1, &mut space_prefix) => "1 hour"
fn fmt_unit(
    f: &mut fmt::Formatter<'_>,
    (singular, plural): (&str, &str),
    value: i64,
    space_prefix: &mut bool,
) -> fmt::Result {
    if value > 0 {
        if *space_prefix {
            write!(f, " ")?;
        }
        write!(f, "{} {}", value, if value == 1 { singular } else { plural })?;
        *space_prefix = true;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn test_trait_display() {
        assert_eq!(Value::Null.to_string(), "null");
        assert_eq!(Value::Bool(true).to_string(), "true");
        assert_eq!(Value::Bool(false).to_string(), "false");
        assert_eq!(Value::Int8(i8::MAX).to_string(), "127");
        assert_eq!(Value::Int16(i16::MAX).to_string(), "32767");
        assert_eq!(Value::Int32(i32::MAX).to_string(), "2147483647");
        assert_eq!(Value::Int64(i64::MAX).to_string(), "9223372036854775807");
        assert_eq!(Value::Int128(i128::MAX).to_string(), "170141183460469231731687303715884105727");
        assert_eq!(Value::UInt8(u8::MAX).to_string(), "255");
        assert_eq!(Value::UInt16(u16::MAX).to_string(), "65535");
        assert_eq!(Value::UInt32(u32::MAX).to_string(), "4294967295");
        assert_eq!(Value::UInt64(u64::MAX).to_string(), "18446744073709551615");
        assert_eq!(Value::UInt128(u128::MAX).to_string(), "340282366920938463463374607431768211455");
        assert_eq!(Value::Float32(f32::MAX).to_string(), "340282350000000000000000000000000000000");
        assert_eq!(Value::Float64(f64::MAX).to_string(), "179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");
        assert_eq!(Value::String("hello world".to_string()).to_string(), "hello world");
        assert_eq!(Value::Blob(vec![0xde, 0xad, 0xbe, 0xef]).to_string(), "[222, 173, 190, 239]");
        assert_eq!(Value::Date32(18628).to_string(), "18628");
        assert_eq!(Value::Timestamp(TimeUnit::Second, 1720070496).to_string(), "2024-07-04T05:21:36Z");
        assert_eq!(Value::Timestamp(TimeUnit::Millisecond, 1720070496101).to_string(), "2024-07-04T05:21:36.101Z");
        assert_eq!(
            Value::Timestamp(TimeUnit::Microsecond, 1720070496101102).to_string(),
            "2024-07-04T05:21:36.101102Z"
        );
        assert_eq!(
            Value::Timestamp(TimeUnit::Nanosecond, 1720070496101102103).to_string(),
            "2024-07-04T05:21:36.101102103Z"
        );
        assert_eq!(Value::Time64(TimeUnit::Second, 13 * 3600 + 20 * 60 + 10).to_string(), "13:20:10");
        assert_eq!(
            Value::Time64(TimeUnit::Millisecond, (13 * 3600 + 20 * 60 + 10) * 1000 + 101).to_string(),
            "13:20:10.101"
        );
        assert_eq!(
            Value::Time64(TimeUnit::Microsecond, (13 * 3600 + 20 * 60 + 10) * 1_000_000 + 101_202).to_string(),
            "13:20:10.101202"
        );
        assert_eq!(
            Value::Time64(TimeUnit::Nanosecond, (13 * 3600 + 20 * 60 + 10) * 1_000_000_000 + 101_202_303).to_string(),
            "13:20:10.101202303"
        );
        assert_eq!(Value::Decimal(Decimal::new(1299, 2)).to_string(), "12.99");

        // INTERVAL
        assert_eq!(Value::Interval { months: 1, days: 1, nanos: 0 }.to_string(), "1 month 1 day");
        assert_eq!(Value::Interval { months: 0, days: 0, nanos: 100_000 }.to_string(), "100 microseconds");
        assert_eq!(
            Value::Interval { months: 12, days: 30, nanos: 72_101_202_303 }.to_string(),
            "12 months 30 days 1 minute 12 seconds 101 milliseconds 202 microseconds 303 nanoseconds"
        );
    }

    #[test]
    fn test_from_for_value() {
        assert_eq!(Value::from(false), Value::Bool(false));
        assert_eq!(Value::from(true), Value::Bool(true));
        assert_eq!(Value::from("hello world"), Value::String("hello world".to_string()));
        assert_eq!(Value::from("hello world".to_string()), Value::String("hello world".to_string()));
        assert_eq!(Value::from(i8::MAX), Value::Int8(i8::MAX));
        assert_eq!(Value::from(i16::MAX), Value::Int16(i16::MAX));
        assert_eq!(Value::from(i32::MAX), Value::Int32(i32::MAX));
        assert_eq!(Value::from(i64::MAX), Value::Int64(i64::MAX));
        assert_eq!(Value::from(i128::MAX), Value::Int128(i128::MAX));
        assert_eq!(Value::from(u8::MAX), Value::UInt8(u8::MAX));
        assert_eq!(Value::from(u16::MAX), Value::UInt16(u16::MAX));
        assert_eq!(Value::from(u32::MAX), Value::UInt32(u32::MAX));
        assert_eq!(Value::from(u64::MAX), Value::UInt64(u64::MAX));
        assert_eq!(Value::from(u128::MAX), Value::UInt128(u128::MAX));
        assert_eq!(Value::from(f32::MAX), Value::Float32(f32::MAX));
        assert_eq!(Value::from(f64::MAX), Value::Float64(f64::MAX));
        assert_eq!(Value::from(vec![0xde, 0xad, 0xbe, 0xef]), Value::Blob(vec![0xde, 0xad, 0xbe, 0xef]));
        assert_eq!(Value::from(chrono::NaiveDate::from_ymd_opt(2021, 1, 1).unwrap()), Value::Date32(18628));
        assert_eq!(
            Value::from(chrono::NaiveDate::from_ymd_opt(2021, 1, 1).unwrap().and_hms_opt(0, 0, 0).unwrap()),
            Value::Timestamp(TimeUnit::Millisecond, 1609459200000)
        );
        assert_eq!(Value::from(Uuid::nil()), Value::Null);
        assert_eq!(
            Value::from(Uuid::from_str("58cb5e1d-5104-49c7-a983-f1dc53c3da84").unwrap()),
            Value::String("58cb5e1d-5104-49c7-a983-f1dc53c3da84".to_string())
        );
    }
}
