use crate::{Error, Result};
use arrow_array::array::Array;

/// A trait to decode values from an Arrow array.
pub trait Decode: Sized {
    fn decode(array: &dyn Array, index: usize) -> Self;
    fn try_decode(array: &dyn Array, index: usize) -> Result<Self>;
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
impl_decode!(bool, BooleanArray);
impl_decode!(String, StringArray);
impl_decode!(Vec<u8>, BinaryArray);

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::*;

    #[test]
    fn test_decode() {
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
        assert_eq!(String::decode(&StringArray::from(vec!["test".to_string()]), 0), "test");
    }
}
