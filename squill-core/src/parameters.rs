use crate::values::{ToValue, Value};

pub enum Parameters {
    None,
    Positional(Vec<Value>),
}

pub const NO_PARAMS: Parameters = Parameters::None;

impl Parameters {
    pub fn from_slice(values: &[&dyn ToValue]) -> Self {
        if values.is_empty() {
            Parameters::None
        } else {
            Parameters::Positional(values.iter().map(|v| v.to_value()).collect())
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Parameters::None => true,
            Parameters::Positional(values) => values.is_empty(),
        }
    }

    pub fn get(&self, index: usize) -> Option<&Value> {
        match self {
            Parameters::None => None,
            Parameters::Positional(values) => values.get(index),
        }
    }
}

impl From<&[&dyn ToValue]> for Parameters {
    fn from(values: &[&dyn ToValue]) -> Self {
        Parameters::from_slice(values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parameters() {
        let parameters = Parameters::from_slice(&[
            &false,
            &true,
            &"hello world",
            &"hello world".to_string(),
            &i8::MAX,
            &i16::MAX,
            &i32::MAX,
            &i64::MAX,
            &i128::MAX,
            &u8::MAX,
            &u16::MAX,
            &u32::MAX,
            &u64::MAX,
            &u128::MAX,
            &f32::MAX,
            &f64::MAX,
            &vec![0xde, 0xad, 0xbe, 0xef],
        ]);
        assert_eq!(parameters.get(0), Some(&Value::Bool(false)));
        assert_eq!(parameters.get(1), Some(&Value::Bool(true)));
        assert_eq!(parameters.get(2), Some(&Value::String("hello world".to_string())));
        assert_eq!(parameters.get(3), Some(&Value::String("hello world".to_string())));
        assert_eq!(parameters.get(4), Some(&Value::Int8(i8::MAX)));
        assert_eq!(parameters.get(5), Some(&Value::Int16(i16::MAX)));
        assert_eq!(parameters.get(6), Some(&Value::Int32(i32::MAX)));
        assert_eq!(parameters.get(7), Some(&Value::Int64(i64::MAX)));
        assert_eq!(parameters.get(8), Some(&Value::Int128(i128::MAX)));
        assert_eq!(parameters.get(9), Some(&Value::UInt8(u8::MAX)));
        assert_eq!(parameters.get(10), Some(&Value::UInt16(u16::MAX)));
        assert_eq!(parameters.get(11), Some(&Value::UInt32(u32::MAX)));
        assert_eq!(parameters.get(12), Some(&Value::UInt64(u64::MAX)));
        assert_eq!(parameters.get(13), Some(&Value::UInt128(u128::MAX)));
        assert_eq!(parameters.get(14), Some(&Value::Float32(f32::MAX)));
        assert_eq!(parameters.get(15), Some(&Value::Float64(f64::MAX)));
        assert_eq!(parameters.get(16), Some(&Value::Blob(vec![0xde, 0xad, 0xbe, 0xef])));
        assert!(Parameters::from_slice(&[]).is_empty());
    }
}
