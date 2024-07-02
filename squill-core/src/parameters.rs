// See {@link https://arrow.apache.org/docs/python/api/datatypes.html}
#[derive(PartialEq, Debug)]
pub enum Parameter {
    Null,
    Bool(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Float32(f32),
    Float64(f64),
    String(String),
}

macro_rules! impl_from_for_parameter {
    ($t:ty, $variant:ident) => {
        impl From<$t> for Parameter {
            fn from(value: $t) -> Self {
                Parameter::$variant(value.into())
            }
        }
    };
}

impl_from_for_parameter!(i8, Int8);
impl_from_for_parameter!(i16, Int16);
impl_from_for_parameter!(i32, Int32);
impl_from_for_parameter!(i64, Int64);
impl_from_for_parameter!(u8, UInt8);
impl_from_for_parameter!(u16, UInt16);
impl_from_for_parameter!(u32, UInt32);
impl_from_for_parameter!(u64, UInt64);
impl_from_for_parameter!(bool, Bool);
impl_from_for_parameter!(f32, Float32);
impl_from_for_parameter!(f64, Float64);
impl_from_for_parameter!(String, String);
impl_from_for_parameter!(&str, String);

pub trait ToParameter {
    fn to_parameter(&self) -> Parameter;
}

impl<T> ToParameter for T where T: Into<Parameter> + Clone {
    fn to_parameter(&self) -> Parameter {
        self.clone().into()
    }
}

pub enum Parameters {
    None,
    Positional(Vec<Parameter>),
}

pub const BIND_NONE: Parameters = Parameters::None;

impl Parameters {
    pub fn from_slice(values: &[&dyn ToParameter]) -> Self {
        if values.is_empty() {
            Parameters::None
        } else {
            Parameters::Positional(
                values
                    .iter()
                    .map(|v| v.to_parameter())
                    .collect()
            )
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Parameters::None => true,
            Parameters::Positional(values) => values.is_empty(),
        }
    }

    pub fn get(&self, index: usize) -> Option<&Parameter> {
        match self {
            Parameters::None => None,
            Parameters::Positional(values) => values.get(index),
        }
    }
}

impl From<&[&dyn ToParameter]> for Parameters {
    fn from(values: &[&dyn ToParameter]) -> Self {
        Parameters::from_slice(values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parameter_from() {
        assert_eq!(Parameter::from(false), Parameter::Bool(false));
        assert_eq!(Parameter::from(true), Parameter::Bool(true));
        assert_eq!(Parameter::from("hello world"), Parameter::String("hello world".to_string()));
        assert_eq!(Parameter::from("hello world".to_string()), Parameter::String("hello world".to_string()));
        assert_eq!(Parameter::from(i8::MAX), Parameter::Int8(i8::MAX));
        assert_eq!(Parameter::from(i16::MAX), Parameter::Int16(i16::MAX));
        assert_eq!(Parameter::from(i32::MAX), Parameter::Int32(i32::MAX));
        assert_eq!(Parameter::from(i64::MAX), Parameter::Int64(i64::MAX));
        assert_eq!(Parameter::from(u8::MAX), Parameter::UInt8(u8::MAX));
        assert_eq!(Parameter::from(u16::MAX), Parameter::UInt16(u16::MAX));
        assert_eq!(Parameter::from(u32::MAX), Parameter::UInt32(u32::MAX));
        assert_eq!(Parameter::from(u64::MAX), Parameter::UInt64(u64::MAX));
        assert_eq!(Parameter::from(f32::MAX), Parameter::Float32(f32::MAX));
        assert_eq!(Parameter::from(f64::MAX), Parameter::Float64(f64::MAX));
    }

    #[test]
    fn test_parameters() {
        let parameters = Parameters::from_slice(
            &[
                &false,
                &true,
                &"hello world",
                &"hello world".to_string(),
                &i8::MAX,
                &i16::MAX,
                &i32::MAX,
                &i64::MAX,
                &u8::MAX,
                &u16::MAX,
                &u32::MAX,
                &u64::MAX,
                &f32::MAX,
                &f64::MAX,
            ]
        );
        assert_eq!(parameters.get(0), Some(&Parameter::Bool(false)));
        assert_eq!(parameters.get(1), Some(&Parameter::Bool(true)));
        assert_eq!(parameters.get(2), Some(&Parameter::String("hello world".to_string())));
        assert_eq!(parameters.get(3), Some(&Parameter::String("hello world".to_string())));
        assert_eq!(parameters.get(4), Some(&Parameter::Int8(i8::MAX)));
        assert_eq!(parameters.get(5), Some(&Parameter::Int16(i16::MAX)));
        assert_eq!(parameters.get(6), Some(&Parameter::Int32(i32::MAX)));
        assert_eq!(parameters.get(7), Some(&Parameter::Int64(i64::MAX)));
        assert_eq!(parameters.get(8), Some(&Parameter::UInt8(u8::MAX)));
        assert_eq!(parameters.get(9), Some(&Parameter::UInt16(u16::MAX)));
        assert_eq!(parameters.get(10), Some(&Parameter::UInt32(u32::MAX)));
        assert_eq!(parameters.get(11), Some(&Parameter::UInt64(u64::MAX)));
        assert_eq!(parameters.get(12), Some(&Parameter::Float32(f32::MAX)));
        assert_eq!(parameters.get(13), Some(&Parameter::Float64(f64::MAX)));
        assert!(Parameters::from_slice(&[]).is_empty());
    }
}
