use arrow_array::builder::{
    ArrayBuilder, Date32Builder, IntervalMonthDayNanoBuilder, Time64MicrosecondBuilder, TimestampMicrosecondBuilder,
};
use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int64Builder,
    Int8Builder, StringBuilder, UInt32Builder,
};
use arrow_array::types::IntervalMonthDayNano;

pub trait ArrayBuilderAppender<T> {
    fn append_value(&mut self, value: Option<T>);
}

macro_rules! impl_array_builder_appender {
    ($data_type:ty, $($builder_type:ty),+) => {
        impl ArrayBuilderAppender<$data_type> for dyn ArrayBuilder {
            fn append_value(&mut self, value: Option<$data_type>) {
                $(
                    if let Some(builder) = self.as_any_mut().downcast_mut::<$builder_type>() {
                        match value {
                            Some(value) => builder.append_value(value),
                            None => builder.append_null(),
                        }
                        return;
                    }
                )+
                panic!("Failed to downcast ArrayBuilder to any of the provided types");
            }
        }
    };
}

impl_array_builder_appender!(bool, BooleanBuilder);
impl_array_builder_appender!(i8, Int8Builder);
impl_array_builder_appender!(i16, Int16Builder);
impl_array_builder_appender!(i32, Int32Builder, Date32Builder);
impl_array_builder_appender!(u32, UInt32Builder);
impl_array_builder_appender!(i64, Int64Builder, TimestampMicrosecondBuilder, Time64MicrosecondBuilder);
impl_array_builder_appender!(f32, Float32Builder);
impl_array_builder_appender!(f64, Float64Builder);
impl_array_builder_appender!(String, StringBuilder);
impl_array_builder_appender!(Vec<u8>, BinaryBuilder);
impl_array_builder_appender!(IntervalMonthDayNano, IntervalMonthDayNanoBuilder);
