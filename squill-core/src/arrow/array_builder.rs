use arrow_array::builder::ArrayBuilder;

pub trait ArrayBuilderAppender<T> {
    fn append_value(&mut self, value: Option<T>);
}

macro_rules! impl_array_builder_appender {
    ($data_type:ty, $builder_type:ty) => {
        impl ArrayBuilderAppender<$data_type> for dyn ArrayBuilder {
            fn append_value(&mut self, value: Option<$data_type>) {
                let builder = self.as_any_mut().downcast_mut::<$builder_type>().unwrap();
                match value {
                    Some(value) => builder.append_value(value),
                    None => builder.append_null(),
                }
            }
        }
    };
}

impl_array_builder_appender!(bool, arrow_array::builder::BooleanBuilder);
impl_array_builder_appender!(i16, arrow_array::builder::Int16Builder);
impl_array_builder_appender!(i32, arrow_array::builder::Int32Builder);
impl_array_builder_appender!(i64, arrow_array::builder::Int64Builder);
impl_array_builder_appender!(f32, arrow_array::builder::Float32Builder);
impl_array_builder_appender!(f64, arrow_array::builder::Float64Builder);
impl_array_builder_appender!(String, arrow_array::builder::StringBuilder);
