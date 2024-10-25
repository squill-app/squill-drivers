use crate::errors::into_driver_error;
use crate::values::ParametersIterator;
use crate::DRIVER_NAME;
use arrow_array::builder::ArrayBuilder;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use postgres::fallible_iterator::FallibleIterator;
use squill_core::driver::{DriverConnection, DriverStatement, Result};
use squill_core::parameters::Parameters;
use std::collections::HashMap;
use std::sync::Arc;

pub(crate) struct Postgres {
    pub(crate) client: postgres::Client,
}

impl DriverConnection for Postgres {
    fn driver_name(&self) -> &str {
        DRIVER_NAME
    }

    fn close(self: Box<Self>) -> Result<()> {
        Ok(())
    }

    fn prepare<'c: 's, 's>(&'c mut self, statement: &str) -> Result<Box<dyn DriverStatement + 's>> {
        Ok(Box::new(PostgresStatement {
            inner: self.client.prepare(statement).map_err(into_driver_error)?,
            client: &mut self.client,
            phantom: std::marker::PhantomData,
        }))
    }
}

pub(crate) struct PostgresStatement<'c> {
    pub(crate) client: &'c mut postgres::Client,
    pub(crate) inner: postgres::Statement,
    pub(crate) phantom: std::marker::PhantomData<&'c ()>,
}

pub trait ArrowArrayAppender<T> {
    fn append_value(&mut self, value: Option<T>);
}

macro_rules! impl_arrow_array_appender {
    ($data_type:ty, $builder_type:ty) => {
        impl ArrowArrayAppender<$data_type> for dyn ArrayBuilder {
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

impl_arrow_array_appender!(bool, arrow_array::builder::BooleanBuilder);
impl_arrow_array_appender!(i16, arrow_array::builder::Int16Builder);
impl_arrow_array_appender!(i32, arrow_array::builder::Int32Builder);
impl_arrow_array_appender!(i64, arrow_array::builder::Int64Builder);
impl_arrow_array_appender!(f32, arrow_array::builder::Float32Builder);
impl_arrow_array_appender!(f64, arrow_array::builder::Float64Builder);
impl_arrow_array_appender!(String, arrow_array::builder::StringBuilder);

impl PostgresStatement<'_> {
    fn column_into_field(column: &postgres::Column) -> Field {
        let name = column.name().to_string();
        let data_type = match *column.type_() {
            postgres_types::Type::BOOL => DataType::Boolean,
            postgres_types::Type::INT2 => DataType::Int16,
            postgres_types::Type::INT4 => DataType::Int32,
            postgres_types::Type::INT8 => DataType::Int64,
            postgres_types::Type::FLOAT4 => DataType::Float32,
            postgres_types::Type::FLOAT8 => DataType::Float64,
            postgres_types::Type::NUMERIC => todo!("Should be DECIMAL type but we don't know the precision and scale"),
            postgres_types::Type::DATE => DataType::Date32,
            postgres_types::Type::TIME => DataType::Time32(arrow_schema::TimeUnit::Second),
            postgres_types::Type::TIMESTAMP => DataType::Timestamp(arrow_schema::TimeUnit::Second, None),
            postgres_types::Type::TIMESTAMPTZ => DataType::Timestamp(arrow_schema::TimeUnit::Second, None),
            postgres_types::Type::TEXT => DataType::Utf8,
            postgres_types::Type::BYTEA => DataType::Binary,
            postgres_types::Type::UUID => DataType::Utf8,
            postgres_types::Type::JSONB => DataType::Utf8,
            // &postgres_types::Type::ARRAY => DataType::List(Box::new(Self::column_into_field(column.element_type().unwrap()))),
            _ => DataType::Binary,
        };

        let mut metadata: HashMap<String, String> = HashMap::new();
        metadata.insert("datasource_type".to_string(), column.type_().to_string());
        Field::new(name, data_type, true).with_metadata(metadata)
    }
}

impl DriverStatement for PostgresStatement<'_> {
    fn execute(&mut self, _parameters: Option<Parameters>) -> Result<u64> {
        Ok(self.client.execute(&self.inner, &[]).map_err(into_driver_error)? as u64)
    }

    fn query<'s>(
        &'s mut self,
        parameters: Option<Parameters>,
    ) -> Result<Box<dyn Iterator<Item = Result<RecordBatch>> + 's>> {
        let params_iter = ParametersIterator::new(&parameters);
        let schema = self.schema();
        let res_iter = self.client.query_raw(&self.inner, params_iter).map_err(into_driver_error)?;
        let iter = PostgresRows { schema, inner: res_iter, max_batch_rows: 1000 };
        Ok(Box::new(iter))
    }

    fn schema(&self) -> SchemaRef {
        let fields: Vec<Field> = self.inner.columns().iter().map(Self::column_into_field).collect::<Vec<Field>>();
        Arc::new(Schema::new(fields))
    }
}

struct PostgresRows<'s> {
    schema: SchemaRef,
    max_batch_rows: usize,
    inner: postgres::RowIter<'s>,
}

impl PostgresRows<'_> {
    fn append_row(arrow_columns: &mut [Box<dyn ArrayBuilder>], row: postgres::Row) -> Result<()> {
        for (index, row_column) in row.columns().iter().enumerate() {
            let builder = &mut arrow_columns[index];
            match *row_column.type_() {
                postgres_types::Type::BOOL => {
                    let value: Option<bool> = row.try_get(index).map_err(into_driver_error)?;
                    builder.append_value(value);
                }
                postgres_types::Type::INT2 => {
                    let value: Option<i16> = row.try_get(index).map_err(into_driver_error)?;
                    builder.append_value(value);
                }
                postgres_types::Type::INT4 => {
                    let value: Option<i32> = row.try_get(index).map_err(into_driver_error)?;
                    builder.append_value(value);
                }
                postgres_types::Type::INT8 => {
                    let value: Option<i64> = row.try_get(index).map_err(into_driver_error)?;
                    builder.append_value(value);
                }
                postgres_types::Type::FLOAT4 => {
                    let value: Option<f32> = row.try_get(index).map_err(into_driver_error)?;
                    builder.append_value(value);
                }
                postgres_types::Type::FLOAT8 => {
                    let value: Option<f64> = row.try_get(index).map_err(into_driver_error)?;
                    builder.append_value(value);
                }
                postgres_types::Type::TEXT => {
                    let value: Option<String> = row.try_get(index).map_err(into_driver_error)?;
                    builder.append_value(value);
                }
                postgres_types::Type::NUMERIC => todo!(),
                postgres_types::Type::DATE => todo!(),
                postgres_types::Type::TIME => todo!(),
                postgres_types::Type::TIMESTAMP => todo!(),
                postgres_types::Type::TIMESTAMPTZ => todo!(),
                postgres_types::Type::BYTEA => todo!(),
                postgres_types::Type::UUID => todo!(),
                postgres_types::Type::JSONB => todo!(),
                _ => todo!(),
            }
        }
        Ok(())
    }
}

impl Iterator for PostgresRows<'_> {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut columns = self
            .schema
            .fields()
            .iter()
            .map(|field| arrow_array::builder::make_builder(field.data_type(), 0))
            .collect::<Vec<_>>();

        let mut row_num = 0;
        let inner = &mut self.inner;
        loop {
            let next_row = inner.next().map_err(into_driver_error);
            match next_row {
                Ok(Some(row)) => match Self::append_row(&mut columns, row) {
                    Ok(_) => {
                        row_num += 1;
                        if row_num >= self.max_batch_rows {
                            break;
                        }
                    }
                    Err(e) => return Some(Err(e)),
                },
                Ok(None) => break,
                Err(e) => return Some(Err(e.into())),
            };
        }
        match row_num {
            0 => None,
            _ => {
                let arrays: Vec<_> = columns.iter_mut().map(|builder| builder.finish()).collect();
                let batch = RecordBatch::try_new(self.schema.clone(), arrays);
                Some(batch.map_err(|e| e.into()))
            }
        }
    }
}
