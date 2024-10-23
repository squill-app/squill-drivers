use crate::errors::driver_error;
use crate::value::Adapter;
use crate::SqliteOptionsRef;
use arrow_array::builder::ArrayBuilder;
use arrow_array::builder::BinaryBuilder;
use arrow_array::builder::Float64Builder;
use arrow_array::builder::Int64Builder;
use arrow_array::builder::NullBuilder;
use arrow_array::builder::StringBuilder;
use arrow_array::RecordBatch;
use arrow_schema::DataType;
use arrow_schema::Field;
use arrow_schema::Schema;
use arrow_schema::SchemaRef;
use squill_core::driver::DriverStatement;
use squill_core::driver::Result;
use squill_core::parameters::Parameters;
use squill_core::Error;
use std::cell::RefCell;
use std::sync::Arc;

pub(crate) struct SqliteStatement<'c> {
    // pub(crate) schema: Option<SchemaRef>,
    pub(crate) inner: rusqlite::Statement<'c>,
    pub(crate) options: SqliteOptionsRef,
}

impl SqliteStatement<'_> {
    fn bind(&mut self, parameters: Parameters) -> Result<()> {
        let expected = self.inner.parameter_count();
        match parameters {
            Parameters::Positional(values) => {
                if expected != values.len() {
                    return Err(Error::InvalidParameterCount { expected, actual: values.len() }.into());
                }
                // The valid values for the index `in raw_bind_parameter` begin at `1`, and end at
                // [`Statement::parameter_count`], inclusive.
                for (index, value) in values.iter().enumerate() {
                    self.inner.raw_bind_parameter(index + 1, Adapter(value)).map_err(driver_error)?;
                }
                Ok(())
            }
        }
    }
}

impl DriverStatement for SqliteStatement<'_> {
    fn execute(&mut self, parameters: Option<Parameters>) -> Result<u64> {
        if let Some(parameters) = parameters {
            self.bind(parameters)?;
        }
        Ok(self.inner.raw_execute().map_err(driver_error)? as u64)
    }

    fn query<'s>(
        &'s mut self,
        parameters: Option<Parameters>,
    ) -> Result<Box<dyn Iterator<Item = Result<RecordBatch>> + 's>> {
        if let Some(parameters) = parameters {
            self.bind(parameters)?;
        }
        let schema = self.schema();
        Ok(Box::new(SqliteRows {
            inner: self.inner.raw_query(),
            options: self.options.clone(),
            schema: RefCell::new(schema),
        }))
    }

    /// Returns the underlying schema of the prepared statement.
    fn schema(&self) -> SchemaRef {
        let fields: Vec<Field> = self
            .inner
            .columns()
            .iter()
            .map(|column| {
                let name = column.name().to_string();
                let data_type = match column.decl_type() {
                    Some("INTEGER") => arrow_schema::DataType::Int64,
                    Some("TEXT") => arrow_schema::DataType::Utf8,
                    Some("REAL") => arrow_schema::DataType::Float64,
                    Some("BLOB") => arrow_schema::DataType::Binary,
                    // If the column type is NULL or there is no decl_type, the column is considered as a NULL type.
                    // For expressions, the decl_type is always NULL so while adding values to the array for this column
                    // we will eventually need to have this type inferred from the data received.
                    _ => arrow_schema::DataType::Null,
                };
                Field::new(name, data_type, true)
            })
            .collect::<Vec<Field>>();
        Arc::new(Schema::new(fields))
    }
}

struct SqliteRows<'s> {
    inner: rusqlite::Rows<'s>,
    options: SqliteOptionsRef,
    schema: RefCell<SchemaRef>,
}

macro_rules! inner_append_value {
    ($BuilderType:ty, $DataType:expr, $value:expr, $columns:expr, $index:expr, $schema:expr, $value_ref:expr) => {
        match $columns[$index].as_any_mut().downcast_mut::<$BuilderType>() {
            Some(builder) => builder.append_value($value),
            None => {
                if let Some(null_builder) = $columns[$index].as_any_mut().downcast_mut::<NullBuilder>() {
                    // we have a NULL column, this means that earlier the type was unknown but now we know
                    // it's an integer we can convert it to an Int64Builder, this will typically happen when
                    // the type was unknown during the preparation of the statement because the column was
                    // an expression.
                    let mut new_builder = <$BuilderType>::new();
                    // FIXME: Ideally we should use `new_builder.append_nulls(null_builder.len())` but it's not available
                    // for all builders (a.k.a `GenericByteBuilder`) so we will append nulls one by one.
                    for _ in 0..null_builder.len() {
                        new_builder.append_null();
                    }
                    new_builder.append_value($value);
                    $columns[$index] = Box::new(new_builder);
                    // We also need to alter the schema to reflect the new type
                    let fields: Vec<Arc<Field>> = $schema
                        .borrow()
                        .fields()
                        .iter()
                        .enumerate()
                        .map(|(i, field)| {
                            if i == $index {
                                Arc::new(Field::new(field.name(), $DataType, field.is_nullable()))
                            } else {
                                field.clone()
                            }
                        })
                        .collect();
                    $schema.replace(Arc::new(Schema::new(fields)));
                } else {
                    panic!(
                        "SQLITE: Unexpected column type (expected {:?}, got {:?}).",
                        $schema.borrow().fields()[$index].data_type(),
                        $value_ref.data_type()
                    );
                }
            }
        }
    };
}

impl SqliteRows<'_> {
    fn append_value(
        schema: &RefCell<SchemaRef>,
        columns: &mut [Box<dyn ArrayBuilder>],
        row: &rusqlite::Row<'_>,
    ) -> Result<()> {
        let len = columns.len();
        for (index, _) in (0..len).enumerate() {
            let value_ref = row.get_ref(index)?;
            match value_ref.data_type() {
                rusqlite::types::Type::Integer => {
                    let value = value_ref.as_i64()?;
                    inner_append_value!(Int64Builder, DataType::Int64, value, columns, index, schema, value_ref);
                }
                rusqlite::types::Type::Text => {
                    let value = value_ref.as_str()?;
                    inner_append_value!(StringBuilder, DataType::Utf8, value, columns, index, schema, value_ref);
                }
                rusqlite::types::Type::Real => {
                    let value = value_ref.as_f64()?;
                    inner_append_value!(Float64Builder, DataType::Float64, value, columns, index, schema, value_ref);
                }
                rusqlite::types::Type::Blob => {
                    let value = value_ref.as_blob()?;
                    inner_append_value!(BinaryBuilder, DataType::Binary, value, columns, index, schema, value_ref);
                }
                rusqlite::types::Type::Null => {
                    if let Some(null_builder) = columns[index].as_any_mut().downcast_mut::<NullBuilder>() {
                        null_builder.append_null();
                    } else if let Some(string_builder) = columns[index].as_any_mut().downcast_mut::<StringBuilder>() {
                        string_builder.append_null();
                    } else if let Some(float_builder) = columns[index].as_any_mut().downcast_mut::<Float64Builder>() {
                        float_builder.append_null();
                    } else if let Some(int_builder) = columns[index].as_any_mut().downcast_mut::<Int64Builder>() {
                        int_builder.append_null();
                    } else if let Some(binary_builder) = columns[index].as_any_mut().downcast_mut::<BinaryBuilder>() {
                        binary_builder.append_null();
                    } else {
                        todo!();
                    }
                }
            }
        }
        Ok(())
    }
}

impl<'c> Iterator for SqliteRows<'c> {
    type Item = Result<arrow_array::RecordBatch>;

    fn next(&mut self) -> Option<Result<arrow_array::RecordBatch>> {
        let mut columns: Vec<Box<dyn ArrayBuilder>> = self
            .schema
            .borrow()
            .fields()
            .iter()
            .map(|field| match field.data_type() {
                DataType::Int64 => Box::new(Int64Builder::new()) as Box<dyn ArrayBuilder>,
                DataType::Utf8 => Box::new(StringBuilder::new()) as Box<dyn ArrayBuilder>,
                DataType::Float64 => Box::new(Float64Builder::new()) as Box<dyn ArrayBuilder>,
                DataType::Binary => Box::new(BinaryBuilder::new()) as Box<dyn ArrayBuilder>,
                DataType::Null => Box::new(NullBuilder::new()) as Box<dyn ArrayBuilder>,
                _ => panic!("Unsupported data type: {:?}", field.data_type()),
            })
            .collect();

        let rows = &mut self.inner;
        let mut row_num = 0;
        loop {
            let row = rows.next();
            match row {
                Ok(Some(row)) => match Self::append_value(&self.schema, &mut columns, row) {
                    Ok(_) => {
                        row_num += 1;
                        if row_num >= self.options.max_batch_rows {
                            break;
                        }
                    }
                    Err(error) => return Some(Err(error)),
                },
                Ok(None) => break,
                Err(error) => return Some(Err(error.into())),
            }
        }
        match row_num {
            0 => None,
            _ => {
                let arrays: Vec<_> = columns.iter_mut().map(|builder| builder.finish()).collect();
                let schema = self.schema.borrow().clone();
                let batch = RecordBatch::try_new(schema, arrays);
                Some(batch.map_err(|e| e.into()))
            }
        }
    }
}
