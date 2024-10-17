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

impl DriverStatement for SqliteStatement<'_> {
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

    fn execute(&mut self) -> Result<u64> {
        Ok(self.inner.raw_execute().map_err(driver_error)? as u64)
    }

    fn query<'s>(&'s mut self) -> Result<Box<dyn Iterator<Item = Result<RecordBatch>> + 's>> {
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
        let schema = Arc::new(Schema::new(fields));
        Ok(Box::new(SqliteRows {
            inner: self.inner.raw_query(),
            options: self.options.clone(),
            schema: RefCell::new(schema),
        }))
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
                // FIXME: remove the unwrap
                Some(Ok(RecordBatch::try_new(self.schema.borrow().clone(), arrays).unwrap()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::IN_MEMORY_URI;
    use ctor::ctor;
    use squill_core::decode::{self, Decode};
    use squill_core::{connection::Connection, execute, query_arrow, Result};

    #[ctor]
    fn before_all() {
        crate::register_driver();
    }

    #[test]
    fn test_schema_storage_classes() -> Result<()> {
        let conn = Connection::open(IN_MEMORY_URI).unwrap();
        execute!(&conn, "CREATE TABLE test (int INTEGER, text TEXT, real REAL, blob BLOB)")?;
        execute!(&conn, "INSERT INTO test (int, text, real, blob) VALUES (1, 'text', 1.1, x'01')")?;
        execute!(&conn, "INSERT INTO test (int, text, real, blob) VALUES (2, NULL, NULL, NULL)")?;
        let mut stmt = conn.prepare("SELECT int, text, real, blob FROM test ORDER BY int")?;
        let mut iter = query_arrow!(&mut stmt)?;
        let batch = iter.next().unwrap()?;

        assert_eq!(batch.schema().fields().len(), 4);
        assert_eq!(batch.num_rows(), 2);

        // first row
        assert_eq!(i64::decode(&batch.column(0), 0), 1);
        assert_eq!(String::decode(&batch.column(1), 0), "text");
        assert_eq!(f64::decode(&batch.column(2), 0), 1.1);
        assert_eq!(Vec::<u8>::decode(&batch.column(3), 0), vec![1]);

        // second row
        assert_eq!(i64::decode(&batch.column(0), 1), 2);
        assert!(batch.column(1).is_null(1));
        assert!(batch.column(2).is_null(1));
        assert!(batch.column(3).is_null(1));

        Ok(())
    }

    // In Sqlite, when using expressions in the SELECT clause, the column type is always at the time of the prepare
    // statement and the current implementation of the driver will eventually infer the type of the column when it get
    // non null values from the column. This test is to ensure that the driver can handle such cases.
    #[test]
    fn test_expressions() -> Result<()> {
        let conn = Connection::open(IN_MEMORY_URI)?;
        let mut stmt = conn.prepare("SELECT 1, 'Welcome', 'いらっしゃいませ', 1.1, x'01', NULL")?;
        let mut iter = query_arrow!(&mut stmt)?;
        let batch = iter.next().unwrap()?;
        assert_eq!(batch.schema().fields().len(), 6);
        assert_eq!(*batch.schema().fields()[0].data_type(), arrow_schema::DataType::Int64);
        assert_eq!(*batch.schema().fields()[1].data_type(), arrow_schema::DataType::Utf8);
        assert_eq!(*batch.schema().fields()[2].data_type(), arrow_schema::DataType::Utf8);
        assert_eq!(*batch.schema().fields()[3].data_type(), arrow_schema::DataType::Float64);
        assert_eq!(*batch.schema().fields()[4].data_type(), arrow_schema::DataType::Binary);
        assert_eq!(*batch.schema().fields()[5].data_type(), arrow_schema::DataType::Null);
        assert_eq!(i64::decode(&batch.column(0), 0), 1);
        assert_eq!(String::decode(&batch.column(1), 0), "Welcome");
        assert_eq!(String::decode(&batch.column(2), 0), "いらっしゃいませ");
        assert_eq!(f64::decode(&batch.column(3), 0), 1.1);
        assert_eq!(Vec::<u8>::decode(&batch.column(4), 0), vec![1]);
        assert!(decode::is_null(batch.column(5), 0));
        Ok(())
    }

    #[test]
    fn test_timestamp() -> Result<()> {
        let conn = Connection::open(IN_MEMORY_URI)?;
        let mut stmt = conn.prepare("SELECT datetime('2023-01-01 10:00:00')")?;
        let mut iter = query_arrow!(&mut stmt)?;
        let batch = iter.next().unwrap()?;
        assert_eq!(batch.schema().fields().len(), 1);
        assert_eq!(*batch.schema().fields()[0].data_type(), arrow_schema::DataType::Utf8);
        assert_eq!(String::decode(&batch.column(0), 0), "2023-01-01 10:00:00");
        Ok(())
    }

    #[test]
    fn test_boolean() -> Result<()> {
        let conn = Connection::open(IN_MEMORY_URI)?;
        let mut stmt = conn.prepare("SELECT TRUE, FALSE")?;
        let mut iter = query_arrow!(&mut stmt)?;
        let batch = iter.next().unwrap()?;
        assert_eq!(batch.schema().fields().len(), 2);
        assert_eq!(*batch.schema().fields()[0].data_type(), arrow_schema::DataType::Int64);
        assert!(bool::decode(&batch.column(0), 0));
        assert!(!bool::decode(&batch.column(1), 0));
        Ok(())
    }
}
