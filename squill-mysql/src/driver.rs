use crate::{errors::driver_error, DRIVER_NAME};
use arrow_array::builder::ArrayBuilder;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use mysql::prelude::Queryable;
use mysql::Binary;
use squill_core::arrow::array_builder::ArrayBuilderAppender;
use squill_core::driver::{DriverConnection, DriverOptionsRef, DriverStatement, Result};
use squill_core::parameters::Parameters;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::error;

pub(crate) struct MySql {
    pub(crate) conn: mysql::Conn,
    pub(crate) options: DriverOptionsRef,
}

impl DriverConnection for MySql {
    fn driver_name(&self) -> &str {
        DRIVER_NAME
    }

    /// Check if the connection is alive.
    fn ping(&mut self) -> Result<()> {
        match self.conn.ping() {
            Ok(_) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    fn close(self: Box<Self>) -> Result<()> {
        Ok(())
    }

    fn prepare<'c: 's, 's>(&'c mut self, _statement: &str) -> Result<Box<dyn DriverStatement + 's>> {
        let inner_stmt = self.conn.prep(_statement)?;
        Ok(Box::new(MySqlStatement {
            inner: inner_stmt,
            client: &mut self.conn,
            options: self.options.clone(),
            schema: None,
        }))
    }
}

pub(crate) struct MySqlStatement<'c> {
    client: &'c mut mysql::Conn,
    inner: mysql::Statement,
    options: DriverOptionsRef,
    schema: Option<SchemaRef>,
}

impl MySqlStatement<'_> {
    fn column_into_field(column: &mysql::Column) -> Field {
        let (arrow_type, mysql_type) = match column.column_type() {
            mysql::consts::ColumnType::MYSQL_TYPE_DECIMAL => (DataType::Decimal128(0, 0), "DECIMAL"),
            mysql::consts::ColumnType::MYSQL_TYPE_TINY => (DataType::Int8, "TINY"),
            mysql::consts::ColumnType::MYSQL_TYPE_SHORT => (DataType::Int16, "SHORT"),
            mysql::consts::ColumnType::MYSQL_TYPE_LONG => (DataType::Int32, "LONG"),
            mysql::consts::ColumnType::MYSQL_TYPE_FLOAT => (DataType::Float32, "FLOAT"),
            mysql::consts::ColumnType::MYSQL_TYPE_DOUBLE => (DataType::Float64, "DOUBLE"),
            mysql::consts::ColumnType::MYSQL_TYPE_NULL => (DataType::Null, "NULL"),
            mysql::consts::ColumnType::MYSQL_TYPE_TIMESTAMP => {
                (DataType::Timestamp(TimeUnit::Microsecond, None), "TIMESTAMP")
            }
            mysql::consts::ColumnType::MYSQL_TYPE_LONGLONG => (DataType::Int64, "LONGLONG"),
            mysql::consts::ColumnType::MYSQL_TYPE_INT24 => (DataType::Int32, "INT24"),
            mysql::consts::ColumnType::MYSQL_TYPE_DATE => (DataType::Date32, "DATE"),
            mysql::consts::ColumnType::MYSQL_TYPE_TIME => (DataType::Time64(TimeUnit::Microsecond), "TIME"),
            mysql::consts::ColumnType::MYSQL_TYPE_DATETIME => {
                (DataType::Timestamp(TimeUnit::Microsecond, None), "DATETIME")
            }
            mysql::consts::ColumnType::MYSQL_TYPE_YEAR => (DataType::Int16, "YEAR"),
            mysql::consts::ColumnType::MYSQL_TYPE_NEWDATE => (DataType::Date32, "NEWDATE"),
            mysql::consts::ColumnType::MYSQL_TYPE_VARCHAR => (DataType::Utf8, "VARCHAR"),
            mysql::consts::ColumnType::MYSQL_TYPE_BIT => (DataType::Boolean, "BIT"),
            mysql::consts::ColumnType::MYSQL_TYPE_TIMESTAMP2 => {
                (DataType::Timestamp(TimeUnit::Microsecond, None), "TIMESTAMP2")
            }
            mysql::consts::ColumnType::MYSQL_TYPE_DATETIME2 => {
                (DataType::Timestamp(TimeUnit::Microsecond, None), "DATETIME2")
            }
            mysql::consts::ColumnType::MYSQL_TYPE_TIME2 => (DataType::Time64(TimeUnit::Microsecond), "TIME2"),
            mysql::consts::ColumnType::MYSQL_TYPE_TYPED_ARRAY => (DataType::Utf8, "TYPED_ARRAY"),
            mysql::consts::ColumnType::MYSQL_TYPE_UNKNOWN => (DataType::Utf8, "UNKNOWN"),
            mysql::consts::ColumnType::MYSQL_TYPE_JSON => (DataType::Utf8, "JSON"),
            mysql::consts::ColumnType::MYSQL_TYPE_NEWDECIMAL => (DataType::Decimal128(0, 0), "NEWDECIMAL"),
            mysql::consts::ColumnType::MYSQL_TYPE_ENUM => (DataType::Utf8, "ENUM"),
            mysql::consts::ColumnType::MYSQL_TYPE_SET => (DataType::Utf8, "SET"),
            mysql::consts::ColumnType::MYSQL_TYPE_TINY_BLOB => (DataType::Binary, "TINY_BLOB"),
            mysql::consts::ColumnType::MYSQL_TYPE_MEDIUM_BLOB => (DataType::Binary, "MEDIUM_BLOB"),
            mysql::consts::ColumnType::MYSQL_TYPE_LONG_BLOB => (DataType::Binary, "LONG_BLOB"),
            mysql::consts::ColumnType::MYSQL_TYPE_BLOB => (DataType::Binary, "BLOB"),
            mysql::consts::ColumnType::MYSQL_TYPE_VAR_STRING => (DataType::Utf8, "VAR_STRING"),
            mysql::consts::ColumnType::MYSQL_TYPE_STRING => (DataType::Utf8, "STRING"),
            mysql::consts::ColumnType::MYSQL_TYPE_GEOMETRY => (DataType::Utf8, "GEOMETRY"),
        };

        let mut metadata: HashMap<String, String> = HashMap::new();
        metadata.insert("datasource_type".to_string(), mysql_type.to_lowercase());
        Field::new(column.name_str(), arrow_type, !column.flags().contains(mysql::consts::ColumnFlags::NOT_NULL_FLAG))
            .with_metadata(metadata)
    }
}

impl DriverStatement for MySqlStatement<'_> {
    fn execute(&mut self, _parameters: Option<Parameters>) -> Result<u64> {
        match self.client.exec_drop(&self.inner, mysql::Params::Empty).map_err(driver_error) {
            Ok(_) => Ok(self.client.affected_rows()),
            Err(err) => Err(err.into()),
        }
    }

    fn query<'s>(
        &'s mut self,
        _parameters: Option<Parameters>,
    ) -> Result<Box<dyn Iterator<Item = Result<RecordBatch>> + 's>> {
        match self.client.exec_iter(&self.inner, mysql::Params::Empty).map_err(driver_error) {
            Ok(query_result) => {
                // build the schema
                let mut fields: Vec<Field> = vec![];
                for column in query_result.columns().as_ref() {
                    fields.push(Self::column_into_field(column));
                }
                let schema = Arc::new(Schema::new(fields));
                let rows = MySqlRows { inner: query_result, schema: schema.clone(), options: self.options.clone() };
                self.schema = Some(schema);
                Ok(Box::new(rows))
            }
            Err(err) => Err(err.into()),
        }
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone().unwrap()
    }
}

impl Drop for MySqlStatement<'_> {
    fn drop(&mut self) {
        if let Err(err) = self.client.close(self.inner.clone()) {
            error!("Failed to close statement: {}", err);
        }
    }
}

struct MySqlRows<'c, 't, 'tc> {
    inner: mysql::QueryResult<'c, 't, 'tc, Binary>,
    options: DriverOptionsRef,
    schema: SchemaRef,
}

impl MySqlRows<'_, '_, '_> {
    fn append_row(arrow_columns: &mut [Box<dyn ArrayBuilder>], row: mysql::Row) -> Result<()> {
        for (index, row_column) in row.columns().iter().enumerate() {
            let builder = &mut arrow_columns[index];
            let mysql_type = row_column.column_type();
            match mysql_type {
                mysql::consts::ColumnType::MYSQL_TYPE_DECIMAL => todo!(),
                mysql::consts::ColumnType::MYSQL_TYPE_TINY => todo!(),
                mysql::consts::ColumnType::MYSQL_TYPE_SHORT => todo!(),
                mysql::consts::ColumnType::MYSQL_TYPE_LONG => todo!(),
                mysql::consts::ColumnType::MYSQL_TYPE_FLOAT => todo!(),
                mysql::consts::ColumnType::MYSQL_TYPE_DOUBLE => todo!(),
                mysql::consts::ColumnType::MYSQL_TYPE_NULL => todo!(),
                mysql::consts::ColumnType::MYSQL_TYPE_TIMESTAMP => todo!(),
                mysql::consts::ColumnType::MYSQL_TYPE_LONGLONG => {
                    builder.append_value(row.get_opt::<i64, usize>(index).transpose()?);
                }
                mysql::consts::ColumnType::MYSQL_TYPE_INT24 => todo!(),
                mysql::consts::ColumnType::MYSQL_TYPE_DATE => todo!(),
                mysql::consts::ColumnType::MYSQL_TYPE_TIME => todo!(),
                mysql::consts::ColumnType::MYSQL_TYPE_DATETIME => todo!(),
                mysql::consts::ColumnType::MYSQL_TYPE_YEAR => todo!(),
                mysql::consts::ColumnType::MYSQL_TYPE_NEWDATE => todo!(),
                mysql::consts::ColumnType::MYSQL_TYPE_VARCHAR => todo!(),
                mysql::consts::ColumnType::MYSQL_TYPE_BIT => todo!(),
                mysql::consts::ColumnType::MYSQL_TYPE_TIMESTAMP2 => todo!(),
                mysql::consts::ColumnType::MYSQL_TYPE_DATETIME2 => todo!(),
                mysql::consts::ColumnType::MYSQL_TYPE_TIME2 => todo!(),
                mysql::consts::ColumnType::MYSQL_TYPE_TYPED_ARRAY => todo!(),
                mysql::consts::ColumnType::MYSQL_TYPE_UNKNOWN => todo!(),
                mysql::consts::ColumnType::MYSQL_TYPE_JSON => todo!(),
                mysql::consts::ColumnType::MYSQL_TYPE_NEWDECIMAL => todo!(),
                mysql::consts::ColumnType::MYSQL_TYPE_ENUM => todo!(),
                mysql::consts::ColumnType::MYSQL_TYPE_SET => todo!(),
                mysql::consts::ColumnType::MYSQL_TYPE_TINY_BLOB => todo!(),
                mysql::consts::ColumnType::MYSQL_TYPE_MEDIUM_BLOB => todo!(),
                mysql::consts::ColumnType::MYSQL_TYPE_LONG_BLOB => todo!(),
                mysql::consts::ColumnType::MYSQL_TYPE_BLOB => todo!(),
                mysql::consts::ColumnType::MYSQL_TYPE_VAR_STRING => todo!(),
                mysql::consts::ColumnType::MYSQL_TYPE_STRING => todo!(),
                mysql::consts::ColumnType::MYSQL_TYPE_GEOMETRY => todo!(),
            }
        }
        Ok(())
    }
}

impl Iterator for MySqlRows<'_, '_, '_> {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut columns = self
            .schema
            .fields()
            .iter()
            .map(|field| arrow_array::builder::make_builder(field.data_type(), 0))
            .collect::<Vec<_>>();

        let max_batch_rows = self.options.max_batch_rows;
        let mut row_num = 0;
        let inner = &mut self.inner;
        loop {
            let new_row = inner.next();
            match new_row {
                Some(Ok(row)) => match Self::append_row(&mut columns, row) {
                    Ok(_) => {
                        row_num += 1;
                        if row_num >= max_batch_rows {
                            break;
                        }
                    }
                    Err(e) => return Some(Err(e)),
                },
                None => break,
                Some(Err(e)) => return Some(Err(e.into())),
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
