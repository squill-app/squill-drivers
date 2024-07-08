use crate::value::Adapter;
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use squill_core::driver::DriverStatement;
use squill_core::parameters::Parameters;
use squill_core::Result;
use std::sync::Arc;

pub(crate) struct SqliteStatement<'c> {
    pub(crate) inner: rusqlite::Statement<'c>,
}

impl DriverStatement for SqliteStatement<'_> {
    fn bind(&mut self, parameters: Parameters) -> Result<()> {
        let expected = self.inner.parameter_count();
        match parameters {
            Parameters::None => {
                if expected > 0 {
                    return Err(Box::new(rusqlite::Error::InvalidParameterCount(expected, 0)));
                }
                Ok(())
            }
            Parameters::Positional(values) => {
                if expected != values.len() {
                    return Err(Box::new(rusqlite::Error::InvalidParameterCount(expected, values.len())));
                }
                // The valid values for the index `in raw_bind_parameter` begin at `1`, and end at
                // [`Statement::parameter_count`], inclusive.
                for (index, value) in values.iter().enumerate() {
                    self.inner.raw_bind_parameter(index + 1, Adapter(value))?;
                }
                Ok(())
            }
        }
    }

    fn execute(&mut self) -> Result<u64> {
        Ok(self.inner.raw_execute()? as u64)
    }

    fn query<'s>(&'s mut self) -> Result<Box<dyn Iterator<Item = Result<RecordBatch>> + 's>> {
        Ok(Box::new(SqliteRows { inner: self.inner.raw_query() }))
    }
}

struct SqliteRows<'s> {
    inner: rusqlite::Rows<'s>,
}

impl<'c> Iterator for SqliteRows<'c> {
    type Item = Result<arrow_array::RecordBatch>;

    fn next(&mut self) -> Option<Result<arrow_array::RecordBatch>> {
        let rows = &mut self.inner;
        let row = rows.next();
        match row {
            Ok(Some(row)) => {
                println!("{:?}", row);
                let schema = Arc::new(Schema::empty());
                Some(Ok(RecordBatch::new_empty(schema)))
            }
            Ok(None) => None,
            Err(e) => Some(Err(Box::new(e))),
        }
    }
}
