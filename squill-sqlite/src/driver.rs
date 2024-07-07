use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use squill_core::driver::DriverStatement;
use squill_core::Result;
use squill_core::{driver::DriverConnection, parameters::Parameters};

use crate::value::Adapter;
use crate::{Sqlite, DRIVER_NAME};

impl DriverConnection for Sqlite {
    fn driver_name(&self) -> &str {
        DRIVER_NAME
    }

    #[allow(unused_variables)]
    fn execute(&self, statement: String, parameters: Parameters) -> Result<u64> {
        todo!()
    }

    #[allow(unused_variables)]
    fn query<'c>(
        &'c self,
        statement: String,
        parameters: Parameters,
    ) -> Result<Box<dyn Iterator<Item = Result<RecordBatch>> + 'c>> {
        todo!()
    }

    fn close(self: Box<Self>) -> Result<()> {
        Ok(())
    }

    fn prepare<'c>(&'c self, statement: &str) -> Result<Box<dyn DriverStatement + 'c>> {
        Ok(Box::new(SqliteStatement { inner: self.conn.prepare(statement)? }))
    }
}

struct SqliteStatement<'c> {
    inner: rusqlite::Statement<'c>,
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

#[cfg(test)]
mod tests {
    use ctor::ctor;
    use path_slash::PathExt;
    use squill_core::{connection::Connection, execute, query};

    use crate::IN_MEMORY_URI;

    #[ctor]
    fn before_all() {
        crate::register_driver();
    }

    #[test]
    fn test_query() {
        let conn = Connection::open(IN_MEMORY_URI).unwrap();
        assert_eq!(execute!(conn, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)").unwrap(), 0);
        assert_eq!(execute!(conn, "INSERT INTO test (id, name) VALUES (1, NULL)").unwrap(), 1);
        assert_eq!(execute!(conn, "INSERT INTO test (id, name) VALUES (?, ?)", 2, "Bob").unwrap(), 1);

        let mut rows = query!(conn, "SELECT * FROM test").unwrap();
        let _ = rows.next();
    }

    #[test]
    fn test_open_memory() {
        let conn = Connection::open(IN_MEMORY_URI).unwrap();
        assert_eq!(conn.driver_name(), "sqlite");
        assert!(conn.close().is_ok());
    }

    #[test]
    fn test_open_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.db");

        // trying to open a file that does not exist in read-only should fail
        assert!(Connection::open(&format!("sqlite://{}?mode=r", file_path.to_slash_lossy())).is_err());
        // trying to open a file that does not exist in read-write should create it
        assert!(
            Connection::open(&format!("sqlite://{}?mode=rwc&max_batch_size=12", file_path.to_slash_lossy())).is_ok()
        );
    }

    #[test]
    fn test_execute() {
        let conn = Connection::open(IN_MEMORY_URI).unwrap();
        assert_eq!(execute!(conn, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)").unwrap(), 0);
        assert_eq!(execute!(conn, "INSERT INTO test (id, name) VALUES (1, 'Alice')").unwrap(), 1);
        assert_eq!(execute!(conn, "INSERT INTO test (id, name) VALUES (?, ?)", 2, "Bob").unwrap(), 1);

        conn.close().unwrap();
    }
}
