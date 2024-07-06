use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use squill_core::Result;
use squill_core::{driver::DriverConnection, parameters::Parameters};

use crate::{Sqlite, DRIVER_NAME};

impl DriverConnection for Sqlite {
    fn driver_name(&self) -> &str {
        DRIVER_NAME
    }

    fn execute(&self, statement: String, parameters: Parameters) -> Result<u64> {
        let mut prepared_statement = self.prepare_and_bind(statement, parameters)?;
        match prepared_statement.raw_execute() {
            Ok(affected_rows) => Ok(affected_rows as u64),
            Err(e) => Err(Box::new(e)),
        }
    }

    fn query<'c>(
        &'c self,
        statement: String,
        parameters: Parameters,
    ) -> Result<Box<dyn Iterator<Item = Result<RecordBatch>> + 'c>> {
        let mut prepared_statement: rusqlite::Statement = self.prepare_and_bind(statement, parameters)?;
        let rows: rusqlite::Rows = prepared_statement.raw_query();
        // let query = SqliteQuery { statement: prepared_statement, rows };
        // Ok(Box::new(query))
        todo!()
    }

    fn close(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}

struct SqliteQuery<'c> {
    statement: rusqlite::Statement<'c>,
    rows: rusqlite::Rows<'c>,
}

impl<'c> Iterator for SqliteQuery<'c> {
    type Item = Result<arrow_array::RecordBatch>;

    fn next(&mut self) -> Option<Result<arrow_array::RecordBatch>> {
        let rows = &mut self.rows;
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
