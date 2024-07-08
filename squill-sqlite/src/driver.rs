use squill_core::driver::DriverConnection;
use squill_core::driver::DriverStatement;
use squill_core::Result;

use crate::statement::SqliteStatement;
use crate::{Sqlite, DRIVER_NAME};

impl DriverConnection for Sqlite {
    fn driver_name(&self) -> &str {
        DRIVER_NAME
    }

    fn close(self: Box<Self>) -> Result<()> {
        Ok(())
    }

    fn prepare<'c: 's, 's>(&'c self, statement: &str) -> Result<Box<dyn DriverStatement + 's>> {
        Ok(Box::new(SqliteStatement { inner: self.conn.prepare(statement)? }))
    }
}

#[cfg(test)]
mod tests {
    use ctor::ctor;
    use path_slash::PathExt;
    use squill_core::{connection::Connection, execute, query_arrow};

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

        let mut stmt = conn.prepare("SELECT * FROM test").unwrap();
        let mut rows = query_arrow!(stmt).unwrap();
        assert!(rows.next().is_some());
        assert!(rows.next().is_some());
        assert!(rows.next().is_none());
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
        assert!(execute!(conn, "INSERT INTO test (id, name) VALUES (1, 'Alice')").is_err());
        assert_eq!(execute!(conn, "INSERT INTO test (id, name) VALUES (?, ?)", 2, "Bob").unwrap(), 1);

        conn.close().unwrap();
    }
}
