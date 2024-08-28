use crate::errors::driver_error;
use crate::statement::SqliteStatement;
use crate::{Sqlite, DRIVER_NAME};
use squill_core::driver::DriverConnection;
use squill_core::driver::DriverStatement;
use squill_core::driver::Result;

impl DriverConnection for Sqlite {
    fn driver_name(&self) -> &str {
        DRIVER_NAME
    }

    fn close(self: Box<Self>) -> Result<()> {
        Ok(())
    }

    fn prepare<'c: 's, 's>(&'c self, statement: &str) -> Result<Box<dyn DriverStatement + 's>> {
        Ok(Box::new(SqliteStatement {
            inner: self.conn.prepare(statement).map_err(driver_error)?,
            options: self.options.clone(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use crate::IN_MEMORY_URI;
    use ctor::ctor;
    use squill_core::{connection::Connection, execute, query_arrow};

    #[ctor]
    fn before_all() {
        crate::register_driver();
    }

    #[test]
    fn test_sqlite_driver() {
        let conn = Connection::open(IN_MEMORY_URI).unwrap();
        assert_eq!(conn.driver_name(), "sqlite");
        assert_eq!(execute!(conn, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)").unwrap(), 0);
        assert_eq!(execute!(conn, "INSERT INTO test (id, name) VALUES (1, NULL)").unwrap(), 1);
        assert_eq!(execute!(conn, "INSERT INTO test (id, name) VALUES (?, ?)", 2, "Bob").unwrap(), 1);
        assert!(execute!(conn, "INSERT INTO test (id, name) VALUES (?, ?)", 2, "Bob").is_err());

        let mut stmt = conn.prepare("SELECT id, name FROM test ORDER BY id").unwrap();
        let mut batch = query_arrow!(stmt).unwrap();
        assert!(batch.next().is_some());
        assert!(batch.next().is_none());

        drop(batch);
        drop(stmt);

        assert!(conn.close().is_ok());
    }
}
