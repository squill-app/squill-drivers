use crate::statement::Statement;
use squill_core::driver::DriverConnection;
use squill_core::factory::Factory;
use squill_core::parameters::Parameters;
use squill_core::row::Row;
use squill_core::{Error, Result};

/// A connection to a data source.
///
/// ```rust,ignore
/// use squill_blocking::Connection;
///
/// let nut conn = Connection::open("mock://").unwrap();
///
/// conn.execute("CREATE TABLE employee (id BIGINT)").unwrap();
/// conn.execute("INSERT INTO employee (id, name) VALUES (?, ?)").execute().unwrap();
///
/// let stmt = conn.prepare("SELECT * FROM employee")?;
/// let rows = query!(stmt, 1, "Alice");
/// ```
pub struct Connection {
    inner: Box<dyn DriverConnection>,
}

impl Connection {
    pub fn open(uri: &str) -> Result<Self> {
        let inner = Factory::open(uri)?;
        Ok(Self { inner })
    }

    /// Get the driver name used by the connection.
    pub fn driver_name(&self) -> &str {
        self.inner.driver_name()
    }

    /// Check if the connection is alive.
    pub fn ping(&mut self) -> Result<()> {
        self.inner.ping().map_err(Error::from)
    }

    /// Prepare a statement.
    ///
    /// Return a [Statement] that can be later used to by `query` or `execute` functions. A prepared statement can be
    /// used multiple times with different parameters.    
    pub fn prepare<S: AsRef<str>>(&mut self, statement: S) -> Result<Statement<'_>> {
        Ok(Statement { inner: self.inner.prepare(statement.as_ref())? })
    }

    /// Execute a statement.
    ///
    /// This function can be called either with a prepared statement or a string as a command.
    ///
    /// Returns the number of rows affected.
    pub fn execute<S: AsRef<str>>(&mut self, statement: S, parameters: Option<Parameters>) -> Result<u64> {
        let mut statement = self.prepare(statement)?;
        statement.execute(parameters)
    }

    /// Query a statement that is expected to return a single [Row].
    ///
    /// Returns `Ok(None)` if the query returned no rows.
    /// If the query returns more than one row, the function will return an the first row and ignore the rest.
    pub fn query_row<S: AsRef<str>>(&mut self, statement: S, parameters: Option<Parameters>) -> Result<Option<Row>> {
        let mut statement = self.prepare(statement)?;
        statement.query_row(parameters)
    }

    /// Query a statement that is expected to return a single row and map it to a value.
    ///
    /// Returns `Ok(None)` if the query returned no rows.
    /// If the query returns more than one row, the function will return an the first row and ignore the rest.
    ///
    /// See [Statement::query_map_row] for more information.
    pub fn query_map_row<S: AsRef<str>, F, T>(
        &mut self,
        statement: S,
        parameters: Option<Parameters>,
        mapping_fn: F,
    ) -> Result<Option<T>>
    where
        F: FnOnce(Row) -> std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>,
    {
        let mut statement = self.prepare(statement)?;
        statement.query_map_row(parameters, mapping_fn)
    }

    /// Close the connection.
    ///
    /// Because a {{Statement}} borrows the connection, all statements must be dropped before calling `close()`.
    ///
    /// ```rust
    /// use squill_blocking::Connection;
    ///
    /// let mut conn = Connection::open("mock://").unwrap();
    /// let stmt = conn.prepare("SELECT 1").unwrap();
    ///
    /// // If not dropped, the rust compiler will complain about it borrowing `conn` when trying to call `conn.close()`.
    /// drop(stmt);
    ///
    /// assert!(conn.close().is_ok());
    /// ```
    pub fn close(self) -> Result<()> {
        self.inner.close().map_err(Error::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use squill_core::{assert_ok, params};

    #[test]
    fn test_prepare() {
        let mut conn = Connection::open("mock://").unwrap();
        assert!(conn.prepare("XINSERT").is_err());
        assert!(conn.prepare("SELECT 1").is_ok());
    }

    #[test]
    fn test_ping() {
        let mut conn = Connection::open("mock://").unwrap();
        assert_ok!(conn.ping());
    }

    #[test]
    fn test_query_map_row() {
        struct TestUser {
            id: i32,
            username: String,
        }

        let mut conn = Connection::open("mock://").unwrap();

        // some rows
        let user = conn
            .query_map_row("SELECT 1", None, |row| {
                Ok(TestUser { id: row.get::<_, _>(0), username: row.get::<_, _>(1) })
            })
            .unwrap()
            .unwrap();
        assert_eq!(user.id, 1);
        assert_eq!(user.username, "user1");

        // no rows
        assert!(conn
            .query_map_row("SELECT 0", None, |row| Ok(TestUser { id: row.get(0), username: "".to_string() }))
            .unwrap()
            .is_none());

        // error by the query
        assert!(conn
            .query_map_row("SELECT X", None, |row| Ok(TestUser { id: row.get(0), username: "".to_string() }))
            .is_err());

        // error by the mapping function
        assert!(conn
            .query_map_row("SELECT 1", None, |row| {
                if row.get::<_, i32>(0) == 2 {
                    Ok(TestUser { id: 2, username: "user2".to_string() })
                } else {
                    Err("error".into())
                }
            })
            .is_err());
    }

    #[test]
    fn test_query_rows() {
        let mut conn = Connection::open("mock://").unwrap();

        // some rows
        let mut stmt = conn.prepare("SELECT 2").unwrap();
        let mut rows = stmt.query_rows(None).unwrap();
        assert_eq!(rows.next().unwrap().unwrap().get::<_, i32>(0), 1);
        assert_eq!(rows.next().unwrap().unwrap().get::<_, i32>(0), 2);
        assert!(rows.next().is_none());
        drop(rows);
        drop(stmt);

        // no rows
        let mut stmt = conn.prepare("SELECT 0").unwrap();
        let mut rows = stmt.query_rows(None).unwrap();
        assert!(rows.next().is_none());
        drop(rows);
        drop(stmt);

        // error on first call to next()
        let mut stmt = conn.prepare("SELECT -1").unwrap();
        let mut rows = stmt.query_rows(None).unwrap();
        assert!(matches!(rows.next(), Some(Err(_))));
        drop(rows);
        drop(stmt);

        // error on call to query_rows()
        let mut stmt = conn.prepare("SELECT X").unwrap();
        assert!(stmt.query_rows(None).is_err());
    }

    #[test]
    fn test_query_row() {
        let mut conn = Connection::open("mock://").unwrap();

        assert_eq!(conn.query_row("SELECT 2", None).unwrap().unwrap().get::<_, i32>(0), 1);
        assert_eq!(conn.query_row("SELECT 1", None).unwrap().unwrap().get::<_, i32>(0), 1);
        assert!(conn.query_row("SELECT 0", None).unwrap().is_none());
        assert!(conn.query_row("SELECT -1", None).is_err());
        assert!(conn.query_row("SELECT X", None).is_err());

        let mut stmt = conn.prepare("SELECT 1").unwrap();
        assert_eq!(stmt.query_row(None).unwrap().unwrap().get::<_, i32>(0), 1);
        assert_eq!(stmt.query_row(None).unwrap().unwrap().get::<_, i32>(0), 1);
    }

    #[test]
    fn test_connection() {
        // Test connection open
        assert!(Connection::open("unknown://").is_err());
        let mut conn = Connection::open("mock://").unwrap();
        assert_eq!(conn.driver_name(), "mock");

        // Test connection prepare
        assert!(conn.prepare("XINSERT").is_err());
        assert!(conn.prepare("SELECT 1").is_ok());

        // Test connection execute
        assert!(conn.execute("XINSERT", None).is_err());
        assert_eq!(conn.execute("INSERT 1", None).unwrap(), 1);
        assert!(conn.execute("SELECT 1", None).is_err()); // SELECT is not allowed for execute().
        assert!(conn.execute("INSERT ?", params!(1, 2)).is_err()); // Number of parameters does not match the number of placeholders
        let mut stmt = conn.prepare("INSERT 1").unwrap();
        assert!(stmt.execute(None).is_ok()); // using a prepared statement
        drop(stmt);

        // Test connection close
        assert!(conn.close().is_ok());
    }
}
