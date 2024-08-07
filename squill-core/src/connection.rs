use crate::driver::DriverConnection;
use crate::factory::Factory;
use crate::parameters::Parameters;
use crate::rows::{Row, Rows};
use crate::statement::{IntoStatement, Statement};
use crate::{Error, Result};
use arrow_array::RecordBatch;

pub struct Connection {
    inner: Box<dyn DriverConnection>,
}

/// A connection to a data source.
///
/// ```rust,ignore
/// use squill_core::connection::Connection;
///
/// let conn = Connection::open("mock://").unwrap();
///
/// conn.execute("CREATE TABLE employee (id BIGINT)").unwrap();
/// conn.prepare("INSERT INTO employee (id, name) VALUES (?, ?)").execute().unwrap();
///
/// let stmt = conn.prepare("SELECT * FROM employee")?;
/// let rows = query!(stmt, 1, "Alice");
///
/// ```
impl Connection {
    pub fn open(uri: &str) -> Result<Self> {
        let inner = Factory::open(uri)?;
        Ok(Self { inner })
    }

    /// Get the driver name used by the connection.
    pub fn driver_name(&self) -> &str {
        self.inner.driver_name()
    }

    /// Prepare a statement.
    ///
    /// Return a {{Statement}} that can be later used to by `query` or `execute` functions. A prepared statement can be
    /// used multiple times with different parameters.
    pub fn prepare<S: AsRef<str>>(&self, statement: S) -> Result<Statement<'_>> {
        Ok(Statement { inner: self.inner.prepare(statement.as_ref())? })
    }

    /// Execute a statement.
    ///
    /// This function can be either used with a prepared statement or a raw query given as a string.
    ///
    /// Returns the number of rows affected.
    pub fn execute<'c, 's, S: IntoStatement<'s>>(&'c self, command: S, parameters: Parameters) -> Result<u64>
    where
        'c: 's,
    {
        let mut statement = command.into_statement(self)?;
        statement.bind(parameters)?;
        statement.execute()
    }

    /// Query a statement and return an iterator of {{RecordBatch}}.
    pub fn query_arrow<'s, 'i>(
        &self,
        statement: &'s mut Statement,
        parameters: Parameters,
    ) -> Result<Box<dyn Iterator<Item = Result<RecordBatch>> + 'i>>
    where
        's: 'i,
    {
        statement.bind(parameters)?;
        statement.query()
    }

    /// Query a statement and return an iterator of {{Row}}.
    pub fn query_rows<'s, 'i>(
        &self,
        statement: &'s mut Statement,
        parameters: Parameters,
    ) -> Result<Box<dyn Iterator<Item = Result<Row>> + 'i>>
    where
        's: 'i,
    {
        match self.query_arrow(statement, parameters) {
            Ok(iterator) => Ok(Box::new(Rows::from(iterator))),
            Err(e) => Err(e),
        }
    }

    /// Query a statement that is expected to return a single {{Row}}.
    ///
    /// Returns `Ok(None)` if the query returned no rows.
    /// If the query returns more than one row, the function will return an the first row and ignore the rest.
    pub fn query_row<'c, 's, S: IntoStatement<'s>>(&'c self, command: S, parameters: Parameters) -> Result<Option<Row>>
    where
        'c: 's,
    {
        let mut statement = command.into_statement(self)?;
        let mut rows = self.query_rows(&mut statement, parameters)?;
        match rows.next() {
            Some(Ok(row)) => Ok(Some(row)),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }

    /// Close the connection.
    ///
    /// Because a {{Statement}} borrows the connection, all statements must be dropped before calling `close()`.
    ///
    /// ```rust
    /// use squill_core::connection::Connection;
    ///
    /// let conn = Connection::open("mock://").unwrap();
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
    use crate::{params, NO_PARAM};

    #[test]
    fn test_connection() {
        // Test connection open
        assert!(Connection::open("unknown://").is_err());
        let conn = Connection::open("mock://").unwrap();
        assert_eq!(conn.driver_name(), "mock");

        // Test connection prepare
        assert!(conn.prepare("XINSERT").is_err());
        assert!(conn.prepare("SELECT 1").is_ok());

        // Test connection execute
        assert!(conn.execute("XINSERT", NO_PARAM).is_err());
        assert_eq!(conn.execute("INSERT 1", NO_PARAM).unwrap(), 1);
        assert!(conn.execute("SELECT 1", NO_PARAM).is_err()); // SELECT is not allowed for execute().
        assert!(conn.execute("INSERT ?", NO_PARAM).is_err()); // Number of parameters does not match the number of placeholders
        assert!(conn.execute(conn.prepare("INSERT 1").unwrap(), NO_PARAM).is_ok()); // using a prepared statement

        // Test connection query
        let mut stmt = conn.prepare("SELECT 1").unwrap();
        assert!(conn.query_arrow(&mut stmt, NO_PARAM).is_ok());
        assert!(conn.query_rows(&mut stmt, NO_PARAM).is_ok());
        assert!(conn.query_arrow(&mut stmt, params!("hello")).is_err());
        drop(stmt);
        let mut stmt = conn.prepare("INSERT 1").unwrap();
        assert!(conn.query_arrow(&mut stmt, NO_PARAM).is_err());
        drop(stmt);
        assert_eq!(conn.query_row("SELECT 1", NO_PARAM).unwrap().unwrap().get::<_, i32>(0), 1);
        assert!(conn.query_row("SELECT 0", NO_PARAM).unwrap().is_none());
        assert!(conn.query_row("SELECT -1", NO_PARAM).is_err());
        assert!(conn.query_row("SELECT X", NO_PARAM).is_err());

        // Test connection close
        assert!(conn.close().is_ok());
    }
}
