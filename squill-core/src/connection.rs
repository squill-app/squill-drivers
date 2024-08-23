use crate::driver::DriverConnection;
use crate::factory::Factory;
use crate::parameters::Parameters;
use crate::rows::{Row, Rows};
use crate::statement::{Statement, StatementRef};
use crate::{Error, Result};
use arrow_array::RecordBatch;

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

    /// Prepare a statement.
    ///
    /// Return a [Statement] that can be later used to by `query` or `execute` functions. A prepared statement can be
    /// used multiple times with different parameters.    
    pub fn prepare<S: AsRef<str>>(&self, statement: S) -> Result<Statement<'_>> {
        Ok(Statement { inner: self.inner.prepare(statement.as_ref())? })
    }

    /// Execute a statement.
    ///
    /// This function can be called either with a prepared statement or a string as a command.
    ///
    /// Returns the number of rows affected.
    pub fn execute<'c, 'r, 's: 'r, S: Into<StatementRef<'r, 's>>>(
        &'c self,
        command: S,
        parameters: Option<Parameters>,
    ) -> Result<u64>
    where
        'c: 's,
    {
        // A closure to bind parameters and execute the statement in order to avoid code duplication.
        let bind_and_execute = |statement: &mut Statement<'s>| -> Result<u64> {
            if let Some(parameters) = parameters {
                statement.bind(parameters)?;
            }
            statement.execute()
        };
        match command.into() {
            StatementRef::Str(s) => {
                let mut statement = self.prepare(s)?;
                bind_and_execute(&mut statement)
            }
            StatementRef::Statement(statement) => bind_and_execute(statement),
        }
    }

    /// Query a statement and return an iterator of [RecordBatch].
    pub fn query_arrow<'s, 'i>(
        &self,
        statement: &'s mut Statement,
        parameters: Option<Parameters>,
    ) -> Result<Box<dyn Iterator<Item = Result<RecordBatch>> + 'i>>
    where
        's: 'i,
    {
        if parameters.is_some() {
            statement.bind(parameters.unwrap())?;
        }
        statement.query()
    }

    /// Query a statement and return an iterator of [Row].
    pub fn query_rows<'s, 'i>(
        &self,
        statement: &'s mut Statement,
        parameters: Option<Parameters>,
    ) -> Result<Box<dyn Iterator<Item = Result<Row>> + 'i>>
    where
        's: 'i,
    {
        match self.query_arrow(statement, parameters) {
            Ok(iterator) => Ok(Box::new(Rows::from(iterator))),
            Err(e) => Err(e),
        }
    }

    /// Query a statement that is expected to return a single [Row].
    ///
    /// Returns `Ok(None)` if the query returned no rows.
    /// If the query returns more than one row, the function will return an the first row and ignore the rest.
    pub fn query_row<'c, 'r, 's: 'r, S: Into<StatementRef<'r, 's>>>(
        &'c self,
        command: S,
        parameters: Option<Parameters>,
    ) -> Result<Option<Row>>
    where
        'c: 's,
    {
        // A closure to bind parameters and execute the statement in order to avoid code duplication.
        let query_and_fetch_first = |statement: &mut Statement<'s>| -> Result<Option<Row>> {
            let mut rows = self.query_rows(statement, parameters)?;
            match rows.next() {
                Some(Ok(row)) => Ok(Some(row)),
                Some(Err(e)) => Err(e),
                None => Ok(None),
            }
        };
        match command.into() {
            StatementRef::Str(s) => {
                let mut statement = self.prepare(s)?;
                query_and_fetch_first(&mut statement)
            }
            StatementRef::Statement(statement) => query_and_fetch_first(statement),
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
    use crate::params;

    #[test]
    fn test_connection_prepare() {
        let conn = Connection::open("mock://").unwrap();
        assert!(conn.prepare("XINSERT").is_err());
        assert!(conn.prepare("SELECT 1").is_ok());
    }

    #[test]
    fn test_connection_query_rows() {
        let conn = Connection::open("mock://").unwrap();

        // some rows
        let mut stmt = conn.prepare("SELECT 2").unwrap();
        let mut rows = conn.query_rows(&mut stmt, None).unwrap();
        assert_eq!(rows.next().unwrap().unwrap().get::<_, i32>(0), 1);
        assert_eq!(rows.next().unwrap().unwrap().get::<_, i32>(0), 2);
        assert!(rows.next().is_none());

        // no rows
        let mut stmt = conn.prepare("SELECT 0").unwrap();
        let mut rows = conn.query_rows(&mut stmt, None).unwrap();
        assert!(rows.next().is_none());

        // error on first call to next()
        let mut stmt = conn.prepare("SELECT -1").unwrap();
        let mut rows = conn.query_rows(&mut stmt, None).unwrap();
        assert!(matches!(rows.next(), Some(Err(_))));

        // error on call to query_rows()
        let mut stmt = conn.prepare("SELECT X").unwrap();
        assert!(conn.query_rows(&mut stmt, None).is_err());
    }

    #[test]
    fn test_connection_query_row() {
        let conn = Connection::open("mock://").unwrap();

        assert_eq!(conn.query_row("SELECT 2", None).unwrap().unwrap().get::<_, i32>(0), 1);
        assert_eq!(conn.query_row("SELECT 1", None).unwrap().unwrap().get::<_, i32>(0), 1);
        assert!(conn.query_row("SELECT 0", None).unwrap().is_none());
        assert!(conn.query_row("SELECT -1", None).is_err());
        assert!(conn.query_row("SELECT X", None).is_err());

        let mut stmt = conn.prepare("SELECT 1").unwrap();
        assert_eq!(conn.query_row(&mut stmt, None).unwrap().unwrap().get::<_, i32>(0), 1);
        assert_eq!(conn.query_row(&mut stmt, None).unwrap().unwrap().get::<_, i32>(0), 1);
    }

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
        assert!(conn.execute("XINSERT", None).is_err());
        assert_eq!(conn.execute("INSERT 1", None).unwrap(), 1);
        assert!(conn.execute("SELECT 1", None).is_err()); // SELECT is not allowed for execute().
        assert!(conn.execute("INSERT ?", params!(1, 2)).is_err()); // Number of parameters does not match the number of placeholders
        let mut stmt = conn.prepare("INSERT 1").unwrap();
        assert!(conn.execute(&mut stmt, None).is_ok()); // using a prepared statement
        drop(stmt);

        // Test connection query
        let mut stmt = conn.prepare("SELECT 1").unwrap();
        assert!(conn.query_arrow(&mut stmt, None).is_ok());
        assert!(conn.query_rows(&mut stmt, None).is_ok());
        assert!(conn.query_arrow(&mut stmt, params!("hello")).is_err());
        drop(stmt);
        let mut stmt = conn.prepare("INSERT 1").unwrap();
        assert!(conn.query_arrow(&mut stmt, None).is_err());
        drop(stmt);
        assert_eq!(conn.query_row("SELECT 1", None).unwrap().unwrap().get::<_, i32>(0), 1);
        assert!(conn.query_row("SELECT 0", None).unwrap().is_none());
        assert!(conn.query_row("SELECT -1", None).is_err());
        assert!(conn.query_row("SELECT X", None).is_err());

        // Test connection close
        assert!(conn.close().is_ok());
    }
}
