use crate::driver::DriverConnection;
use crate::factory::Factory;
use crate::parameters::Parameters;
use crate::statement::{IntoStatement, Statement};
use crate::Result;
use arrow_array::RecordBatch;

pub struct Connection {
    inner: Box<dyn DriverConnection>,
}

/// A connection to a data source.
///
/// ```rust,ignore
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

    pub fn driver_name(&self) -> &str {
        self.inner.driver_name()
    }

    pub fn prepare<S: AsRef<str>>(&self, statement: S) -> Result<Statement<'_>> {
        Ok(Statement { inner: self.inner.prepare(statement.as_ref())? })
    }

    pub fn execute<'c, 's, S: IntoStatement<'s>>(&'c self, command: S, parameters: Parameters) -> Result<u64>
    where
        'c: 's,
    {
        let mut statement = command.into_statement(self)?;
        statement.bind(parameters)?;
        statement.execute()
    }

    pub fn query<'s, 'i>(
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

    pub fn close(self) -> Result<()> {
        self.inner.close()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{MockDriverConnection, MockDriverFactory};

    #[test]
    fn test_connection() {
        let mut mock_factory = MockDriverFactory::new();
        mock_factory.expect_schemes().returning(|| &["mock-connection-open"]);
        mock_factory.expect_open().returning(|_| {
            let mut mock_conn = MockDriverConnection::new();
            mock_conn.expect_driver_name().return_const("mock".to_string());
            mock_conn.expect_close().returning(|| Ok(()));
            Ok(Box::new(mock_conn))
        });
        Factory::register(Box::new(mock_factory));

        let conn = Connection::open("mock-connection-open://").unwrap();
        assert_eq!(conn.driver_name(), "mock");
        assert!(conn.close().is_ok());

        Factory::unregister("mock-connection-open");
    }
}
