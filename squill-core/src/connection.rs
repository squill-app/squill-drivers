use crate::driver::{DriverConnection, DriverStatement};
use crate::factory::Factory;
use crate::parameters::Parameters;
use crate::Result;
use arrow_array::RecordBatch;

pub struct Connection<'c> {
    inner: Box<dyn DriverConnection>,
    phantom: std::marker::PhantomData<&'c ()>,
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
impl<'c> Connection<'c> {
    pub fn open(uri: &str) -> Result<Self> {
        let inner = Factory::open(uri)?;
        Ok(Self { inner, phantom: std::marker::PhantomData })
    }

    pub fn driver_name(&self) -> &str {
        self.inner.driver_name()
    }

    pub fn prepare<'s, S: AsRef<str>>(&'c self, statement: S) -> Result<PreparedStatement<'s>>
    where
        'c: 's,
    {
        Ok(PreparedStatement { inner: self.inner.prepare(statement.as_ref())? })
    }

    pub fn execute<'s, S: IntoStatement<'s>>(&'c self, command: S, parameters: Parameters) -> Result<u64>
    where
        'c: 's,
    {
        let mut statement = command.into_statement(self)?;
        statement.bind(parameters)?;
        statement.execute()
    }

    pub fn query<'s>(
        &'c self,
        statement: &'s mut PreparedStatement<'c>,
        parameters: Parameters,
    ) -> Result<Box<dyn Iterator<Item = Result<RecordBatch>> + 's>>
    where
        'c: 's,
    {
        statement.bind(parameters)?;
        statement.query()
    }

    pub fn close(self) -> Result<()> {
        self.inner.close()
    }
}

pub struct PreparedStatement<'s> {
    inner: Box<dyn DriverStatement + 's>,
}

impl<'s> PreparedStatement<'s> {
    pub fn bind(&mut self, parameters: Parameters) -> Result<()> {
        self.inner.bind(parameters)
    }

    pub fn execute(&mut self) -> Result<u64> {
        self.inner.execute()
    }

    pub fn query<'i>(&'s mut self) -> Result<Box<dyn Iterator<Item = Result<RecordBatch>> + 'i>>
    where
        's: 'i,
    {
        self.inner.query()
    }

    pub fn query_with_params(
        &'s mut self,
        parameters: Parameters,
    ) -> Result<Box<dyn Iterator<Item = Result<RecordBatch>> + 's>> {
        self.inner.bind(parameters)?;
        self.inner.query()
    }
}

pub trait IntoStatement<'s> {
    fn into_statement<'c: 's>(self, conn: &'c Connection) -> Result<PreparedStatement<'s>>;
}

impl<'s> IntoStatement<'s> for &str {
    fn into_statement<'c: 's>(self, conn: &'c Connection) -> Result<PreparedStatement<'s>> {
        conn.prepare(self)
    }
}

impl<'s> IntoStatement<'s> for String {
    fn into_statement<'c: 's>(self, conn: &'c Connection) -> Result<PreparedStatement<'s>> {
        conn.prepare(self)
    }
}

impl<'s> IntoStatement<'s> for PreparedStatement<'s> {
    fn into_statement<'c: 's>(self, _conn: &'c Connection) -> Result<PreparedStatement<'s>> {
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection() {
        let conn = Connection::open("mock://").unwrap();
        assert_eq!(conn.driver_name(), "mock");
        assert!(conn.close().is_ok());
    }

    #[test]
    fn test_statement() {
        let conn = Connection::open("mock://").unwrap();
        let mut stmt = conn.prepare("CREATE TABLE test(id INT)").unwrap();
        assert!(stmt.execute().is_ok());

        let mut stmt = conn.prepare("SELECT * FROM employee").unwrap();
        assert!(stmt.query().is_ok());
    }
}
