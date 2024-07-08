use crate::connection::Connection;
use crate::driver::DriverStatement;
use crate::parameters::Parameters;
use crate::Result;
use arrow_array::RecordBatch;

/// A prepared statement.
///
/// A statement is a query that has been prepared for execution. It can be bound with parameters and executed.
pub struct Statement<'c> {
    pub(crate) inner: Box<dyn DriverStatement + 'c>,
}

impl Statement<'_> {
    pub fn bind(&mut self, parameters: Parameters) -> Result<()> {
        self.inner.bind(parameters)
    }

    pub fn execute(&mut self) -> Result<u64> {
        self.inner.execute()
    }

    pub fn query<'s: 'i, 'i>(&'s mut self) -> Result<Box<dyn Iterator<Item = Result<RecordBatch>> + 'i>> {
        self.inner.query()
    }

    pub fn query_with_params<'s: 'i, 'i>(
        &'s mut self,
        parameters: Parameters,
    ) -> Result<Box<dyn Iterator<Item = Result<RecordBatch>> + 's>> {
        self.inner.bind(parameters)?;
        self.inner.query()
    }
}

pub trait IntoStatement<'s> {
    fn into_statement<'c: 's>(self, conn: &'c Connection) -> Result<Statement<'s>>;
}

impl<'s> IntoStatement<'s> for &str {
    fn into_statement<'c: 's>(self, conn: &'c Connection) -> Result<Statement<'s>> {
        conn.prepare(self)
    }
}

impl<'s> IntoStatement<'s> for String {
    fn into_statement<'c: 's>(self, conn: &'c Connection) -> Result<Statement<'s>> {
        conn.prepare(self)
    }
}

impl<'s> IntoStatement<'s> for Statement<'s> {
    fn into_statement<'c: 's>(self, _conn: &'c Connection) -> Result<Statement<'s>> {
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use crate::connection::Connection;

    #[test]
    fn test_statement() {
        let conn = Connection::open("mock://").unwrap();
        let mut stmt = conn.prepare("CREATE TABLE test(id INT)").unwrap();
        assert!(stmt.execute().is_ok());

        let mut stmt = conn.prepare("SELECT * FROM employee").unwrap();
        let mut rows = stmt.query().unwrap();
        assert!(rows.next().is_none());
    }
}
