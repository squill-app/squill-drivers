use crate::connection::Connection;
use crate::driver::DriverStatement;
use crate::parameters::Parameters;
use crate::{Error, Result};
use arrow_array::RecordBatch;

/// A prepared statement.
///
/// A statement is a query that has been prepared for execution. It can be bound with parameters and executed.
pub struct Statement<'c> {
    pub(crate) inner: Box<dyn DriverStatement + 'c>,
}

impl Statement<'_> {
    pub fn bind(&mut self, parameters: Parameters) -> Result<()> {
        self.inner.bind(parameters).map_err(Error::from)
    }

    pub fn execute(&mut self) -> Result<u64> {
        self.inner.execute().map_err(Error::from)
    }

    pub fn query<'s: 'i, 'i>(&'s mut self) -> Result<Box<dyn Iterator<Item = Result<RecordBatch>> + 'i>> {
        match self.inner.query() {
            Ok(iterator) => {
                let iterator = iterator.map(|result| result.map_err(Error::from));
                Ok(Box::new(iterator))
            }
            Err(e) => Err(Error::from(e)),
        }
    }

    pub fn query_with_params<'s: 'i, 'i>(
        &'s mut self,
        parameters: Parameters,
    ) -> Result<Box<dyn Iterator<Item = Result<RecordBatch>> + 's>> {
        self.inner.bind(parameters)?;
        self.query()
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

    #[test]
    fn test_statement() {
        // todo!("Implement test_statement")
    }
}
