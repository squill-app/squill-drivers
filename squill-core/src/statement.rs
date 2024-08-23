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

/// An enum that can be either a string or a mutable reference to a statement.
///
/// This enum is used to allow some functions to accept either a string or a mutable reference to a statement.
///
/// # Example
/// ```rust
/// # use squill_core::connection::Connection;
/// # use squill_core::statement::{Statement, StatementRef};
/// fn my_function<'r, 's: 'r, S: Into<StatementRef<'r, 's>>>(statement: S) -> &'r str {
///     let statement_ref: StatementRef = statement.into();
///     match statement_ref {
///         StatementRef::Str(s) => s,
///         StatementRef::Statement(_statement) => "statement",
///     }
/// }
///
/// let conn = Connection::open("mock://").unwrap();
/// let mut statement = conn.prepare("SELECT 1").unwrap();
/// assert_eq!(my_function("SELECT 1"), "SELECT 1");
/// assert_eq!(my_function(&mut statement), "statement");
/// ```
pub enum StatementRef<'r, 's> {
    Str(&'r str),
    Statement(&'r mut Statement<'s>),
}

/// Conversion of a [std::str] reference to [StatementRef].
impl<'r, 's: 'r> From<&'s str> for StatementRef<'r, '_> {
    fn from(s: &'s str) -> Self {
        StatementRef::Str(s)
    }
}

/// Conversion of a [Statement] mutable reference to [StatementRef].
/// Create a `StatementRef` from a mutable reference to a statement.
impl<'r, 's> From<&'r mut Statement<'s>> for StatementRef<'r, 's> {
    fn from(statement: &'r mut Statement<'s>) -> Self {
        StatementRef::Statement(statement)
    }
}
