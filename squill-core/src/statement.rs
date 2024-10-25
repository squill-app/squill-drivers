use crate::driver::DriverStatement;
use crate::parameters::Parameters;
use crate::rows::{Row, Rows};
use crate::{Error, Result};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

/// A prepared statement.
///
/// A statement is a query that has been prepared for execution. It can be bound with parameters and executed.
pub struct Statement<'c> {
    pub(crate) inner: Box<dyn DriverStatement + 'c>,
}

impl Statement<'_> {
    pub fn execute(&mut self, parameters: Option<Parameters>) -> Result<u64> {
        self.inner.execute(parameters).map_err(Error::from)
    }

    pub fn query<'s: 'i, 'i>(
        &'s mut self,
        parameters: Option<Parameters>,
    ) -> Result<Box<dyn Iterator<Item = Result<RecordBatch>> + 'i>> {
        match self.inner.query(parameters) {
            Ok(iterator) => {
                let iterator = iterator.map(|result| result.map_err(Error::from));
                Ok(Box::new(iterator))
            }
            Err(e) => Err(Error::from(e)),
        }
    }

    /// Query a statement and return an iterator of [Row].
    pub fn query_rows<'s: 'i, 'i>(
        &'s mut self,
        parameters: Option<Parameters>,
    ) -> Result<Box<dyn Iterator<Item = Result<Row>> + 'i>> {
        match self.query(parameters) {
            Ok(iterator) => Ok(Box::new(Rows::from(iterator))),
            Err(e) => Err(e),
        }
    }

    /// Query a statement that is expected to return a single [Row].
    ///
    /// Returns `Ok(None)` if the query returned no rows.
    /// If the query returns more than one row, the function will return an the first row and ignore the rest.
    pub fn query_row(&mut self, parameters: Option<Parameters>) -> Result<Option<Row>> {
        let mut rows = self.query_rows(parameters)?;
        match rows.next() {
            Some(Ok(row)) => Ok(Some(row)),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }

    /// Query a statement that is expected to return a single row and map it to a value.
    ///
    /// Returns `Ok(None)` if the query returned no rows.
    /// If the query returns more than one row, the function will return an the first row and ignore the rest.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use squill_core::connection::Connection;
    /// struct TestUser {
    ///     id: i32,
    ///     username: String,
    /// }
    ///
    /// let mut conn = Connection::open("mock://").unwrap();
    ///
    /// // some rows
    /// let user = conn
    ///     .query_map_row("SELECT 1", None, |row| {
    ///         Ok(TestUser { id: row.get::<_, _>(0), username: row.get::<_, _>(1) })
    ///     })
    ///     .unwrap()
    ///     .unwrap();
    /// assert_eq!(user.id, 1);
    /// assert_eq!(user.username, "user1");
    /// ```
    pub fn query_map_row<F, T>(&mut self, parameters: Option<Parameters>, mapping_fn: F) -> Result<Option<T>>
    where
        F: FnOnce(Row) -> std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>,
    {
        match self.query_row(parameters)? {
            Some(row) => Ok(Some(mapping_fn(row)?)),
            None => Ok(None),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}
