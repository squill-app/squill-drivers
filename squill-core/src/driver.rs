use crate::parameters::Parameters;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use std::sync::Arc;

#[cfg(any(test, feature = "mock"))]
use mockall::automock;

/// The error type that the drivers will use to return errors.
///
/// It's a pass-through error type that the drivers will use to return errors. Because each driver may have to deal with
/// specific error types coming from the underlying crate used to interact with the database, the drivers will have to
/// convert those errors to this error type.
///
/// It doesn't prevent the drivers from using {{crate::error::Error}} when appropriate but it should be converted into
/// this error type using {{crate::error::Error::into}}.
pub type DriverError = Box<dyn std::error::Error + Send + Sync>;

pub type Result<T> = std::result::Result<T, DriverError>;

#[cfg_attr(any(test, feature = "mock"), automock)]
pub trait DriverConnection {
    /// Get the name of the driver.
    ///
    /// The name of the driver should be one of the schemes used to register the driver with the factory but it's not
    /// enforced. This name if mostly intended for logging and debugging purposes.
    fn driver_name(&self) -> &str;

    /// Prepare a statement for execution.
    ///
    /// If the statement used parameters, the statement should be prepared with placeholders for the parameters. The
    /// placeholders themselves are depending on the driver implementation. For example, the placeholders could be
    /// `$1, $2, ...` when using PostgtreSQL or `?` when using SQLite and MySQL.
    fn prepare<'c, 's>(&'c mut self, statement: &str) -> Result<Box<dyn DriverStatement + 's>>
    where
        'c: 's;

    /// Close the connection.
    ///
    /// Since the connection may be borrowed, the connection should be closed when the last reference to the connection
    /// is dropped.
    fn close(self: Box<Self>) -> Result<()>;
}

/// A prepared statement ready to be executed.
///
/// A prepared statement can be executed multiple times with different parameters.
#[cfg_attr(any(test, feature = "mock"), automock)]
pub trait DriverStatement {
    /// Execute the statement.
    ///
    /// The number of parameters must match the number of placeholders in the statement otherwise an error will be
    /// returned.
    ///
    /// Returns the number of rows affected by the statement.
    /// Executing a statement that starts with "SELECT" my return an error depending on the driver implementation.
    fn execute(&mut self, parameters: Option<Parameters>) -> Result<u64>;

    /// Execute a `SELECT` statement.
    ///
    /// The number of parameters must match the number of placeholders in the statement otherwise an error will be
    /// returned.
    ///
    /// Returns an iterator over the record batches returned by the statement.
    fn query<'s>(
        &'s mut self,
        parameters: Option<Parameters>,
    ) -> Result<Box<dyn Iterator<Item = Result<RecordBatch>> + 's>>;

    /// Get the schema of the last [`query`](Self::query) execution of the statement.
    ///
    /// Returns the schema of the record batches from the last [`query`](Self::query) execution.
    /// This function is useful when a statement does not return any rows but the schema is still needed.
    ///
    /// Because this function is using a reference to the statement, it's not possible to call this function while the
    /// iterator returned by [`query`](Self::query) is still alive. This function should be called after the iterator
    /// returned by [`query`](Self::query) is consumed at least once and after the iterator is dropped.
    ///
    /// WARNING: This function may panic if the statement was not queried before calling this function or if the
    /// iterator returned by [`query`](Self::query) was not consumed at least once.
    fn schema(&self) -> SchemaRef;
}

#[cfg_attr(any(test, feature = "mock"), automock)]
pub trait DriverFactory: Sync + Send {
    /// Get the schemes associated with the driver.
    fn schemes(&self) -> &'static [&'static str];
    fn open(&self, uri: &str, options: DriverOptionsRef) -> Result<Box<dyn DriverConnection>>;
}

/// The options that can be used by any driver.
pub struct DriverOptions {
    /// The maximum number rows that can be written in a single batch (default is 10,000 rows).
    pub max_batch_rows: usize,

    /// The maximum number of bytes that can be written in a single batch (default is 1MB).
    pub max_batch_bytes: usize,
}

impl Default for DriverOptions {
    fn default() -> Self {
        Self { max_batch_rows: 1_0000, max_batch_bytes: 1_000_000 }
    }
}

pub type DriverOptionsRef = Arc<DriverOptions>;

#[cfg(any(test, feature = "mock"))]
#[ctor::ctor]
fn init() {
    crate::mock::register_driver();
}

#[cfg(any(test, feature = "mock"))]
#[ctor::dtor]
fn cleanup() {
    crate::factory::Factory::unregister("mock");
}
