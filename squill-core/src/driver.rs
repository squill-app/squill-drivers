use crate::parameters::Parameters;
use arrow_array::RecordBatch;

use arrow_schema::SchemaRef;
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
    fn prepare<'c, 's>(&'c self, statement: &str) -> Result<Box<dyn DriverStatement + 's>>
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
    /// Bind the parameters to the statement.
    ///
    /// The latest bound parameters will be used when the statement is executed via {{execute}} or {{query}}.
    /// The number of parameters must match the number of placeholders in the statement otherwise an error will be
    /// returned.
    fn bind(&mut self, parameters: Parameters) -> Result<()>;

    /// Execute the statement.
    ///
    /// Returns the number of rows affected by the statement.
    /// Executing a statement that starts with "SELECT" my return an error depending on the driver implementation.
    fn execute(&mut self) -> Result<u64>;

    /// Execute a `SELECT` statement.
    ///
    /// Returns an iterator over the record batches returned by the statement.
    fn query<'s>(&'s mut self) -> Result<Box<dyn Iterator<Item = Result<RecordBatch>> + 's>>;

    /// Returns the underlying schema of the prepared statement.
    ///
    /// The schema is the schema of the result set returned by the statement.
    /// # Panics
    /// This method may panic if the method is called before the statement is executed via a call to {{query}}.
    fn schema(&self) -> SchemaRef;
}

#[cfg_attr(any(test, feature = "mock"), automock)]
pub trait DriverFactory: Sync + Send {
    /// Get the schemes associated with the driver.
    fn schemes(&self) -> &'static [&'static str];
    fn open(&self, uri: &str) -> Result<Box<dyn DriverConnection>>;
}

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
