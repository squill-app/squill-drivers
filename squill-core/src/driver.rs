use crate::parameters::Parameters;
use crate::Result;
use arrow_array::RecordBatch;

pub trait DriverConnection {
    /**
     * Get the name of the driver.
     */
    fn driver_name(&self) -> &str;

    /**
     * Close the connection.
     */
    fn close(self: Box<Self>) -> Result<()>;

    /**
     * Execute a query and return the number of rows affected.
     */
    fn execute(&self, query: String, parameters: Parameters) -> Result<u64>;

    /**
     * Execute a query and return the result set.
     */
    fn query<'c>(
        &'c self,
        statement: String,
        parameters: Parameters,
    ) -> Result<Box<dyn Iterator<Item = Result<RecordBatch>> + 'c>>;
}

pub trait DriverFactory: Sync + Send {
    /// Get the schemes associated with the driver.
    fn schemes(&self) -> &'static [&'static str];
    fn open(&self, uri: &str) -> Result<Box<dyn DriverConnection>>;
}
