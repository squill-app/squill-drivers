use crate::parameters::Parameters;
use crate::Result;
use arrow_array::RecordBatch;

pub trait DriverConnection {
    /**
     * Get the name of the driver.
     */
    fn driver_name(&self) -> &str;

    fn prepare<'c>(&'c self, statement: &str) -> Result<Box<dyn DriverStatement + 'c>>;

    /**
     * Close the connection.
     */
    fn close(self: Box<Self>) -> Result<()>;
}

pub trait DriverStatement {
    fn bind(&mut self, parameters: Parameters) -> Result<()>;
    fn execute(&mut self) -> Result<u64>;
    fn query<'s>(&'s mut self) -> Result<Box<dyn Iterator<Item = Result<RecordBatch>> + 's>>;
}

pub trait DriverFactory: Sync + Send {
    /// Get the schemes associated with the driver.
    fn schemes(&self) -> &'static [&'static str];
    fn open(&self, uri: &str) -> Result<Box<dyn DriverConnection>>;
}
