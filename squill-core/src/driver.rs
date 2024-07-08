use crate::parameters::Parameters;
use crate::Result;
use arrow_array::RecordBatch;

#[cfg(test)]
use mockall::automock;

#[cfg_attr(test, automock)]
pub trait DriverConnection {
    /**
     * Get the name of the driver.
     */
    fn driver_name(&self) -> &str;

    fn prepare<'c, 's>(&'c self, statement: &str) -> Result<Box<dyn DriverStatement + 's>>
    where
        'c: 's;

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

#[cfg_attr(test, automock)]
pub trait DriverFactory: Sync + Send {
    /// Get the schemes associated with the driver.
    fn schemes(&self) -> &'static [&'static str];
    fn open(&self, uri: &str) -> Result<Box<dyn DriverConnection>>;
}
