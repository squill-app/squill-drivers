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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::factory::Factory;
    use ctor::ctor;

    struct MockDriverConnection {}

    impl DriverConnection for MockDriverConnection {
        fn driver_name(&self) -> &str {
            "mock"
        }

        fn close(self: Box<Self>) -> Result<()> {
            Ok(())
        }

        fn execute(&self, _query: String, _parameters: Parameters) -> Result<u64> {
            Ok(0)
        }

        fn query<'c>(
            &'c self,
            _statement: String,
            _parameters: Parameters,
        ) -> Result<Box<dyn Iterator<Item = Result<RecordBatch>> + 'c>> {
            Ok(Box::new(std::iter::empty()))
        }
    }

    struct MockDriverFactory {}

    impl DriverFactory for MockDriverFactory {
        fn schemes(&self) -> &'static [&'static str] {
            &["mock"]
        }

        fn open(&self, _uri: &str) -> Result<Box<dyn DriverConnection>> {
            Ok(Box::new(MockDriverConnection {}))
        }
    }

    #[ctor]
    fn register_mock_driver() {
        Factory::register(Box::new(MockDriverFactory {}));
    }
}
