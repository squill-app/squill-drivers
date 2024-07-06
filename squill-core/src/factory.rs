use lazy_static::lazy_static;
use std::sync::Arc;
use std::sync::Mutex;

use crate::driver::DriverConnection;
use crate::driver::DriverFactory;
use crate::Result;

lazy_static! {
    pub static ref DRIVER_FACTORIES: Factory = Factory::new();
}

pub struct Factory {
    registered_factories: Mutex<Vec<Arc<Box<dyn DriverFactory>>>>,
}

impl Factory {
    fn new() -> Factory {
        Factory { registered_factories: Mutex::new(Vec::new()) }
    }

    fn find(&self, scheme: &str) -> Option<Arc<Box<dyn DriverFactory>>> {
        for driver_factory in self.registered_factories.lock().unwrap().iter() {
            if driver_factory.schemes().contains(&scheme) {
                return Some(driver_factory.clone());
            }
        }
        None
    }

    fn push(&self, driver: Box<dyn DriverFactory>) {
        self.registered_factories.lock().unwrap().push(Arc::new(driver));
    }

    pub fn register(driver: Box<dyn DriverFactory>) {
        DRIVER_FACTORIES.push(driver);
    }

    pub fn open(uri: &str) -> Result<Box<dyn DriverConnection>> {
        match uri.split(':').next() {
            Some(scheme) => match DRIVER_FACTORIES.find(scheme) {
                Some(driver) => driver.open(uri),
                None => Err(format!("No driver found for scheme: {}", scheme).into()),
            },
            None => Err(format!("Invalid URI: {}", &uri).into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::parameters::Parameters;

    use super::*;
    use arrow_array::RecordBatch;
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
