use lazy_static::lazy_static;
use std::sync::Arc;
use std::sync::Mutex;

use crate::driver::DriverConnection;
use crate::driver::DriverFactory;
use crate::Result;

lazy_static! {
    pub static ref DRIVER_FACTORIES: Factory = Factory { registered_factories: Mutex::new(Vec::new()) };
}

pub struct Factory {
    registered_factories: Mutex<Vec<Arc<Box<dyn DriverFactory>>>>,
}

impl Factory {
    pub fn register(driver: Box<dyn DriverFactory>) {
        DRIVER_FACTORIES.registered_factories.lock().unwrap().push(Arc::new(driver));
    }

    #[cfg(test)]
    pub fn unregister(scheme: &str) {
        let mut factories = DRIVER_FACTORIES.registered_factories.lock().unwrap();
        factories.retain(|f| !f.schemes().contains(&scheme));
    }

    pub fn open(uri: &str) -> Result<Box<dyn DriverConnection>> {
        match uri.split(':').next() {
            Some(scheme) => {
                let scheme_regex = regex::Regex::new("^[a-zA-Z][a-zA-Z0-9+.-]*$")?;
                if !scheme_regex.is_match(scheme) {
                    return Err(format!("Invalid driver scheme: {}", scheme).into());
                }
                match DRIVER_FACTORIES.find(scheme) {
                    Some(driver) => driver.open(uri),
                    None => Err(format!("No driver found for scheme: {}", scheme).into()),
                }
            }
            None => Err(format!("Invalid URI: {}", &uri).into()),
        }
    }

    fn find(&self, scheme: &str) -> Option<Arc<Box<dyn DriverFactory>>> {
        for driver_factory in self.registered_factories.lock().unwrap().iter() {
            if driver_factory.schemes().contains(&scheme) {
                return Some(driver_factory.clone());
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{MockDriverConnection, MockDriverFactory};

    #[test]
    fn test_register() {
        let mut mock_factory = MockDriverFactory::new();
        mock_factory.expect_schemes().returning(|| &["mock"]);
        mock_factory.expect_open().returning(|_| Ok(Box::new(MockDriverConnection::new())));
        Factory::register(Box::new(mock_factory));

        assert!(Factory::open("mock://").is_ok());
        assert!(Factory::open("unknown://").is_err());
        assert!(Factory::open("invalid/:://").is_err());

        Factory::unregister("mock");
    }

    /*


        struct MockDriverConnection {}

        impl DriverConnection for MockDriverConnection {
            fn driver_name(&self) -> &str {
                "mock"
            }

            fn prepare<'c, 's>(&'c self, _statement: &str) -> Result<Box<dyn DriverStatement + 's>>
            where
                'c: 's,
            {
                Ok(Box::new(MockDriverStatement {}))
            }

            fn close(self: Box<Self>) -> Result<()> {
                Ok(())
            }
        }

        struct MockDriverStatement {}

        impl DriverStatement for MockDriverStatement {
            fn bind(&mut self, _parameters: Parameters) -> Result<()> {
                Ok(())
            }

            fn execute(&mut self) -> Result<u64> {
                Ok(0)
            }

            fn query<'s>(&'s mut self) -> Result<Box<dyn Iterator<Item = Result<RecordBatch>> + 's>> {
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

    */
}
