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
        if let Some(captures) = regex::Regex::new("^([a-zA-Z][a-zA-Z0-9+.-]*):")?.captures(uri) {
            if captures.len() > 1 {
                let scheme = captures.get(1).unwrap().as_str();
                match DRIVER_FACTORIES.find(scheme) {
                    Some(driver) => return driver.open(uri),
                    None => return Err(format!("No driver found for scheme: {}", scheme).into()),
                }
            }
        }
        Err(format!("Invalid URI: {}", &uri).into())
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
        mock_factory.expect_schemes().returning(|| &["mock-factory-register"]);
        mock_factory.expect_open().returning(|_| Ok(Box::new(MockDriverConnection::new())));
        Factory::register(Box::new(mock_factory));

        assert!(Factory::open("mock-factory-register://").is_ok());
        assert!(Factory::open("unknown://").is_err());
        assert!(Factory::open("invalid/:://").is_err());
        assert!(Factory::open("").is_err());

        Factory::unregister("mock-factory-register");
    }
}
