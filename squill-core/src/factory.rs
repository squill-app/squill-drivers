use lazy_static::lazy_static;
use std::sync::Arc;
use std::sync::Mutex;

use crate::drivers::DriverConnection;
use crate::drivers::DriverFactory;
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
