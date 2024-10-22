use crate::driver::DriverConnection;
use crate::driver::DriverFactory;
use crate::error::Error;
use crate::Result;
use lazy_static::lazy_static;
use path_slash::PathExt;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

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

    #[cfg(any(test, feature = "mock"))]
    pub fn unregister(scheme: &str) {
        let mut factories = DRIVER_FACTORIES.registered_factories.lock().unwrap();
        factories.retain(|f| !f.schemes().contains(&scheme));
    }

    pub fn open(uri: &str) -> Result<Box<dyn DriverConnection>> {
        // Extract the scheme from the URI.
        if let Some(captures) = regex::Regex::new("^([a-zA-Z][a-zA-Z0-9+.-]*):")?.captures(uri) {
            // It is safe to unwrap because the regex has matched and the capture group must be present otherwise the
            // regex would not match.
            let scheme = captures.get(1).unwrap().as_str();
            match DRIVER_FACTORIES.find(scheme) {
                Some(driver) => return driver.open(uri).map_err(Error::from),
                None => return Err(Error::DriverNotFound { scheme: scheme.to_string() }),
            }
        }
        Err(Error::InvalidUri { uri: uri.to_string(), reason: "No scheme found".to_string() })
    }

    /// Make a file path suitable for an URI.
    ///
    /// - On Windows, the path is converted to a slash-separated path and a leading slash is added if needed.
    /// - On Unix, the path is returned as is.
    pub fn to_uri_path(path: &PathBuf) -> String {
        if cfg!(target_os = "windows") {
            if path.is_absolute() {
                // An absolute path for an URI is expected to start by / and not C:/
                format!("/{}", path.to_slash_lossy())
            } else {
                path.to_slash_lossy().to_string()
            }
        } else {
            path.to_string_lossy().into_owned()
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

    #[test]
    fn test_register() {
        assert!(Factory::open("mock://").is_ok());
        assert!(Factory::open("unknown://").is_err());
        assert!(Factory::open("invalid/:://").is_err());
        assert!(Factory::open("").is_err());
    }
}
