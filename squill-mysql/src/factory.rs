use crate::driver::MySql;
use crate::errors::driver_error;
use crate::DRIVER_NAME;
use squill_core::driver::{DriverConnection, DriverFactory, DriverOptionsRef, Result};
use squill_core::Error;

pub(crate) struct MySqlFactory {}

impl DriverFactory for MySqlFactory {
    fn schemes(&self) -> &'static [&'static str] {
        &[DRIVER_NAME]
    }

    /// Open a connection to a MySQL database.
    fn open(&self, uri: &str, options: DriverOptionsRef) -> Result<Box<dyn DriverConnection>> {
        let opts = mysql::Opts::from_url(uri)
            .map_err(|url_error| Error::InvalidUri { uri: uri.to_string(), reason: url_error.to_string() })?;
        let conn: mysql::Conn = mysql::Conn::new(opts).map_err(driver_error)?;
        Ok(Box::new(MySql { conn, options }))
    }
}
