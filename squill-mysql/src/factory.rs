use crate::errors::driver_error;
use crate::{MySql, DRIVER_NAME};
use squill_core::driver::{DriverConnection, DriverFactory, Result};
use squill_core::Error;

pub(crate) struct MySqlFactory {}

impl DriverFactory for MySqlFactory {
    fn schemes(&self) -> &'static [&'static str] {
        &[DRIVER_NAME]
    }

    /// FIXME: SHOULD USE THE URI PARSING ERROR REASON (_url_error)
    /// Open a connection to a MySQL database.
    fn open(&self, uri: &str) -> Result<Box<dyn DriverConnection>> {
        let opts = mysql::Opts::from_url(uri)
            .map_err(|url_error| Error::InvalidUri { uri: uri.to_string(), reason: url_error.to_string() })?;
        let conn: mysql::Conn = mysql::Conn::new(opts).map_err(driver_error)?;
        Ok(Box::new(MySql { conn: std::cell::RefCell::new(conn) }))
    }
}

#[cfg(test)]
mod tests {
    use ctor::ctor;
    use squill_core::factory::Factory;
    use tokio_test::assert_ok;

    #[ctor]
    fn before_all() {
        crate::register_driver();
    }

    #[test]
    fn test_mysql_factory() {
        let ci_database_uri = env!("CI_MYSQL_URI");
        assert_ok!(Factory::open(ci_database_uri));
    }
}
