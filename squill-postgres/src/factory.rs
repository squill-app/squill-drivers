use crate::driver::Postgres;
use squill_core::driver::{DriverConnection, DriverFactory, DriverOptionsRef, Result};

pub(crate) struct PostgresFactory {}

impl DriverFactory for PostgresFactory {
    fn schemes(&self) -> &'static [&'static str] {
        &["postgres", "postgresql"]
    }

    /// Open a connection to a PostgreSQL database.
    fn open(&self, uri: &str, options: DriverOptionsRef) -> Result<Box<dyn DriverConnection>> {
        let client = postgres::Client::connect(uri, postgres::NoTls)?;
        Ok(Box::new(Postgres { client, options }))
    }
}
