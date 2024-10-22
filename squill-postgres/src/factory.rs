use crate::Postgres;
use squill_core::driver::{DriverConnection, DriverFactory, Result};
use std::{cell::RefCell, rc::Rc};

pub(crate) struct PostgresFactory {}

impl DriverFactory for PostgresFactory {
    fn schemes(&self) -> &'static [&'static str] {
        &["postgres", "postgresql"]
    }

    /// Open a connection to a PostgreSQL database.
    fn open(&self, uri: &str) -> Result<Box<dyn DriverConnection>> {
        let client = postgres::Client::connect(uri, postgres::NoTls)?;
        Ok(Box::new(Postgres { client: Rc::new(RefCell::new(client)) }))
    }
}
