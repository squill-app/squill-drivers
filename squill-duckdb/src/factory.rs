use squill_core::drivers::{ DriverConnection, DriverFactory };
use squill_core::Result;
use crate::DuckDB;
use crate::IN_MEMORY_URI_PATH;

pub(crate) struct DuckDBFactory {}

impl DriverFactory for DuckDBFactory {
    fn schemes(&self) -> &'static [&'static str] {
        &["duckdb"]
    }

    fn open(&self, uri: &str) -> Result<Box<dyn DriverConnection>> {
        let parsed_uri = url::Url::parse(uri)?;
        let mut path = parsed_uri.path();
        // Initialization of the configuration from the URI query parameters
        // See: https://duckdb.org/docs/configuration/overview.html#configuration-reference
        let mut config = duckdb::Config::default();
        for (key, value) in parsed_uri.query_pairs() {
            config = config.with(key.as_ref(), value.as_ref())?;
        }
        if parsed_uri.path() == IN_MEMORY_URI_PATH {
            // The path is the URI starts with a `/` but the duckdb::Connection::open_with_flags expects just ":memory:"
            // for in memory databases.
            path = ":memory:";
        }
        let conn = duckdb::Connection::open_with_flags(path, config)?;
        Ok(Box::new(DuckDB { conn }))
    }
}
