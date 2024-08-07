use crate::DuckDB;
use crate::IN_MEMORY_URI_PATH;
use squill_core::driver::Result;
use squill_core::driver::{DriverConnection, DriverFactory};
use squill_core::Error;

pub(crate) struct DuckDBFactory {}

impl DriverFactory for DuckDBFactory {
    fn schemes(&self) -> &'static [&'static str] {
        &["duckdb"]
    }

    fn open(&self, uri: &str) -> Result<Box<dyn DriverConnection>> {
        let parsed_uri = url::Url::parse(uri).map_err(|_| Error::InvalidUri { uri: uri.to_string() })?;
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
        } else if cfg!(target_os = "windows") {
            path = path.char_indices().nth(1).map_or("", |(i, _)| &path[i..]);
        }
        let conn = duckdb::Connection::open_with_flags(path, config)?;
        Ok(Box::new(DuckDB { conn }))
    }
}
