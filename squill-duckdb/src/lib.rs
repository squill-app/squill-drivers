#![doc = include_str!("../README.md")]
use squill_core::factory::Factory;

mod driver;
mod factory;
mod statement;
mod values;

/// The name of the driver for DuckDB.
pub const DRIVER_NAME: &str = "duckdb";

/// The path in a URI for in-memory databases.
pub const IN_MEMORY_URI_PATH: &str = "/:memory:";

/// The URI for in-memory databases.
///
/// # Example
/// ```rust
/// # use squill_core::connection::Connection;
/// let conn = Connection::open("duckdb:///:in-memory:?threads=4&max_memory=2GB");
/// ```
pub const IN_MEMORY_URI: &str = "duckdb:///:memory:";

pub(crate) struct DuckDB {
    conn: duckdb::Connection,
}

pub fn register_driver() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        Factory::register(Box::new(factory::DuckDBFactory {}));
    });
}
