use squill_core::factory::Factory;

mod driver;
mod factory;
mod value;

/// The name of the driver for SQLite.
pub const DRIVER_NAME: &str = "sqlite";

/// The path in a URI for in-memory databases.
pub const IN_MEMORY_URI_PATH: &str = "/:memory:";

/// The URI for in-memory databases.
///
/// # Example
/// ```rust
/// # use squill_core::connection::Connection;
/// let conn = Connection::open("sqlite:///:in-memory:");
/// ```
pub const IN_MEMORY_URI: &str = "sqlite:///:memory:";

pub(crate) struct Sqlite {
    conn: rusqlite::Connection,
}

pub fn register_driver() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        Factory::register(Box::new(factory::SqliteFactory {}));
    });
}
