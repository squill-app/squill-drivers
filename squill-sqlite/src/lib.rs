use squill_core::factory::Factory;

mod driver;
mod factory;
mod statement;
mod value;

/// The name of the driver for SQLite.
pub const DRIVER_NAME: &str = "sqlite";

/// URI for an in-memory databases.
///
/// # Example
/// ```rust
/// # use squill_core::connection::Connection;
/// # use squill_sqlite::IN_MEMORY_URI;
/// let conn = Connection::open(IN_MEMORY_URI);
/// ```
pub const IN_MEMORY_URI: &str = "sqlite::memory:";

pub(crate) struct SqliteOptions {
    /// The maximum number rows that can be written in a single batch.
    /// Default is 10,000 rows.
    max_batch_rows: usize,

    /// The maximum number of bytes that can be written in a single batch.
    /// Default is 1MB.
    max_batch_bytes: usize,
}

impl Default for SqliteOptions {
    fn default() -> Self {
        Self { max_batch_rows: 10000, max_batch_bytes: 1_000_000 }
    }
}

type SqliteOptionsRef = std::sync::Arc<SqliteOptions>;

pub(crate) struct Sqlite {
    conn: rusqlite::Connection,
    options: SqliteOptionsRef,
}

pub fn register_driver() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        Factory::register(Box::new(factory::SqliteFactory {}));
    });
}
