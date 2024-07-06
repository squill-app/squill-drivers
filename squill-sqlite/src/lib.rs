use squill_core::factory::Factory;
use squill_core::parameters::Parameters;
use squill_core::Result;
use value::Adapter;

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

impl Sqlite {
    fn prepare_and_bind(&self, statement: String, parameters: Parameters) -> Result<rusqlite::Statement> {
        let mut stmt = self.conn.prepare(&statement)?;
        let expected = stmt.parameter_count();
        match parameters {
            Parameters::None => {
                if expected > 0 {
                    return Err(Box::new(rusqlite::Error::InvalidParameterCount(expected, 0)));
                }
                Ok(stmt)
            }
            Parameters::Positional(values) => {
                if expected != values.len() {
                    return Err(Box::new(rusqlite::Error::InvalidParameterCount(expected, values.len())));
                }
                // The valid values for the index `in raw_bind_parameter` begin at `1`, and end at
                // [`Statement::parameter_count`], inclusive.
                for (index, value) in values.iter().enumerate() {
                    stmt.raw_bind_parameter(index + 1, Adapter(value))?;
                }
                Ok(stmt)
            }
        }
    }
}

pub fn register_driver() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        Factory::register(Box::new(factory::SqliteFactory {}));
    });
}
