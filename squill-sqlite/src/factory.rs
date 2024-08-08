use crate::Sqlite;
use crate::SqliteOptions;
use crate::DRIVER_NAME;
use squill_core::driver::{DriverConnection, DriverFactory, Result};
use squill_core::Error;
use std::sync::Arc;

pub(crate) struct SqliteFactory {}

impl DriverFactory for SqliteFactory {
    fn schemes(&self) -> &'static [&'static str] {
        &[DRIVER_NAME]
    }

    /// Open a connection to a SQLite database.
    ///
    /// The URI must be in the format as defined at https://www.sqlite.org/uri.html` except for the scheme that is
    /// expected to be `sqlite` instead of `file`.
    fn open(&self, uri: &str) -> Result<Box<dyn DriverConnection>> {
        // Replace the scheme `sqlite` by `file` as expected by the SQLite driver.
        let mut sqlite_uri = uri.to_string();
        sqlite_uri.replace_range(0.."sqlite:".len(), "file:");

        // SQLite is expecting to have some flags set when opening a database even if the `mode` URI parameter will
        // eventually override them.
        let conn = rusqlite::Connection::open_with_flags(
            &sqlite_uri,
            rusqlite::OpenFlags::SQLITE_OPEN_URI
                | rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        )?;

        // Parse additional the URI parameters specific to the implementation of the driver.
        let mut options = SqliteOptions::default();
        let parsed_uri = url::Url::parse(&sqlite_uri).map_err(|_| Error::InvalidUri { uri: uri.to_string() })?;
        parsed_uri.query_pairs().try_for_each(|(key, value)| {
            if key == "max_batch_rows" {
                if let Ok(value) = value.parse::<usize>() {
                    options.max_batch_rows = value;
                } else {
                    return Err(Error::InvalidUri { uri: uri.to_string() });
                }
            } else if key == "max_batch_bytes" {
                if let Ok(max_batch_bytes) = value.parse::<bytesize::ByteSize>() {
                    options.max_batch_bytes = max_batch_bytes.0 as usize;
                } else {
                    return Err(Error::InvalidUri { uri: uri.to_string() });
                }
            }
            Ok(())
        })?;
        Ok(Box::new(Sqlite { conn, options: Arc::new(options) }))
    }
}

#[cfg(test)]
mod tests {
    use ctor::ctor;
    use squill_core::factory::Factory;

    #[ctor]
    fn before_all() {
        crate::register_driver();
    }

    #[test]
    fn test_open_memory() {
        assert!(Factory::open("sqlite::memory:").is_ok());
        assert!(Factory::open("sqlite:memdb1?mode=memory&cache=shared").is_ok());
    }

    #[test]
    fn test_open_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.db");

        // trying to open a file that does not exist in read-only should fail
        assert!(Factory::open(&format!("sqlite://{}?mode=ro", Factory::to_uri_path(&file_path))).is_err());
        // trying to open a file that does not exist in read-write should create it
        assert!(Factory::open(&format!("sqlite://{}?mode=rwc", Factory::to_uri_path(&file_path))).is_ok());
        // now that the file exists, opening it in read-only should work
        assert!(Factory::open(&format!("sqlite://{}?mode=ro", Factory::to_uri_path(&file_path))).is_ok());
    }
}
