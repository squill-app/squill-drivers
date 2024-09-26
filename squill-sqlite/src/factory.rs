use crate::Sqlite;
use crate::SqliteOptions;
use crate::DRIVER_NAME;
use squill_core::driver::{DriverConnection, DriverFactory, Result};
use squill_core::Error;
use std::sync::Arc;

/// Special path in an URI for an in-memory databases.
/// This form is not the expected URI path to be given to driver implementations but it is the path returned by the
/// [url] crate when parsing the URI.
/// The expected URI format are described at https://www.sqlite.org/inmemorydb.html
const IN_MEMORY_URI_PATH: &str = "/:memory:";

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

        // Parse URI parameters to set the options and connection open flags.
        let mut options = SqliteOptions::default();
        let mut flags = rusqlite::OpenFlags::SQLITE_OPEN_URI;
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
            } else if key == "mode" {
                // Despite using `SQLITE_OPEN_URI` the documentation is explicit about the flags that must include
                // one of the three combination below.
                // See https://www.sqlite.org/c3ref/open.html
                match value.as_ref() {
                    "ro" => flags |= rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
                    "rw" => flags |= rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
                    "rwc" => {
                        flags |= rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE
                    }
                    "memory" => {
                        flags |= rusqlite::OpenFlags::SQLITE_OPEN_MEMORY | rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                    }
                    _ => return Err(Error::InvalidUri { uri: uri.to_string() }),
                }
            }
            Ok(())
        })?;

        if parsed_uri.path() == IN_MEMORY_URI_PATH
            && !flags.contains(rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE)
            && !flags.contains(rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
        {
            // For an in-memory database we need to specify the mode to be read-write.
            flags |= rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE;
        }

        Ok(Box::new(Sqlite {
            conn: rusqlite::Connection::open_with_flags(&sqlite_uri, flags)?,
            options: Arc::new(options),
        }))
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
    fn test_open_memory() {
        assert_ok!(Factory::open("sqlite::memory:"));
        assert_ok!(Factory::open("sqlite:memdb1?mode=memory&cache=shared"));
    }

    #[test]
    fn test_open_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.db");

        // trying to open a file that does not exist in read-only should fail
        assert!(Factory::open(&format!("sqlite://{}?mode=ro", Factory::to_uri_path(&file_path))).is_err());
        // trying to open a file that does not exist in read-write should create it
        assert_ok!(Factory::open(&format!("sqlite://{}?mode=rwc", Factory::to_uri_path(&file_path))));
        // now that the file exists, opening it in read-only should work
        assert_ok!(Factory::open(&format!("sqlite://{}?mode=ro", Factory::to_uri_path(&file_path))));
    }
}
