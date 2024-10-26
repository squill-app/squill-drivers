use crate::Sqlite;
use crate::DRIVER_NAME;
use squill_core::driver::{DriverConnection, DriverFactory, DriverOptionsRef, Result};
use squill_core::Error;

pub(crate) struct SqliteFactory {}

impl DriverFactory for SqliteFactory {
    fn schemes(&self) -> &'static [&'static str] {
        &[DRIVER_NAME]
    }

    /// Open a connection to a SQLite database.
    ///
    /// The URI must be in the format as defined at https://www.sqlite.org/uri.html` except for the scheme that is
    /// expected to be `sqlite` instead of `file`.
    fn open(&self, uri: &str, options: DriverOptionsRef) -> Result<Box<dyn DriverConnection>> {
        // Replace the scheme `sqlite` by `file` as expected by the SQLite driver.
        let mut sqlite_uri = uri.to_string();
        sqlite_uri.replace_range(0.."sqlite:".len(), "file:");

        // Parse URI parameters to set the options and connection open flags.
        let mut flags = rusqlite::OpenFlags::SQLITE_OPEN_URI;
        let parsed_uri = url::Url::parse(&sqlite_uri)
            .map_err(|e| Error::InvalidUri { uri: uri.to_string(), reason: e.to_string() })?;
        parsed_uri.query_pairs().try_for_each(|(key, value)| {
            if key == "mode" {
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
                    _ => {
                        return Err(Error::InvalidUri {
                            uri: uri.to_string(),
                            reason: "Invalid value for mode".to_string(),
                        })
                    }
                }
            }
            Ok(())
        })?;

        if !flags.contains(rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE)
            && !flags.contains(rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
        {
            // If the open flags do not specify a mode, we assume that the database is read-write.
            flags |= rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE;
        }

        Ok(Box::new(Sqlite { conn: rusqlite::Connection::open_with_flags(&sqlite_uri, flags)?, options }))
    }
}
