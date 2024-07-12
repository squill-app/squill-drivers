use crate::Sqlite;
use crate::DRIVER_NAME;
use crate::IN_MEMORY_URI;
use crate::IN_MEMORY_URI_PATH;
use squill_core::driver::{DriverConnection, DriverFactory, Result};
use squill_core::Error;

pub(crate) struct SqliteFactory {}

impl DriverFactory for SqliteFactory {
    fn schemes(&self) -> &'static [&'static str] {
        &[DRIVER_NAME]
    }

    fn open(&self, uri: &str) -> Result<Box<dyn DriverConnection>> {
        let parsed_uri = url::Url::parse(uri).map_err(|_| Error::InvalidUri { uri: uri.to_string() })?;
        let mut sqlite_uri = uri.to_string();
        // SQLite id expecting to have some flags set when opening a database even if the `mode` URI parameter will
        // eventually override them.
        let mut flags = rusqlite::OpenFlags::SQLITE_OPEN_URI | rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE;
        if parsed_uri.path() == IN_MEMORY_URI_PATH {
            sqlite_uri.replace_range(0..IN_MEMORY_URI.len(), "file::memory:");
        } else {
            flags.insert(rusqlite::OpenFlags::SQLITE_OPEN_CREATE);
            sqlite_uri.replace_range(0.."sqlite:".len(), "file:");
        }
        let conn = rusqlite::Connection::open_with_flags(sqlite_uri, flags)?;
        Ok(Box::new(Sqlite { conn }))
    }
}
