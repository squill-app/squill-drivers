use rusqlite::ffi::ErrorCode;
use squill_core::error::Error;

/// Convert a `rusqlite::Error` into a `squill_core::error::Error`.
pub(crate) fn driver_error(rusqlite_error: rusqlite::Error) -> Error {
    match rusqlite_error {
        rusqlite::Error::QueryReturnedNoRows => Error::NotFound,
        rusqlite::Error::SqliteFailure(e, _) => match e.code {
            ErrorCode::ConstraintViolation => Error::ConstraintViolation { error: Box::new(rusqlite_error) },
            ErrorCode::DiskFull => Error::StorageFull { error: Box::new(rusqlite_error) },
            ErrorCode::OutOfMemory => Error::OutOfMemory { error: Box::new(rusqlite_error) },
            _ => Error::DriverError { error: Box::new(rusqlite_error) },
        },
        _ => Error::DriverError { error: Box::new(rusqlite_error) },
    }
}
