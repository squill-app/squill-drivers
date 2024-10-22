use squill_core::error::Error;

/// Convert a `postgres::Error` into a `squill_core::error::Error`.
pub(crate) fn into_driver_error(postgres_error: postgres::Error) -> Error {
    Error::DriverError { error: Box::new(postgres_error) }
}
