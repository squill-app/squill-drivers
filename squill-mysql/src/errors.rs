use squill_core::error::Error;

/// Convert a `mysql::Error` into a `squill_core::error::Error`.
pub(crate) fn driver_error(mysql_error: mysql::Error) -> Error {
    match mysql_error {
        mysql::Error::DriverError(driver_error) => match driver_error {
            mysql::DriverError::CouldNotConnect(_) => Error::ConnectionFailed { message: driver_error.to_string() },
            mysql::DriverError::Timeout => Error::Timeout,
            mysql::DriverError::ConnectTimeout => Error::Timeout,
            mysql::DriverError::MismatchedStmtParams(expected, provided) => {
                Error::InvalidParameterCount { expected: expected as usize, actual: provided }
            }
            _ => Error::DriverError { error: Box::new(driver_error) },
        },
        _ => Error::DriverError { error: Box::new(mysql_error) },
    }
}
