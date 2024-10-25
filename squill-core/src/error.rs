/// Error type for library.
///
/// This library is defining 2 error types:
/// - {Error}: is the main error type for the library and the one the users of the library will interact with.
/// - {DriverError}: is the error type that the drivers will use to return errors. Only developers of drivers will
///   interact with this error type.
#[derive(Debug)]
pub enum Error {
    ArrowError {
        error: arrow_schema::ArrowError,
    },

    /// There is a constraint violation.
    /// This error is used when a constraint is violated. For example, when a unique constraint is violated.
    ConstraintViolation {
        error: Box<dyn std::error::Error + Send + Sync>,
    },

    /// The driver is reporting that there is no more space in the storage.
    StorageFull {
        error: Box<dyn std::error::Error + Send + Sync>,
    },

    DriverNotFound {
        scheme: String,
    },

    /// The driver is reporting that the input of a statement is invalid.
    InputError {
        message: String,
        input: String,
        offset: usize,
        error: Box<dyn std::error::Error + Send + Sync>,
    },

    InternalError {
        error: Box<dyn std::error::Error + Send + Sync>,
    },

    InvalidParameterCount {
        expected: usize,
        actual: usize,
    },

    InvalidType {
        expected: String,
        actual: String,
    },

    InvalidUri {
        uri: String,
        reason: String,
    },

    NotFound,

    OutOfBounds {
        index: usize,
    },

    /// The driver is reporting that it is out of memory.
    OutOfMemory {
        error: Box<dyn std::error::Error + Send + Sync>,
    },

    UnsupportedDataType {
        data_type: String,
    },

    /// An error that doesn't fit in any of the other error types.
    DriverError {
        error: Box<dyn std::error::Error + Send + Sync>,
    },
}

impl From<crate::driver::DriverError> for Error {
    fn from(err: Box<dyn std::error::Error + Send + Sync>) -> Self {
        match err.downcast::<Error>() {
            Ok(error) => *error,
            Err(error) => Error::InternalError { error },
        }
    }
}

impl From<regex::Error> for Error {
    fn from(e: regex::Error) -> Self {
        Error::InternalError { error: Box::new(e) }
    }
}

impl From<&str> for Error {
    fn from(e: &str) -> Self {
        Error::InternalError { error: e.into() }
    }
}

impl From<arrow_schema::ArrowError> for Error {
    fn from(e: arrow_schema::ArrowError) -> Self {
        Error::ArrowError { error: e }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::ArrowError { error } => write!(f, "{}", error),
            Error::ConstraintViolation { error } => write!(f, "{}", error),
            Error::StorageFull { error } => write!(f, "{}", error),
            Error::DriverError { error } => write!(f, "{}", error),
            Error::DriverNotFound { scheme } => write!(f, "No driver found for scheme: {}", scheme),
            Error::InputError { message, input, offset, error } => write!(
                f,
                "Input error: message: '{}', input: '{}', offset: {}, error: {}",
                message, input, offset, error
            ),
            Error::InternalError { error } => write!(f, "{}", error),
            Error::InvalidParameterCount { expected, actual } => {
                write!(f, "Invalid parameter count: expected {}, actual {}", expected, actual)
            }
            Error::InvalidType { expected, actual } => {
                write!(f, "Invalid type: expected '{}', actual '{}'", expected, actual)
            }
            Error::InvalidUri { uri, reason } => write!(f, "Invalid URI: {} (reason: {})", uri, reason),
            Error::NotFound => write!(f, "Not found"),
            Error::OutOfBounds { index } => write!(f, "Out of bounds index {}", index),
            Error::OutOfMemory { error } => write!(f, "{}", error),
            Error::UnsupportedDataType { data_type } => write!(f, "Unsupported type: {}", data_type),
        }
    }
}

impl std::error::Error for Error {}
