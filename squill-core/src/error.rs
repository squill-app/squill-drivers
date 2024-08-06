/// Error type for library.
///
/// This library is defining 2 error types:
/// - {Error}: is the main error type for the library and the one the users of the library will interact with.
/// - {DriverError}: is the error type that the drivers will use to return errors. Only developers of drivers will
///   interact with this error type.
#[derive(Debug)]
pub enum Error {
    ArrowError { error: arrow_schema::ArrowError },
    DriverError { error: Box<dyn std::error::Error + Send + Sync> },
    DriverNotFound { scheme: String },
    InternalError { error: Box<dyn std::error::Error + Send + Sync> },
    InvalidParameterCount { expected: usize, actual: usize },
    InvalidType { expected: String, actual: String },
    InvalidUri { uri: String },
    NotFound,
    OutOfBounds { index: usize },
    UnsupportedDataType,
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
            Error::DriverError { error } => write!(f, "{}", error),
            Error::DriverNotFound { scheme } => write!(f, "No driver found for scheme: {}", scheme),
            Error::InternalError { error } => write!(f, "{}", error),
            Error::InvalidParameterCount { expected, actual } => {
                write!(f, "Invalid parameter count: expected {}, actual {}", expected, actual)
            }
            Error::InvalidType { expected, actual } => {
                write!(f, "Invalid type: expected '{}', actual '{}'", expected, actual)
            }
            Error::InvalidUri { uri } => write!(f, "Invalid URI: {}", uri),
            Error::NotFound => write!(f, "Not found"),
            Error::OutOfBounds { index } => write!(f, "Out of bounds index {}", index),
            Error::UnsupportedDataType => write!(f, "Unsupported type"),
        }
    }
}

impl std::error::Error for Error {}
