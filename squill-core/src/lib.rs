pub mod connection;
pub mod drivers;
pub mod factory;
pub mod macros;
pub mod parameters;
pub mod rows;
pub mod values;

/// The error type used across the library.
///
/// Because each driver may have its own error type, we need to use a trait object to abstract over them.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug)]
pub struct UnsupportedError {
    message: String,
}

impl UnsupportedError {
    pub fn new(message: &str) -> Self {
        Self { message: message.to_string() }
    }
}

impl std::error::Error for UnsupportedError {}

impl std::fmt::Display for UnsupportedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Unsupported: {}", self.message)
    }
}

/// A specialized `Result` type for this library.
pub type Result<T> = std::result::Result<T, Error>;
