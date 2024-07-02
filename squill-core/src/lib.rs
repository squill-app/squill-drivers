pub mod rows;
pub mod parameters;
pub mod drivers;
pub mod macros;
pub mod factory;
pub mod connection;

/// The error type used across the library.
///
/// Because each driver may have its own error type, we need to use a trait object to abstract over them.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// A specialized `Result` type for this library.
pub type Result<T> = std::result::Result<T, Error>;
