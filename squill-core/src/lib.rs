#![forbid(unsafe_code)]

use parameters::Parameters;

pub mod connection;
pub mod driver;
pub mod error;
pub mod factory;
pub mod macros;
pub mod parameters;
pub mod rows;
pub mod statement;
pub mod values;

/// The error type used across the library.
///
/// Because each driver may have its own error type, we need to use a trait object to abstract over them.
// pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub type Error = error::Error;

/// A specialized `Result` type for this library.
pub type Result<T> = std::result::Result<T, Error>;

/// A helper constant for no parameters when binding a statement.
pub const NO_PARAM: Parameters = Parameters::None;
