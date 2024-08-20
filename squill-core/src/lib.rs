#![forbid(unsafe_code)]

pub mod connection;
pub mod decode;
pub mod driver;
pub mod error;
pub mod factory;
pub mod macros;
pub mod parameters;
pub mod rows;
pub mod statement;
pub mod values;

/// The mock module is only available when running test or when the `mock` feature is enabled.
/// It provides a mock implementation of the driver and connection to be used in tests.
#[cfg(any(test, feature = "mock"))]
pub mod mock;

/// The error type used across the library.
///
/// All errors produced by the crates in this workspace are supposed to be {{Error}}. Only the drivers are allowed to
/// return their own error types {{DriverError}} which will be then converted to an {{Error}}.
pub type Error = error::Error;

/// A specialized `Result` type for this library.
pub type Result<T> = std::result::Result<T, Error>;
