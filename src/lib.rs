//! # Crate Topology
//!
//! The [`squill-driver`] project is implemented as multiple sub-crates, which are then re-exported by
//! this top-level crate.
//!
//! Crate authors can choose to depend on this top-level crate, or just
//! the sub-crates they need.
//!
//! The current list of sub-crates is:
//!
//! * [`squill-core`][squill_core] - the core traits and types
//! * [`squill-duckdb`][squill_duckdb] - the [DuckDB](https://duckdb.org) driver
//! * [`squill-async`][squill_async] - asynchronous adapter for squill drivers

pub use squill_core::parameters::Parameters;
pub use squill_core::connection::Connection;
pub use squill_core::rows::Rows;
pub use squill_core::rows::Row;
pub use squill_core::Result;

// Re-export the `query!` and `execute!` macros.
pub use squill_core::query;
pub use squill_core::execute;

#[cfg(feature = "async")]
pub mod futures {
    pub use squill_async::RecordBatchStream;
    pub use squill_async::Connection;
}

pub fn register_drivers() {
    #[cfg(feature = "duckdb")]
    squill_duckdb::register_driver();
}
