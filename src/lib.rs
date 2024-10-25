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
//! * [`squill-sqlite`][squill_sqlite] - the [SQLite](https://www.sqlite.org) driver
//! * [`squill-async`][squill_async] - asynchronous adapter for squill drivers

pub use squill_core::connection::Connection;
pub use squill_core::decode::Decode;
pub use squill_core::error::Error;
pub use squill_core::factory::Factory;
pub use squill_core::parameters::Parameters;
pub use squill_core::rows::Row;
pub use squill_core::rows::Rows;
pub use squill_core::statement::Statement;
pub use squill_core::Result;

// Re-export the macros.
pub use squill_core::{execute, params};

#[cfg(feature = "async")]
pub mod futures {
    pub use squill_async::Connection;
    pub use squill_async::RecordBatchStream;
    pub use squill_async::RowStream;
    pub use squill_async::Statement;
}

#[cfg(feature = "sqlite")]
pub mod sqlite {
    pub use squill_sqlite::DRIVER_NAME;
    pub use squill_sqlite::IN_MEMORY_SPECIAL_FILENAME;
    pub use squill_sqlite::IN_MEMORY_URI;
}

#[cfg(feature = "duckdb")]
pub mod duckdb {
    pub use squill_duckdb::DRIVER_NAME;
}

#[cfg(feature = "serde")]
pub mod serde {
    pub use squill_serde::*;
}

pub fn register_drivers() {
    #[cfg(feature = "duckdb")]
    squill_duckdb::register_driver();
    #[cfg(feature = "sqlite")]
    squill_sqlite::register_driver();
}
