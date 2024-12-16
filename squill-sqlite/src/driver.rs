use crate::errors::driver_error;
use crate::statement::SqliteStatement;
use crate::{Sqlite, DRIVER_NAME};
use squill_core::driver::DriverConnection;
use squill_core::driver::DriverStatement;
use squill_core::driver::Result;

impl DriverConnection for Sqlite {
    fn driver_name(&self) -> &str {
        DRIVER_NAME
    }

    /// Check if the connection is alive.
    fn ping(&mut self) -> Result<()> {
        // An SQLite connection does not involve a network layer as it operates in-process. This means the concept of
        // "checking if a connection is alive" is less about verifying network connectivity (like with other databases)
        // and more about ensuring the internal state of the connection and database instance is healthy.
        // Executing a lightweight, non-intrusive query (like SELECT 1) is a simple and efficient way to validate the
        // connection.
        match self.conn.execute_batch("SELECT 1") {
            Ok(_) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    fn close(self: Box<Self>) -> Result<()> {
        Ok(())
    }

    fn prepare<'c: 's, 's>(&'c mut self, statement: &str) -> Result<Box<dyn DriverStatement + 's>> {
        Ok(Box::new(SqliteStatement {
            inner: self.conn.prepare(statement).map_err(driver_error)?,
            options: self.options.clone(),
        }))
    }
}
