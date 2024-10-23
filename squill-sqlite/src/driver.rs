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
