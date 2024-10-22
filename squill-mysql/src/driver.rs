use crate::{MySql, DRIVER_NAME};
use squill_core::driver::{DriverConnection, DriverStatement, Result};

impl DriverConnection for MySql {
    fn driver_name(&self) -> &str {
        DRIVER_NAME
    }

    fn close(self: Box<Self>) -> Result<()> {
        Ok(())
    }

    fn prepare<'c: 's, 's>(&'c self, _statement: &str) -> Result<Box<dyn DriverStatement + 's>> {
        todo!()
    }
}
