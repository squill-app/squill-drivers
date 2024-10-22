use crate::{errors::into_driver_error, statement::PostgresStatement, Postgres, DRIVER_NAME};
use squill_core::driver::{DriverConnection, DriverStatement, Result};

impl DriverConnection for Postgres {
    fn driver_name(&self) -> &str {
        DRIVER_NAME
    }

    fn close(self: Box<Self>) -> Result<()> {
        Ok(())
    }

    fn prepare<'c: 's, 's>(&'c self, statement: &str) -> Result<Box<dyn DriverStatement + 's>> {
        let mut client = self.client.borrow_mut();
        Ok(Box::new(PostgresStatement {
            inner: client.prepare(statement).map_err(into_driver_error)?,
            client: self.client.clone(),
            phantom: std::marker::PhantomData,
        }))
    }
}
