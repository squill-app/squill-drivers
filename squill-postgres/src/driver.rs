use crate::{errors::into_driver_error, statement::PostgresStatement, Postgres, DRIVER_NAME};
use squill_core::driver::{DriverConnection, DriverStatement, Result};

impl DriverConnection for Postgres {
    fn driver_name(&self) -> &str {
        DRIVER_NAME
    }

    fn close(self: Box<Self>) -> Result<()> {
        Ok(())
    }

    fn prepare<'c: 's, 's>(&'c mut self, statement: &str) -> Result<Box<dyn DriverStatement + 's>> {
        Ok(Box::new(PostgresStatement {
            inner: self.client.prepare(statement).map_err(into_driver_error)?,
            client: &mut self.client,
            phantom: std::marker::PhantomData,
        }))
    }
}
