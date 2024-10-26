use crate::DRIVER_NAME;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use squill_core::{
    driver::{DriverConnection, DriverOptionsRef, DriverStatement, Result},
    parameters::Parameters,
};

pub(crate) struct MySql {
    pub(crate) conn: mysql::Conn,
    pub(crate) options: DriverOptionsRef,
}

impl DriverConnection for MySql {
    fn driver_name(&self) -> &str {
        DRIVER_NAME
    }

    fn close(self: Box<Self>) -> Result<()> {
        Ok(())
    }

    fn prepare<'c: 's, 's>(&'c mut self, _statement: &str) -> Result<Box<dyn DriverStatement + 's>> {
        todo!()
    }
}

pub(crate) struct MySqlStatement<'c> {
    pub(crate) inner: mysql::Statement,
    pub(crate) phantom: std::marker::PhantomData<&'c ()>,
}

impl DriverStatement for MySqlStatement<'_> {
    fn execute(&mut self, _parameters: Option<Parameters>) -> Result<u64> {
        todo!()
    }

    fn query<'s>(
        &'s mut self,
        _parameters: Option<Parameters>,
    ) -> Result<Box<dyn Iterator<Item = Result<RecordBatch>> + 's>> {
        todo!()
    }

    fn schema(&self) -> SchemaRef {
        todo!()
    }
}
