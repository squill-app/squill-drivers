use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use squill_core::driver::{DriverStatement, Result};
use squill_core::parameters::Parameters;

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
