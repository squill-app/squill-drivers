use crate::errors::into_driver_error;
use arrow_array::RecordBatch;
use squill_core::driver::{DriverStatement, Result};
use squill_core::parameters::Parameters;
use std::cell::RefCell;
use std::rc::Rc;

pub(crate) struct PostgresStatement<'c> {
    pub(crate) client: Rc<RefCell<postgres::Client>>,
    pub(crate) inner: postgres::Statement,
    pub(crate) phantom: std::marker::PhantomData<&'c ()>,
}

impl DriverStatement for PostgresStatement<'_> {
    fn execute(&mut self, _parameters: Option<Parameters>) -> Result<u64> {
        let mut client = self.client.borrow_mut();
        Ok(client.execute(&self.inner, &[]).map_err(into_driver_error)? as u64)
    }

    fn query<'s>(
        &'s mut self,
        _parameters: Option<Parameters>,
    ) -> Result<Box<dyn Iterator<Item = Result<RecordBatch>> + 's>> {
        todo!()
    }
}
