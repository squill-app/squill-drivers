use crate::connection::{Command, Handle};
use crate::{await_on, RecordBatchStream};
use futures::future::{err, BoxFuture};
use squill_core::parameters::Parameters;
use squill_core::{Error, Result};
use tokio::sync::oneshot;

pub struct Statement<'c> {
    pub(crate) handle: Handle,
    pub(crate) command_tx: crossbeam_channel::Sender<Command>,
    pub(crate) phantom: std::marker::PhantomData<&'c ()>,
}

impl Statement<'_> {
    pub fn bind(&self, _parameters: Parameters) -> BoxFuture<'_, Result<()>> {
        todo!("Implement bind method")
    }

    pub fn execute(&self, parameters: Parameters) -> BoxFuture<'_, Result<u64>> {
        let (tx, rx) = oneshot::channel();
        if let Err(e) = self.command_tx.send(Command::ExecutePreparedStatement { handle: self.handle, parameters, tx })
        {
            return Box::pin(err::<u64, Error>(Box::new(e)));
        }
        await_on!(rx)
    }

    pub fn query(&self) -> BoxFuture<'_, Result<RecordBatchStream<'_>>> {
        todo!("Implement query method")
    }
}

impl Drop for Statement<'_> {
    fn drop(&mut self) {
        let _ = self.command_tx.send(Command::DropStatement { handle: self.handle });
    }
}
