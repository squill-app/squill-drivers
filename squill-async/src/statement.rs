use crate::connection::{Command, Handle};
use crate::{await_on, RecordBatchStream};
use futures::future::{err, BoxFuture};
use squill_core::parameters::Parameters;
use squill_core::{Error, Result};
use tokio::sync::oneshot;

/// A prepared statement.
///
/// Created by [Connection::prepare], a statement is a query/command that has been prepared for execution.
/// It can be bound with parameters and executed. It can be reused multiple times, with or without parameters.
/// Parameters can be bound using the [Statement::bind] method and are re-used for each execution until new parameters
/// are bound.
pub struct Statement<'c> {
    pub(crate) handle: Handle,
    command_tx: crossbeam_channel::Sender<Command>,
    phantom: std::marker::PhantomData<&'c ()>,
}

impl Statement<'_> {
    pub(crate) fn new(handle: Handle, command_tx: crossbeam_channel::Sender<Command>) -> Self {
        Self { handle, command_tx, phantom: std::marker::PhantomData }
    }

    pub fn bind(&mut self, parameters: Parameters) -> BoxFuture<'_, Result<()>> {
        let (tx, rx) = oneshot::channel();
        if let Err(e) = self.command_tx.send(Command::Bind { handle: self.handle, parameters, tx }) {
            return Box::pin(err::<(), Error>(Error::DriverError { error: e.into() }));
        }
        await_on!(rx)
    }

    pub fn execute(&mut self, parameters: Option<Parameters>) -> BoxFuture<'_, Result<u64>> {
        let (tx, rx) = oneshot::channel();
        if let Err(e) = self.command_tx.send(Command::ExecutePreparedStatement { handle: self.handle, parameters, tx })
        {
            return Box::pin(err::<u64, Error>(Error::DriverError { error: e.into() }));
        }
        await_on!(rx)
    }

    pub fn query(&mut self) -> BoxFuture<'_, Result<RecordBatchStream<'_>>> {
        let (tx, rx) = oneshot::channel();
        if let Err(e) = self.command_tx.send(Command::Query { handle: self.handle, parameters: None, tx }) {
            return Box::pin(err::<RecordBatchStream<'_>, Error>(Error::DriverError { error: e.into() }));
        }
        Box::pin(async move {
            match rx.await {
                Ok(Ok(())) => Ok(RecordBatchStream::new(self.command_tx.clone())),
                Ok(Err(error)) => Err(Error::DriverError { error }),
                Err(error) => Err(Error::DriverError { error: error.into() }),
            }
        })
    }
}

impl Drop for Statement<'_> {
    fn drop(&mut self) {
        // We ignore the result because drop is not async and we cannot wait for full completion of the command anyway.
        let _ = self.command_tx.send(Command::DropStatement { handle: self.handle });
    }
}

/// An enum that can be either a string or a mutable reference to a statement.
///
/// This enum is used to allow some functions to accept either a string or a mutable reference to a statement.
/// See [squill_core::statement::StatementRef] for an example.
/// ```
pub enum StatementRef<'r, 's> {
    Str(&'r str),
    Statement(&'r mut Statement<'s>),
}

/// Conversion of a [std::str] reference to [StatementRef].
impl<'r, 's: 'r> From<&'s str> for StatementRef<'r, '_> {
    fn from(s: &'s str) -> Self {
        StatementRef::Str(s)
    }
}

/// Conversion of a [Statement] mutable reference to [StatementRef].
/// Create a `StatementRef` from a mutable reference to a statement.
impl<'r, 's> From<&'r mut Statement<'s>> for StatementRef<'r, 's> {
    fn from(statement: &'r mut Statement<'s>) -> Self {
        StatementRef::Statement(statement)
    }
}
