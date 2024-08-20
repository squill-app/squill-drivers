use crate::connection::{Command, Handle};
use crate::{await_on, RecordBatchStream};
use either::Either;
use futures::future::{err, BoxFuture};
use squill_core::parameters::Parameters;
use squill_core::{Error, Result};
use tokio::sync::oneshot;

/// A prepared statement.
///
/// A statement is a query/command that has been prepared for execution. It can be bound with parameters and executed.
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

/// A trait to allow either a string or a statement to be used in a method expecting a statement.
///
/// This trait is implemented for `&str`, `String`, and `Statement`.
/// Note that this trait is not intended to be implemented by the user and it differs from its blocking counterpart for
/// performance reasons.
pub trait IntoStatement<'s> {
    fn into_statement(self) -> Either<String, Statement<'s>>;
}

impl<'s> IntoStatement<'s> for &str {
    fn into_statement(self) -> Either<String, Statement<'s>> {
        Either::Left(self.to_string())
    }
}

impl<'s> IntoStatement<'s> for String {
    fn into_statement(self) -> Either<String, Statement<'s>> {
        Either::Left(self)
    }
}

impl<'s> IntoStatement<'s> for Statement<'s> {
    fn into_statement(self) -> Either<String, Statement<'s>> {
        Either::Right(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_either_statement() {
        let conn = crate::Connection::open("mock:://").await.unwrap();

        assert!("SELECT 1".into_statement().is_left());
        assert!(String::from("SELECT 1").into_statement().is_left());
        assert!(conn.prepare("SELECT 1").await.unwrap().into_statement().is_right());
    }
}
