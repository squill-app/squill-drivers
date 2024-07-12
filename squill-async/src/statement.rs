use crate::connection::{Command, Handle};
use crate::{await_on, RecordBatchStream};
use either::Either;
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
            return Box::pin(err::<u64, Error>(Error::DriverError { error: e.into() }));
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

/// A trait to allow either a string or a statement to be used in a method.
pub trait EitherStatement<'s> {
    fn either_statement(self) -> Either<String, Statement<'s>>;
}

impl<'s> EitherStatement<'s> for &str {
    fn either_statement(self) -> Either<String, Statement<'s>> {
        Either::Left(self.to_string())
    }
}

impl<'s> EitherStatement<'s> for String {
    fn either_statement(self) -> Either<String, Statement<'s>> {
        Either::Left(self)
    }
}

impl<'s> EitherStatement<'s> for Statement<'s> {
    fn either_statement(self) -> Either<String, Statement<'s>> {
        Either::Right(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_either_statement() {
        let conn = crate::Connection::open("mock:://").await.unwrap();

        assert!("SELECT 1".either_statement().is_left());
        assert!(String::from("SELECT 1").either_statement().is_left());
        assert!(conn.prepare("SELECT 1").await.unwrap().either_statement().is_right());
    }
}
