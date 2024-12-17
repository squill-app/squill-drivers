use crate::connection::{into_error, Command};
use crate::{await_on, RecordBatchStream, RowStream};
use arrow_schema::SchemaRef;
use futures::future::{err, BoxFuture};
use futures::StreamExt;
use squill_core::parameters::Parameters;
use squill_core::row::Row;
use squill_core::{Error, Result};
use tokio::sync::oneshot;
use tracing::debug;

/// A prepared statement.
///
/// Created by [Connection::prepare], a statement is a query/command that has been prepared for execution.
/// It can be bound with parameters and executed. It can be reused multiple times, with or without parameters.
/// Parameters can be bound using the [Statement::bind] method and are re-used for each execution until new parameters
/// are bound.
pub struct Statement<'c> {
    /// The command sender is used to send commands to the connection thread.
    command_tx: crossbeam_channel::Sender<Command>,

    /// This field is used to make sure the connection will be mut borrowed until the statement is dropped.
    ///
    /// This is important for two reasons:
    /// - Make the async API consistent with the sync API.
    /// - Make sure the client thread is on the right state to process the next command. When a statement is created
    ///   the connection thread is only expecting command for the statement and will only process commands at the
    ///   connection level once the statement is dropped.
    phantom: std::marker::PhantomData<&'c ()>,
}

impl Statement<'_> {
    pub(crate) fn new(command_tx: crossbeam_channel::Sender<Command>) -> Self {
        Self { command_tx, phantom: std::marker::PhantomData }
    }

    pub fn schema(&self) -> BoxFuture<'_, Result<SchemaRef>> {
        let (tx, rx) = oneshot::channel();
        if let Err(e) = self.command_tx.send(Command::GetSchema { tx }) {
            return Box::pin(err::<SchemaRef, Error>(Error::DriverError { error: e.into() }));
        }
        await_on!(rx)
    }

    pub fn execute(&mut self, parameters: Option<Parameters>) -> BoxFuture<'_, Result<u64>> {
        let (tx, rx) = oneshot::channel();
        if let Err(e) = self.command_tx.send(Command::ExecutePreparedStatement { parameters, tx }) {
            return Box::pin(err::<u64, Error>(Error::DriverError { error: e.into() }));
        }
        await_on!(rx)
    }

    pub fn query<'s: 'i, 'i>(
        &'s mut self,
        parameters: Option<Parameters>,
    ) -> BoxFuture<'i, Result<RecordBatchStream<'i>>> {
        let (tx, rx) = oneshot::channel();
        if let Err(e) = self.command_tx.send(Command::QueryPreparedStatement { parameters, tx }) {
            return Box::pin(err::<RecordBatchStream<'i>, Error>(Error::DriverError { error: e.into() }));
        }
        Box::pin(async move {
            match rx.await {
                Ok(Ok(())) => Ok(RecordBatchStream::new(self.command_tx.clone())),
                Ok(Err(error)) => Err(Error::DriverError { error }),
                Err(error) => Err(Error::DriverError { error: error.into() }),
            }
        })
    }

    /// Query a statement and return stream of Rows.
    pub fn query_rows<'s: 'i, 'i>(
        &'s mut self,
        parameters: Option<Parameters>,
    ) -> BoxFuture<'i, Result<RowStream<'i>>> {
        Box::pin(async move {
            let stream = self.query(parameters).await?;
            Ok(RowStream::from(stream))
        })
    }

    pub fn query_row(&mut self, parameters: Option<Parameters>) -> BoxFuture<'_, Result<Option<Row>>> {
        Box::pin(async move {
            let mut stream = self.query_rows(parameters).await?;
            let row = stream.next().await;
            match row {
                Some(Ok(row)) => Ok(Some(row)),
                Some(Err(e)) => Err(e),
                None => Ok(None),
            }
        })
    }

    pub fn query_map_row<'s: 'r, 'r, F, T>(
        &'s mut self,
        parameters: Option<Parameters>,
        mapping_fn: F,
    ) -> BoxFuture<'r, Result<Option<T>>>
    where
        F: FnOnce(Row) -> std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>
            + std::marker::Send
            + 'r + 's,
    {
        Box::pin(async move {
            let mut stream = self.query_rows(parameters).await?;
            let row = stream.next().await;
            match row {
                Some(Ok(row)) => {
                    let mapped = mapping_fn(row)?;
                    Ok(Some(mapped))
                }
                Some(Err(e)) => Err(e),
                None => Ok(None),
            }
        })
    }
}

impl Drop for Statement<'_> {
    /// Drop the statement.
    ///
    /// This function sends a command to drop the statement. The driver will confirm the drop by sending a message back.
    /// We need to wait for the confirmation before dropping the statement otherwise the connection thread might still
    /// be on the wrong state to process the next command.
    fn drop(&mut self) {
        let (tx, rx) = oneshot::channel();
        match self.command_tx.send(Command::DropStatement { tx }) {
            // FIXME: Not sure we actually need to wait for the confirmation.
            Ok(()) => {
                if let Err(e) = futures::executor::block_on(rx) {
                    debug!("Error waiting for statement drop confirmation: {}", e);
                }
            }
            Err(e) => {
                debug!("Error dropping statement: {}", e);
            }
        }
    }
}
