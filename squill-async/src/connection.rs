use arrow_array::RecordBatch;
use futures::future::{err, BoxFuture};
use squill_core::driver;
use squill_core::driver::{DriverConnection, DriverStatement};
use squill_core::factory::Factory;
use squill_core::parameters::Parameters;
use squill_core::{Error, Result};
use std::collections::HashMap;
use std::thread;
use tokio::sync::oneshot;
use tracing::error;

use crate::statement::{IntoStatement, Statement};
use crate::RecordBatchStream;

/// An handle to an object owned by the connection's thread.
pub(crate) type Handle = u64;

#[macro_export]
macro_rules! await_on {
    ($rx:expr) => {
        Box::pin(async move {
            match $rx.await {
                Ok(Ok(value)) => Ok(value),
                Ok(Err(e)) => Err(Error::DriverError { error: e }),
                Err(e) => Err(Error::InternalError { error: e.into() }),
            }
        })
    };
}

pub struct Connection {
    pub(crate) command_tx: crossbeam_channel::Sender<Command>,
}

impl Connection {
    pub fn open<T: Into<String>>(uri: T) -> BoxFuture<'static, Result<Self>> {
        let (command_tx, command_rx): (crossbeam_channel::Sender<Command>, crossbeam_channel::Receiver<Command>) =
            crossbeam_channel::bounded(1);
        let uri: String = uri.into();
        let (open_tx, open_rx) = oneshot::channel();

        let thread_spawn_result = thread::Builder::new()
            // .name(params.thread_name.clone())
            .spawn(move || match Factory::open(&uri) {
                Ok(inner_conn) => {
                    if open_tx.send(Ok(Self { command_tx })).is_err() {
                        error!("Channel communication failed.");
                    } else {
                        Self::command_loop(inner_conn, command_rx);
                    }
                }
                Err(e) => {
                    if open_tx.send(Err(e)).is_err() {
                        error!("Channel communication error while opening the connection.");
                    }
                }
            });

        if thread_spawn_result.is_err() {
            Box::pin(async { Err("Failed to spawn thread".into()) })
        } else {
            Box::pin(async {
                match open_rx.await {
                    Ok(Ok(conn)) => Ok(conn),
                    Ok(Err(e)) => Err(e),
                    Err(e) => Err(Error::DriverError { error: e.into() }),
                }
            })
        }
    }

    pub fn close(self) -> BoxFuture<'static, Result<()>> {
        let (tx, rx) = oneshot::channel();
        if let Err(e) = self.command_tx.send(Command::Close { tx }) {
            return Box::pin(err::<(), Error>(Error::InternalError { error: e.into() }));
        }
        await_on!(rx)
    }

    pub fn prepare<S: Into<String>>(&self, statement: S) -> BoxFuture<'_, Result<Statement<'_>>> {
        let (tx, rx) = oneshot::channel();
        if let Err(e) = self.command_tx.send(Command::PrepareStatement { statement: statement.into(), tx }) {
            return Box::pin(err::<Statement<'_>, Error>(Error::DriverError { error: e.into() }));
        }
        Box::pin(async move {
            match rx.await {
                Ok(Ok(handle)) => {
                    Ok(Statement { handle, command_tx: self.command_tx.clone(), phantom: std::marker::PhantomData })
                }
                Ok(Err(e)) => Err(Error::DriverError { error: e }),
                Err(e) => Err(Error::InternalError { error: e.into() }),
            }
        })
    }

    pub fn execute<'c, 's, S: IntoStatement<'s>>(
        &'c self,
        command: S,
        parameters: Option<Parameters>,
    ) -> BoxFuture<'s, Result<u64>>
    where
        'c: 's,
    {
        let either = command.into_statement();
        if either.is_left() {
            let string_command = either.left().unwrap();
            let (tx, rx) = oneshot::channel();
            if let Err(e) = self.command_tx.send(Command::Execute { statement: string_command, parameters, tx }) {
                return Box::pin(err::<u64, Error>(Error::DriverError { error: e.into() }));
            }
            await_on!(rx)
        } else {
            Box::pin(async move {
                let statement = either.right().unwrap();
                statement.execute(parameters).await
            })
        }
    }

    pub fn query<'conn>(
        &'conn self,
        statement: &mut Statement<'conn>,
        parameters: Option<Parameters>,
    ) -> BoxFuture<'conn, Result<RecordBatchStream<'conn>>> {
        let (tx, rx) = oneshot::channel();
        if let Err(e) = self.command_tx.send(Command::Query { handle: statement.handle, parameters, tx }) {
            return Box::pin(err::<RecordBatchStream<'conn>, Error>(Error::DriverError { error: e.into() }));
        }
        Box::pin(async move {
            match rx.await {
                Ok(Ok(())) => Ok(RecordBatchStream::new(self)),
                Ok(Err(error)) => Err(Error::DriverError { error }),
                Err(error) => Err(Error::DriverError { error: error.into() }),
            }
        })
    }

    pub(crate) fn fetch_cursor(
        &self,
        tx: tokio::sync::mpsc::Sender<driver::Result<Option<RecordBatch>>>,
    ) -> Result<()> {
        if let Err(e) = self.command_tx.send(Command::FetchCursor { tx }) {
            return Err(Error::InternalError { error: e.into() });
        }
        Ok(())
    }

    pub(crate) fn drop_cursor(&self) -> Result<()> {
        if let Err(e) = self.command_tx.send(Command::DropCursor) {
            return Err(Error::DriverError { error: e.into() });
        }
        Ok(())
    }
}

pub(crate) enum Command {
    Bind {
        handle: Handle,
        parameters: Parameters,
        tx: oneshot::Sender<driver::Result<()>>,
    },
    Close {
        tx: oneshot::Sender<driver::Result<()>>,
    },
    DropStatement {
        handle: Handle,
    },
    DropCursor,
    Execute {
        statement: String,
        parameters: Option<Parameters>,
        tx: oneshot::Sender<driver::Result<u64>>,
    },
    ExecutePreparedStatement {
        handle: Handle,
        parameters: Option<Parameters>,
        tx: oneshot::Sender<driver::Result<u64>>,
    },
    FetchCursor {
        tx: tokio::sync::mpsc::Sender<driver::Result<Option<RecordBatch>>>,
    },
    PrepareStatement {
        statement: String,
        tx: oneshot::Sender<driver::Result<Handle>>,
    },
    Query {
        handle: Handle,
        parameters: Option<Parameters>,
        tx: oneshot::Sender<driver::Result<()>>,
    },
}

macro_rules! blocking_send_response_and_break_on_error {
    ($tx:expr, $value:expr) => {
        if $tx.blocking_send($value).is_err() {
            error!("Channel communication failed while sending statement fetching response.");
            break;
        }
    };
}

macro_rules! send_response_and_break_on_error {
    ($tx:expr, $value:expr) => {
        if $tx.send($value).is_err() {
            error!("Channel communication failed while sending command response.");
            break;
        }
    };
}

impl Connection {
    fn command_loop(inner_conn: Box<dyn DriverConnection>, command_rx: crossbeam_channel::Receiver<Command>) {
        let mut prepared_statements: HashMap<Handle, Box<dyn DriverStatement>> = HashMap::new();
        let mut next_handle: Handle = 1;
        loop {
            let command = command_rx.recv();
            match command {
                Ok(Command::Bind { handle, parameters, tx }) => {
                    if let Some(stmt) = prepared_statements.get_mut(&handle) {
                        send_response_and_break_on_error!(tx, stmt.bind(parameters));
                    } else {
                        send_response_and_break_on_error!(tx, Err("Invalid statement handle".into()));
                    }
                }

                //
                // Close the connection.
                //
                Ok(Command::Close { tx }) => {
                    drop(prepared_statements);
                    let result = inner_conn.close();
                    // We don't care if the receiver is closed, because we are closing the
                    // connection anyway.
                    send_response_and_break_on_error!(tx, result);
                    // Once the connection is closed, we need to break the loop and exit the thread.
                    break;
                }

                //
                // Drop a prepared statement.
                //
                // The prepared statement is removed from the `prepared_statements` map. The caller
                // is not expecting any response.
                Ok(Command::DropStatement { handle }) => {
                    prepared_statements.remove(&handle);
                }

                // Prepare and execute a statement at once.
                //
                // This is a convenience method that prepares a statement, binds the parameters, and
                // executes it in one go. The response sent back to the caller is the number of rows
                // affected by the statement.
                // Using this command is more efficient than preparing and then executing the
                // prepared statement because it avoids the overhead of sending back the handle to
                // the prepared statement and then run a second command to execute it.
                Ok(Command::Execute { statement, parameters, tx }) => match inner_conn.prepare(&statement) {
                    Ok(mut stmt) => match parameters {
                        Some(parameters) => match stmt.bind(parameters) {
                            Ok(_) => send_response_and_break_on_error!(tx, stmt.execute()),
                            Err(e) => send_response_and_break_on_error!(tx, Err(e)),
                        },
                        None => send_response_and_break_on_error!(tx, stmt.execute()),
                    },
                    Err(e) => {
                        send_response_and_break_on_error!(tx, Err(e));
                    }
                },

                // Execute a prepared statement.
                Ok(Command::ExecutePreparedStatement { handle, parameters, tx }) => {
                    if let Some(stmt) = prepared_statements.get_mut(&handle) {
                        match parameters {
                            Some(parameters) => match stmt.bind(parameters) {
                                Ok(_) => send_response_and_break_on_error!(tx, stmt.execute()),
                                Err(e) => send_response_and_break_on_error!(tx, Err(e)),
                            },
                            None => send_response_and_break_on_error!(tx, stmt.execute()),
                        }
                    } else {
                        send_response_and_break_on_error!(tx, Err("Invalid statement handle".into()));
                    }
                }

                Ok(Command::Query { handle, parameters, tx }) => {
                    if let Some(stmt) = prepared_statements.get_mut(&handle) {
                        if let Some(parameters) = parameters {
                            if let Err(e) = stmt.bind(parameters) {
                                send_response_and_break_on_error!(tx, Err(e));
                                continue;
                            }
                        }
                        match stmt.query() {
                            Ok(mut rows) => {
                                send_response_and_break_on_error!(tx, Ok(()));
                                loop {
                                    match command_rx.recv() {
                                        Ok(Command::DropCursor) => {
                                            // The cursor is dropped, so we need to break the cursor loop.
                                            break;
                                        }
                                        Ok(Command::FetchCursor { tx }) => {
                                            match rows.next() {
                                                Some(Ok(batch)) => {
                                                    blocking_send_response_and_break_on_error!(tx, Ok(Some(batch)));
                                                }
                                                Some(Err(e)) => {
                                                    // An error occurred while fetching the next record batch.
                                                    // This is a fatal error for this query and we are not expecting to
                                                    // receive any more fetch commands for it but still we need to wait
                                                    // for the DropCursor command to break the loop.
                                                    error!("Error getting next record batch: {:?}", e);
                                                    blocking_send_response_and_break_on_error!(tx, Err(e));
                                                }
                                                None => {
                                                    // The iterator is exhausted.
                                                    // We need to break the loop to make the connection available
                                                    // for other operations.
                                                    blocking_send_response_and_break_on_error!(tx, Ok(None));
                                                    break;
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            error!("Channel communication error: {:?}", e);
                                            return;
                                        }
                                        _ => {
                                            // We are not expecting any other command while fetching rows.
                                            // If this occurs, this is most likely because an iterator on a query
                                            // as not been exhausted and not dropped before trying to re-use the
                                            // connection for another operation. This is a programming error and the
                                            // code should be fixed by either exhausting the iterator or dropping it.
                                            error!("Unexpected command while fetching rows.");
                                            return;
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                send_response_and_break_on_error!(tx, Err(e));
                            }
                        }
                    }
                }

                // Prepare a statement.
                //
                // The statement is prepared and stored in the `prepared_statements` map. The
                // response sent back to the caller is the handle to the prepared statement.
                // The handle is used to identify the prepared statement when executing or dropping
                // it.
                // If the response channel is closed, the loop is broken and the thread exits so
                // there is no need to check the result of the send operation and no risk of
                // leaking statements.
                Ok(Command::PrepareStatement { statement, tx }) => match inner_conn.prepare(&statement) {
                    Ok(stmt) => {
                        prepared_statements.insert(next_handle, stmt);
                        send_response_and_break_on_error!(tx, Ok(next_handle));
                        next_handle += 1;
                    }
                    Err(e) => {
                        send_response_and_break_on_error!(tx, Err(e));
                    }
                },
                Ok(Command::FetchCursor { tx }) => {
                    // This command is not expected at this point. It means the developer is trying to use an iterator
                    // that is already exhausted.
                    blocking_send_response_and_break_on_error!(tx, Ok(None));
                    break;
                }
                Ok(Command::DropCursor) => {
                    // This happen when the RecordBatchStream is dropped. We should ignore it because it simply means
                    // that either an error occurred while fetching the next record batch or the iterator is exhausted.
                    // There is nothing to do here since this command doesn't expect a response.
                }
                Err(e) => {
                    error!("Channel communication error: {:?}", e);
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::Connection;
    use futures::StreamExt;
    use squill_core::params;

    #[tokio::test]
    async fn test_open() {
        assert!(Connection::open("unknown://").await.is_err());
        assert!(Connection::open("mock://").await.is_ok());
    }

    #[tokio::test]
    async fn test_prepare() {
        let conn = Connection::open("mock://").await.unwrap();
        assert!(conn.prepare("SELECT 1").await.is_ok());
        assert!(conn.prepare("XINSERT").await.is_err());
    }

    #[tokio::test]
    async fn test_execute() {
        let conn = Connection::open("mock://").await.unwrap();

        // Using string statement
        assert_eq!(conn.execute("INSERT 1", None).await.unwrap(), 1);
        assert!(conn.execute("SELECT 1", None).await.is_err()); // SELECT is not allowed
        assert!(conn.execute("INSERT 1", params!(1)).await.is_err()); // bind parameters mismatch

        // Using prepared statement
        assert!(conn.execute(conn.prepare("INSERT 1").await.unwrap(), None).await.is_ok());
        assert!(conn.execute(conn.prepare("SELECT 1").await.unwrap(), None).await.is_err());
        assert!(conn.execute(conn.prepare("INSERT 1").await.unwrap(), params!(1)).await.is_err());
    }

    #[tokio::test]
    async fn test_query() {
        let conn = Connection::open("mock://").await.unwrap();

        let mut stmt = conn.prepare("SELECT 1").await.unwrap();
        assert!(conn.query(&mut stmt, params!("hello")).await.is_err());
        let mut iter = conn.query(&mut stmt, None).await.unwrap();
        assert!(iter.next().await.is_some());
        assert!(iter.next().await.is_none());
        drop(stmt);

        let mut stmt = conn.prepare("INSERT 1").await.unwrap();
        assert!(conn.query(&mut stmt, None).await.is_err());
        drop(stmt);

        // Testing not exhausting the iterator, if the iterator was not dropped
        let mut stmt = conn.prepare("SELECT 1").await.unwrap();
        let mut iter = conn.query(&mut stmt, None).await.unwrap();
        assert!(iter.next().await.is_some());
        drop(iter);
        drop(stmt);

        let mut stmt = conn.prepare("SELECT 1").await.unwrap();
        let mut iter = conn.query(&mut stmt, None).await.unwrap();
        assert!(iter.next().await.is_some());
    }
}
