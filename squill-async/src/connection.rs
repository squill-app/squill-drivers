use arrow_array::RecordBatch;
use futures::future::{err, BoxFuture};
use squill_core::driver::DriverStatement;
use squill_core::factory::Factory;
use squill_core::parameters::Parameters;
use squill_core::{Error, Result};
use std::collections::HashMap;
use std::thread;
use tokio::sync::oneshot;
use tracing::error;

use crate::statement::Statement;
use crate::RecordBatchStream;

/// An handle to an object owned by the connection's thread.
pub(crate) type Handle = u64;

pub struct Connection {
    pub(crate) command_tx: crossbeam_channel::Sender<Command>,
}

pub(crate) enum Command {
    Close { tx: oneshot::Sender<Result<()>> },
    DropStatement { handle: Handle },
    Execute { statement: String, parameters: Parameters, tx: oneshot::Sender<Result<u64>> },
    ExecutePreparedStatement { handle: Handle, parameters: Parameters, tx: oneshot::Sender<Result<u64>> },
    Fetch { tx: tokio::sync::mpsc::Sender<Result<Option<RecordBatch>>> },
    PrepareStatement { statement: String, tx: oneshot::Sender<Result<Handle>> },
    Query { statement: String, parameters: Parameters, tx: oneshot::Sender<Result<()>> },
}

#[macro_export]
macro_rules! await_on {
    ($rx:expr) => {
        Box::pin(async move {
            match $rx.await {
                Ok(Ok(value)) => Ok(value),
                Ok(Err(e)) => Err(e),
                Err(e) => Err(Box::new(e) as Error),
            }
        })
    };
}

macro_rules! blocking_send_response {
    ($tx:expr, $value:expr) => {
        if $tx.blocking_send($value).is_err() {
            error!("Channel communication failed while sending statement fetching response.");
            break;
        }
    };
}

macro_rules! send_response {
    ($tx:expr, $value:expr) => {
        if $tx.send($value).is_err() {
            error!("Channel communication failed while sending command response.");
            break;
        }
    };
}

impl Connection {
    pub fn close(self) -> BoxFuture<'static, Result<()>> {
        let (tx, rx) = oneshot::channel();
        if let Err(e) = self.command_tx.send(Command::Close { tx }) {
            return Box::pin(err::<(), Error>(Box::new(e)));
        }
        await_on!(rx)
    }

    pub fn prepare<S: Into<String>>(&self, statement: S) -> BoxFuture<'_, Result<Statement<'_>>> {
        let (tx, rx) = oneshot::channel();
        if let Err(e) = self.command_tx.send(Command::PrepareStatement { statement: statement.into(), tx }) {
            return Box::pin(err::<Statement<'_>, Error>(Box::new(e)));
        }
        Box::pin(async move {
            match rx.await {
                Ok(Ok(handle)) => {
                    Ok(Statement { handle, command_tx: self.command_tx.clone(), phantom: std::marker::PhantomData })
                }
                Ok(Err(e)) => Err(e),
                Err(e) => Err(Box::new(e) as Error),
            }
        })
    }

    pub fn execute(&self, statement: String, parameters: Parameters) -> BoxFuture<'static, Result<u64>> {
        let (tx, rx) = oneshot::channel();
        if let Err(e) = self.command_tx.send(Command::Execute { statement, parameters, tx }) {
            return Box::pin(err::<u64, Error>(Box::new(e)));
        }
        await_on!(rx)
    }

    pub fn query<'conn>(
        &'conn self,
        statement: String,
        parameters: Parameters,
    ) -> BoxFuture<'conn, Result<RecordBatchStream<'conn>>> {
        let (tx, rx) = oneshot::channel();
        if let Err(e) = self.command_tx.send(Command::Query { statement, parameters, tx }) {
            return Box::pin(err::<RecordBatchStream<'conn>, Error>(Box::new(e)));
        }
        Box::pin(async move {
            match rx.await {
                Ok(Ok(())) => Ok(RecordBatchStream::new(self)),
                Ok(Err(e)) => Err(e),
                Err(e) => Err(Box::new(e) as Error),
            }
        })
    }

    pub(crate) fn fetch(&self, tx: tokio::sync::mpsc::Sender<Result<Option<arrow_array::RecordBatch>>>) -> Result<()> {
        if let Err(e) = self.command_tx.send(Command::Fetch { tx }) {
            return Err(Box::new(e) as Error);
        }
        Ok(())
    }
}

impl Connection {
    pub fn open<T: Into<String>>(uri: T) -> BoxFuture<'static, Result<Self>> {
        let (command_tx, command_rx): (crossbeam_channel::Sender<Command>, crossbeam_channel::Receiver<Command>) =
            crossbeam_channel::bounded(1);
        let uri: String = uri.into();
        let (open_tx, open_rx) = oneshot::channel();

        let thread_spawn_result = thread::Builder::new()
            // .name(params.thread_name.clone())
            .spawn(move || {
                let inner_conn_result = Factory::open(&uri);
                match inner_conn_result {
                    Ok(inner_conn) => {
                        let mut prepared_statements: HashMap<Handle, Box<dyn DriverStatement>> = HashMap::new();
                        let mut next_handle: Handle = 1;
                        let mut last_query_rows_iterator: Option<Box<dyn Iterator<Item = Result<RecordBatch>>>> = None;
                        if open_tx.send(Ok(Self { command_tx })).is_err() {
                            error!("Channel communication failed.");
                        } else {
                            loop {
                                let command = command_rx.recv();
                                match command {
                                    //
                                    // Close the connection.
                                    //
                                    Ok(Command::Close { tx }) => {
                                        // Because the last query rows iterator is borrowing the connection, we need to
                                        // drop it before closing the connection.
                                        drop(last_query_rows_iterator);
                                        drop(prepared_statements);
                                        let result = inner_conn.close();
                                        // We don't care if the receiver is closed, because we are closing the
                                        // connection anyway.
                                        send_response!(tx, result);
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
                                    Ok(Command::Execute { statement, parameters, tx }) => {
                                        match inner_conn.prepare(&statement) {
                                            Ok(mut stmt) => match stmt.bind(parameters) {
                                                Ok(_) => {
                                                    send_response!(tx, stmt.execute());
                                                }
                                                Err(e) => {
                                                    send_response!(tx, Err(e));
                                                }
                                            },
                                            Err(e) => {
                                                send_response!(tx, Err(e));
                                            }
                                        }
                                    }

                                    // Execute a prepared statement.
                                    Ok(Command::ExecutePreparedStatement { handle, parameters, tx }) => {
                                        if let Some(stmt) = prepared_statements.get_mut(&handle) {
                                            match stmt.bind(parameters) {
                                                Ok(_) => {
                                                    send_response!(tx, stmt.execute());
                                                }
                                                Err(e) => {
                                                    send_response!(tx, Err(e));
                                                }
                                            }
                                        } else {
                                            send_response!(tx, Err("Invalid statement handle".into()));
                                        }
                                    }

                                    Ok(Command::Query { statement, parameters, tx }) => {
                                        todo!("Implement query command")
                                        /*
                                        let result = inner_conn.query(statement, parameters);
                                        match result {
                                            Ok(rows) => {
                                                last_query_rows_iterator = Some(rows);
                                                send_response!(tx, Ok(()));
                                            }
                                            Err(e) => {
                                                send_response!(tx, Err(e));
                                            }
                                        }
                                        */
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
                                    Ok(Command::PrepareStatement { statement, tx }) => {
                                        match inner_conn.prepare(&statement) {
                                            Ok(stmt) => {
                                                prepared_statements.insert(next_handle, stmt);
                                                send_response!(tx, Ok(next_handle));
                                                next_handle += 1;
                                            }
                                            Err(e) => {
                                                send_response!(tx, Err(e));
                                            }
                                        }
                                    }
                                    Ok(Command::Fetch { tx }) => {
                                        if let Some(rows) = last_query_rows_iterator.as_mut() {
                                            match rows.next() {
                                                Some(Ok(batch)) => {
                                                    blocking_send_response!(tx, Ok(Some(batch)));
                                                }
                                                Some(Err(e)) => {
                                                    error!("Error getting next record batch: {:?}", e);
                                                    blocking_send_response!(tx, Err(e));
                                                    break;
                                                }
                                                None => {
                                                    // The iterator is exhausted, so we need to drop it.
                                                    last_query_rows_iterator = None;
                                                    blocking_send_response!(tx, Ok(None));
                                                }
                                            }
                                        } else {
                                            error!("No query rows iterator available.");
                                            break;
                                        }
                                    }
                                    Err(e) => {
                                        error!("Channel communication error: {:?}", e);
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        let result = open_tx.send(Err(e));
                        if result.is_err() {
                            error!("Channel communication error while opening the connection.");
                        }
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
                    Err(e) => Err(Box::new(e) as Error),
                }
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::Connection;

    #[tokio::test]
    async fn test_open() {
        // Opening a connection with an unknown scheme should fail
        assert!(Connection::open("unknown://").await.is_err());
    }
}
