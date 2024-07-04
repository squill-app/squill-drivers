use arrow_array::RecordBatch;
use futures::future::{err, BoxFuture};
use squill_core::factory::Factory;
use squill_core::parameters::Parameters;
use squill_core::{Error, Result};
use std::thread;
use tokio::sync::oneshot;
use tracing::error;

use crate::RecordBatchStream;

pub struct Connection {
    command_tx: crossbeam_channel::Sender<Command>,
}

enum Command {
    Close { tx: oneshot::Sender<Result<()>> },
    Execute { statement: String, parameters: Parameters, tx: oneshot::Sender<Result<u64>> },
    Query { statement: String, parameters: Parameters, tx: oneshot::Sender<Result<()>> },
    Fetch { tx: tokio::sync::mpsc::Sender<Result<Option<RecordBatch>>> },
}

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
                        let mut last_query_rows_iterator: Option<Box<dyn Iterator<Item = Result<RecordBatch>>>> = None;
                        if open_tx.send(Ok(Self { command_tx })).is_err() {
                            error!("Channel communication failed.");
                        } else {
                            loop {
                                let command = command_rx.recv();
                                match command {
                                    /*
                                     * Close the connection.
                                     */
                                    Ok(Command::Close { tx }) => {
                                        // Because the last query rows iterator is borrowing the connection, we need to
                                        // drop it before closing the connection.
                                        drop(last_query_rows_iterator);
                                        let result = inner_conn.close();
                                        // We don't care if the receiver is closed, because we are closing the
                                        // connection anyway.
                                        send_response!(tx, result);
                                        // Once the connection is closed, we need to break the loop and exit the thread.
                                        break;
                                    }
                                    Ok(Command::Execute { statement, parameters, tx }) => {
                                        let result = inner_conn.execute(statement, parameters);
                                        send_response!(tx, result);
                                    }
                                    Ok(Command::Query { statement, parameters, tx }) => {
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

    pub fn close(self) -> BoxFuture<'static, Result<()>> {
        let (tx, rx) = oneshot::channel();
        if let Err(e) = self.command_tx.send(Command::Close { tx }) {
            return Box::pin(err::<(), Error>(Box::new(e)));
        }
        await_on!(rx)
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
