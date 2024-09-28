use crate::statement::{Statement, StatementRef};
use crate::{RecordBatchStream, RowStream};
use arrow_array::RecordBatch;
use futures::future::{err, BoxFuture};
use futures::StreamExt;
use squill_core::driver;
use squill_core::driver::{DriverConnection, DriverStatement};
use squill_core::factory::Factory;
use squill_core::parameters::Parameters;
use squill_core::rows::Row;
use squill_core::{clean_statement, Error, Result};
use std::collections::HashMap;
use std::thread;
use tokio::sync::oneshot;
use tracing::{debug, error, event, Level};

/// An handle to an object owned by the connection's thread.
pub(crate) type Handle = u64;

pub(crate) fn into_error(e: Box<dyn std::error::Error + Send + Sync>) -> Error {
    match e.downcast::<Error>() {
        Ok(error) => *error,
        Err(e) => Error::DriverError { error: e },
    }
}

#[macro_export]
macro_rules! await_on {
    ($rx:expr) => {
        Box::pin(async move {
            match $rx.await {
                Ok(Ok(value)) => Ok(value),
                Ok(Err(e)) => Err(into_error(e)),
                Err(e) => Err(Error::InternalError { error: e.into() }),
            }
        })
    };
}

// The `Connection` struct is a non-blocking version of the `squill_core::connection::Connection`.
//
// The async version of the connection is based on a thread that runs the blocking operations and a command channel
// that allows sending commands to the thread.
//
// The thread is spawned when the connection is opened and runs a loop that waits for commands on the command channel.
// The commands are executed by the thread and the results are sent back to the caller through the command channel.
// The thread is stopped when the connection is closed of if any error occurs while trying to send a response back to
// the caller (e.g. the receiver is closed) and so no further commands can be executed on that connection.
//
// The blocking `Connection` and `Statement` objects owned by the thread and never cross the thread boundary, when a new
// `Statement` is created, a handle (i64) to the statement is sent back to the caller and the caller uses the handle to
// identify the statement when sending commands to the thread.
//
// Most of the methods of the `Connection` and `Statement` structs are expecting a the mutable reference of themselves
// (`&mut self`), this is not a requirement of the async version of the connection but a design choice to avoid an
// inconstancy between the blocking and non-blocking versions of the library.

/// A non-blocking connection to a data source.
pub struct Connection {
    pub(crate) command_tx: crossbeam_channel::Sender<Command>,
}

impl Connection {
    pub fn open<T: Into<String>>(uri: T) -> BoxFuture<'static, Result<Self>> {
        let (command_tx, command_rx): (crossbeam_channel::Sender<Command>, crossbeam_channel::Receiver<Command>) =
            crossbeam_channel::bounded(1);
        let uri: String = uri.into();
        let (open_tx, open_rx) = oneshot::channel();
        debug!("Opening: {}", uri);

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
        let statement = statement.into();
        event!(Level::DEBUG, message = %{ clean_statement(&statement) });
        if let Err(e) = self.command_tx.send(Command::PrepareStatement { statement, tx }) {
            return Box::pin(err::<Statement<'_>, Error>(Error::DriverError { error: e.into() }));
        }
        Box::pin(async move {
            match rx.await {
                Ok(Ok(handle)) => Ok(Statement::new(handle, self.command_tx.clone())),
                Ok(Err(e)) => Err(Error::DriverError { error: e }),
                Err(e) => Err(Error::InternalError { error: e.into() }),
            }
        })
    }

    pub fn execute<'conn, 'r, 's: 'r, S: Into<StatementRef<'r, 's>>>(
        &'conn self,
        command: S,
        parameters: Option<Parameters>,
    ) -> BoxFuture<'r, Result<u64>>
    where
        'conn: 's,
    {
        match command.into() {
            StatementRef::Str(s) => {
                let (tx, rx) = oneshot::channel();
                let statement = s.to_string();
                event!(Level::DEBUG, message = %{ clean_statement(&statement) });
                if let Err(e) = self.command_tx.send(Command::Execute { statement, parameters, tx }) {
                    return Box::pin(err::<u64, Error>(Error::DriverError { error: e.into() }));
                }
                await_on!(rx)
            }
            StatementRef::Statement(statement) => Box::pin(async move { statement.execute(parameters).await }),
        }
    }

    pub fn query_arrow<'s, 'conn: 's>(
        &'conn self,
        statement: &'s mut Statement<'conn>,
        parameters: Option<Parameters>,
    ) -> BoxFuture<'s, Result<RecordBatchStream<'s>>> {
        let (tx, rx) = oneshot::channel();
        if let Err(e) = self.command_tx.send(Command::Query { handle: statement.handle, parameters, tx }) {
            return Box::pin(err::<RecordBatchStream<'s>, Error>(Error::DriverError { error: e.into() }));
        }
        Box::pin(async move {
            match rx.await {
                Ok(Ok(())) => Ok(RecordBatchStream::new(self.command_tx.clone())),
                Ok(Err(error)) => Err(Error::DriverError { error }),
                Err(error) => Err(Error::DriverError { error: error.into() }),
            }
        })
    }

    pub fn query_rows<'s, 'conn: 's>(
        &'conn self,
        statement: &'s mut Statement<'conn>,
        parameters: Option<Parameters>,
    ) -> BoxFuture<'s, Result<RowStream<'s>>> {
        Box::pin(async move {
            let stream = self.query_arrow(statement, parameters).await?;
            Ok(RowStream::from(stream))
        })
    }

    pub fn query_row<'conn, 'r, 's: 'r, S: Into<StatementRef<'r, 's>>>(
        &'conn self,
        query: S,
        parameters: Option<Parameters>,
    ) -> BoxFuture<'r, Result<Option<Row>>>
    where
        'conn: 's,
    {
        match query.into() {
            StatementRef::Str(s) => Box::pin(async move {
                let mut statement = self.prepare(s).await?;
                self.query_row_inner(&mut statement, parameters).await
            }),
            StatementRef::Statement(statement) => {
                Box::pin(async move { self.query_row_inner(statement, parameters).await })
            }
        }
    }

    /// Query a statement that is expected to return a single row and map it to a value.
    ///
    /// Returns `Ok(None)` if the query returned no rows.
    /// If the query returns more than one row, the function will return an the first row and ignore the rest.
    pub fn query_map_row<'conn, 'r, 's: 'r, S: Into<StatementRef<'r, 's>>, F, T>(
        &'conn self,
        query: S,
        parameters: Option<Parameters>,
        mapping_fn: F,
    ) -> BoxFuture<'r, Result<Option<T>>>
    where
        'conn: 's,
        F: FnOnce(Row) -> std::result::Result<T, Box<dyn std::error::Error + Send + Sync>> + std::marker::Send + 'r,
    {
        match query.into() {
            StatementRef::Str(s) => Box::pin(async move {
                let mut statement = self.prepare(s).await?;
                self.query_map_row_inner(&mut statement, parameters, mapping_fn).await
            }),
            StatementRef::Statement(statement) => {
                Box::pin(async move { self.query_map_row_inner(statement, parameters, mapping_fn).await })
            }
        }
    }

    // This method is used to avoid code duplication in the `query_row` method.
    async fn query_row_inner<'s, 'conn: 's>(
        &'conn self,
        statement: &'s mut Statement<'conn>,
        parameters: Option<Parameters>,
    ) -> Result<Option<Row>> {
        let mut rows = self.query_rows(statement, parameters).await?;
        match rows.next().await {
            Some(Ok(row)) => Ok(Some(row)),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }

    // This method is used to avoid code duplication in the `query_map_row` method.
    async fn query_map_row_inner<'s, 'conn: 's, F, T>(
        &'conn self,
        statement: &'s mut Statement<'conn>,
        parameters: Option<Parameters>,
        mapping_fn: F,
    ) -> Result<Option<T>>
    where
        F: FnOnce(Row) -> std::result::Result<T, Box<dyn std::error::Error + Send + Sync>> + std::marker::Send,
    {
        let row = self.query_row_inner(statement, parameters).await?;
        match row {
            Some(row) => Ok(Some(mapping_fn(row)?)),
            None => Ok(None),
        }
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

// A set of macros to simplify the code in the command loop.
//
// The macros are used to send responses back to the caller and break the loop if an error occurs.
// The loop must be broken if an error occurs while sending a response back to the caller because the connection is no
// longer usable if we were not able to send a response from a command.

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
                //
                // Bind parameters to a prepared statement.
                //
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
                //
                Ok(Command::DropStatement { handle }) => {
                    prepared_statements.remove(&handle);
                }

                //
                // Prepare and execute a statement at once.
                //
                // This is a convenience method that prepares a statement, binds the parameters, and
                // executes it in one go. The response sent back to the caller is the number of rows
                // affected by the statement.
                // Using this command is more efficient than preparing and then executing the
                // prepared statement because it avoids the overhead of sending back the handle to
                // the prepared statement and then run a second command to execute it.
                //
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

                //
                // Execute a prepared statement.
                //
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

                //
                // Execute a query.
                //
                // The query is executed and the response sent back to the caller once we have the iterator on the
                // result, then we enter  loop waiting for commands to fetch the rows. This design driven by the
                // lifetime of the iterator that is tied to the statement which need stay in scope until the iterator
                // is no longer used.
                //
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
                                        //
                                        // Drop the cursor.
                                        //
                                        Ok(Command::DropCursor) => {
                                            // The cursor is dropped, so we need to break the cursor loop.
                                            break;
                                        }

                                        //
                                        // Fetch the next record batch.
                                        //
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

                                        //
                                        // The channel is closed (connection is closed).
                                        //
                                        Err(_e) => {
                                            break;
                                        }

                                        //
                                        // Unexpected command.
                                        //
                                        _ => {
                                            // We are not expecting any other command while fetching rows.
                                            // If this occurs, this is likely because an iterator on a query has not
                                            // been exhausted and not dropped before trying to re-use the connection for
                                            // another operation. This is a programming error and the code should be
                                            // fixed by either exhausting the iterator or dropping it.
                                            panic!("Unexpected command while fetching rows.");
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
                //
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

                //
                // Unexpected FetchCursor command.
                //
                Ok(Command::FetchCursor { tx }) => {
                    // This command is not expected at this point. It means the developer is trying to use an iterator
                    // that is already exhausted.
                    blocking_send_response_and_break_on_error!(tx, Ok(None));
                    break;
                }

                //
                // Unexpected DropCursor command.
                //
                Ok(Command::DropCursor) => {
                    // This happen when the RecordBatchStream is dropped. We should ignore it because it simply means
                    // that either an error occurred while fetching the next record batch or the iterator is exhausted.
                    // There is nothing to do here since this command doesn't expect a response.
                }

                //
                // The channel is closed (connection is closed).
                //
                Err(_e) => {
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
    async fn test_bind() {
        let conn = Connection::open("mock://").await.unwrap();
        let mut stmt = conn.prepare("SELECT ?").await.unwrap();
        assert!(stmt.bind(params!(1)).await.is_ok());
        assert!(stmt.bind(params!(1, 2)).await.is_err());
    }

    #[tokio::test]
    async fn test_execute() {
        let conn = Connection::open("mock://").await.unwrap();

        // Using string statement
        assert_eq!(conn.execute("INSERT 1", None).await.unwrap(), 1);
        assert!(conn.execute("SELECT 1", None).await.is_err()); // SELECT is not allowed
        assert!(conn.execute("INSERT 1", params!(1)).await.is_err()); // bind parameters mismatch

        // Using prepared statement
        let mut stmt = conn.prepare("INSERT 1").await.unwrap();
        assert!(conn.execute(&mut stmt, None).await.is_ok());
        drop(stmt);

        let mut stmt = conn.prepare("SELECT 1").await.unwrap();
        assert!(conn.execute(&mut stmt, None).await.is_err());
        let mut stmt = conn.prepare("INSERT 1").await.unwrap();
        assert!(conn.execute(&mut stmt, params!(1)).await.is_err());
    }

    #[tokio::test]
    async fn test_query_rows() {
        let conn = Connection::open("mock://").await.unwrap();

        // Empty result
        let mut stmt = conn.prepare("SELECT 0").await.unwrap();
        let mut rows = conn.query_rows(&mut stmt, None).await.unwrap();
        assert!(rows.next().await.is_none());

        // More that one row.
        let mut stmt = conn.prepare("SELECT 2").await.unwrap();
        let mut rows = conn.query_rows(&mut stmt, None).await.unwrap();
        assert_eq!(rows.next().await.unwrap().unwrap().get::<_, i32>(0), 1);
        assert_eq!(rows.next().await.unwrap().unwrap().get::<_, i32>(0), 2);
        assert!(rows.next().await.is_none());

        // Error af the first iteration
        let mut stmt = conn.prepare("SELECT -1").await.unwrap();
        let mut rows = conn.query_rows(&mut stmt, None).await.unwrap();
        assert!(rows.next().await.unwrap().is_err());
    }

    #[tokio::test]
    async fn test_query_row() {
        let conn = Connection::open("mock://").await.unwrap();

        assert_eq!(conn.query_row("SELECT 2", None).await.unwrap().unwrap().get::<_, i32>(0), 1);
        assert_eq!(conn.query_row("SELECT 1", None).await.unwrap().unwrap().get::<_, i32>(0), 1);
        assert!(conn.query_row("SELECT 0", None).await.unwrap().is_none());
        assert!(conn.query_row("SELECT -1", None).await.is_err());
        assert!(conn.query_row("SELECT X", None).await.is_err());

        let mut stmt = conn.prepare("SELECT 1").await.unwrap();
        assert_eq!(conn.query_row(&mut stmt, None).await.unwrap().unwrap().get::<_, i32>(0), 1);
        assert_eq!(conn.query_row(&mut stmt, None).await.unwrap().unwrap().get::<_, i32>(0), 1);
    }

    #[tokio::test]
    async fn test_connection_query_map_row() {
        struct TestUser {
            id: i32,
            username: String,
        }

        let conn = Connection::open("mock://").await.unwrap();

        // some rows
        let user = conn
            .query_map_row("SELECT 1", None, |row| {
                Ok(TestUser { id: row.get::<_, _>(0), username: row.get::<_, _>(1) })
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(user.id, 1);
        assert_eq!(user.username, "user1");

        // no rows
        assert!(conn
            .query_map_row("SELECT 0", None, |row| {
                Ok(TestUser { id: row.get::<_, _>(0), username: row.get::<_, _>(1) })
            })
            .await
            .unwrap()
            .is_none());

        // error by the driver
        assert!(conn
            .query_map_row("SELECT -1", None, |row| {
                Ok(TestUser { id: row.get::<_, _>(0), username: row.get::<_, _>(1) })
            })
            .await
            .is_err());

        // error by the mapping function
        assert!(conn
            .query_map_row("SELECT 1", None, |row| {
                if row.get::<_, i32>(0) == 1 {
                    Err("Error".into())
                } else {
                    Ok(TestUser { id: row.get::<_, _>(0), username: row.get::<_, _>(1) })
                }
            })
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_query_arrow() {
        let conn = Connection::open("mock://").await.unwrap();

        let mut stmt = conn.prepare("SELECT 1").await.unwrap();
        assert!(conn.query_arrow(&mut stmt, params!("hello")).await.is_err());
        let mut iter = conn.query_arrow(&mut stmt, None).await.unwrap();
        assert!(iter.next().await.is_some());
        assert!(iter.next().await.is_none());
        drop(iter);
        drop(stmt);

        let mut stmt = conn.prepare("INSERT 1").await.unwrap();
        assert!(conn.query_arrow(&mut stmt, None).await.is_err());
        drop(stmt);

        // Testing not exhausting the iterator, if the iterator was not dropped
        let mut stmt = conn.prepare("SELECT 1").await.unwrap();
        let mut iter = conn.query_arrow(&mut stmt, None).await.unwrap();
        assert!(iter.next().await.is_some());
        drop(iter);
        drop(stmt);

        let mut stmt = conn.prepare("SELECT 1").await.unwrap();
        let mut iter = conn.query_arrow(&mut stmt, None).await.unwrap();
        assert!(iter.next().await.is_some());
        drop(iter);
        drop(stmt);

        // Test using Statement::query
        let mut stmt = conn.prepare("SELECT 1").await.unwrap();
        let mut iter = stmt.query().await.unwrap();
        assert!(iter.next().await.is_some());
    }
}
