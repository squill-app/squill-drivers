use crate::statement::Statement;
use arrow_array::RecordBatch;
use futures::future::{err, BoxFuture};
use squill_core::driver;
use squill_core::driver::{DriverConnection, DriverStatement};
use squill_core::error::Error;
use squill_core::factory::Factory;
use squill_core::parameters::Parameters;
use squill_core::rows::Row;
use squill_core::{clean_statement, Result};
use std::fmt::{Display, Formatter};
use std::thread;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, event, Level};

/// Convert [std::error::Error] into an [Error].
///
/// If the error is already an [Error], it is returned as is, otherwise it is wrapped in an [Error::DriverError].
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
                Ok(driver_conn) => {
                    if open_tx.send(Ok(Self { command_tx })).is_err() {
                        error!("Channel communication failed.");
                    } else if let Err(e) = Self::main_command_loop(driver_conn, command_rx) {
                        error!("Connection did not close cleanly: {}", e);
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

    /// Prepare a statement.
    ///
    /// Because of the lifetime of the statement, the connection is no longer usable until the statement is dropped.
    /// The called must use the [Statement] returned to execute or query the results.
    pub fn prepare<S: Into<String>>(&mut self, statement: S) -> BoxFuture<'_, Result<Statement<'_>>> {
        let (tx, rx) = oneshot::channel();
        let statement = statement.into();
        event!(Level::DEBUG, message = %{ clean_statement(&statement) });
        if let Err(e) = self.command_tx.send(Command::PrepareStatement { statement, tx }) {
            return Box::pin(err::<Statement<'_>, Error>(Error::DriverError { error: e.into() }));
        }
        Box::pin(async move {
            match rx.await {
                Ok(Ok(())) => Ok(Statement::new(self.command_tx.clone())),
                Ok(Err(e)) => Err(Error::DriverError { error: e }),
                Err(e) => Err(Error::InternalError { error: e.into() }),
            }
        })
    }

    /// Execute a statement.
    ///
    /// This is a convenience method that prepares a statement, binds the parameters, and executes it in one go.
    /// The response sent back to the caller is the number of rows affected by the statement.
    ///
    /// Using this command is more efficient than preparing and then executing the prepared statement because it avoids
    /// the overhead of preparing prepared statement and then execute it.
    pub fn execute<S: Into<String>>(
        &mut self,
        statement: S,
        parameters: Option<Parameters>,
    ) -> BoxFuture<'_, Result<u64>> {
        let (tx, rx) = oneshot::channel();
        let statement = statement.into();
        event!(Level::DEBUG, message = %{ clean_statement(&statement) });
        if let Err(e) = self.command_tx.send(Command::Execute { statement, parameters, tx }) {
            return Box::pin(err::<u64, Error>(Error::DriverError { error: e.into() }));
        }
        await_on!(rx)
    }

    /// Execute a query expecting to return at most one row.
    pub fn query_row<S: Into<String>>(
        &mut self,
        statement: S,
        parameters: Option<Parameters>,
    ) -> BoxFuture<'_, Result<Option<Row>>> {
        let statement: String = statement.into();
        Box::pin(async move {
            let mut statement = self.prepare(statement).await?;
            statement.query_row(parameters).await
        })
    }

    pub fn query_map_row<'c, 's, 'r, S, F, T>(
        &'c mut self,
        statement: S,
        parameters: Option<Parameters>,
        mapping_fn: F,
    ) -> BoxFuture<'r, Result<Option<T>>>
    where
        'c: 'r,
        's: 'r,
        S: Into<String> + 's,
        F: FnOnce(Row) -> std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>
            + std::marker::Send
            + 'r + 's,
    {
        let statement: String = statement.into();
        Box::pin(async move {
            let mut statement = self.prepare(statement).await?;
            statement.query_map_row(parameters, mapping_fn).await
        })
    }
}

pub(crate) enum Command {
    Close { tx: oneshot::Sender<driver::Result<()>> },
    DropStatement { tx: oneshot::Sender<driver::Result<()>> },
    DropCursor,
    Execute { statement: String, parameters: Option<Parameters>, tx: oneshot::Sender<driver::Result<u64>> },
    ExecutePreparedStatement { parameters: Option<Parameters>, tx: oneshot::Sender<driver::Result<u64>> },
    FetchCursor { tx: mpsc::Sender<driver::Result<Option<RecordBatch>>> },
    PrepareStatement { statement: String, tx: oneshot::Sender<driver::Result<()>> },
    QueryPreparedStatement { parameters: Option<Parameters>, tx: oneshot::Sender<driver::Result<()>> },
}

impl Display for Command {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Command::Close { .. } => write!(f, "Close"),
            Command::DropStatement { .. } => write!(f, "DropStatement"),
            Command::DropCursor => write!(f, "DropCursor"),
            Command::Execute { statement, .. } => write!(f, "Execute: {}", statement),
            Command::ExecutePreparedStatement { .. } => write!(f, "ExecutePreparedStatement"),
            Command::FetchCursor { .. } => write!(f, "FetchCursor"),
            Command::PrepareStatement { statement, .. } => write!(f, "PrepareStatement: {}", statement),
            Command::QueryPreparedStatement { .. } => write!(f, "QueryPreparedStatement"),
        }
    }
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

/// Send a response back to the caller and return an error if the channel is closed.
///
/// This is a convenience function to be used instead of `tx.send(value)`. When the channel is closed  `tx.send(value)`
/// return a [std::result::Result] but the error variant of it if not an error but the value that was not sent. Using
/// this method makes the code easier to return an error when the channel is closed.
///
/// # Example
/// ```rust,ignore
/// send_response(value)?;
/// ```
fn send_response<T>(tx: oneshot::Sender<driver::Result<T>>, value: driver::Result<T>) -> Result<()> {
    if tx.send(value).is_err() {
        let error =
            Error::InternalError { error: "Channel communication failed while sending command response.".into() };
        error!("{}", error);
        return Err(error);
    }
    Ok(())
}

impl Connection {
    ///
    /// The main command loop for the connection.
    ///
    fn main_command_loop(
        mut driver_conn: Box<dyn DriverConnection>,
        command_rx: crossbeam_channel::Receiver<Command>,
    ) -> Result<()> {
        loop {
            let command = command_rx.recv();
            match command {
                //
                // Close the connection.
                //
                Ok(Command::Close { tx }) => {
                    let result = driver_conn.close();
                    // We don't care if the receiver is closed, because we are closing the
                    // connection anyway.
                    send_response(tx, result)?;
                    // Once the connection is closed, we need to break the loop and exit the thread.
                    break;
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
                Ok(Command::Execute { statement, parameters, tx }) => match driver_conn.prepare(&statement) {
                    Ok(mut stmt) => send_response(tx, stmt.execute(parameters))?,
                    Err(e) => send_response(tx, Err(e))?,
                },

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
                Ok(Command::PrepareStatement { statement, tx }) => match driver_conn.prepare(&statement) {
                    Ok(mut stmt) => {
                        send_response(tx, Ok(()))?;
                        Self::stmt_command_loop(&mut *stmt, command_rx.clone())?;
                    }
                    Err(e) => {
                        send_response_and_break_on_error!(tx, Err(e));
                    }
                },

                //
                // Unexpected command.
                //
                Ok(command) => {
                    error!("Unexpected command: {}", command);
                    break;
                }

                //
                // The channel is closed (connection is closed).
                //
                Err(_e) => {
                    error!("Channel communication failed.");
                    break;
                }
            }
        }
        Ok(())
    }

    ///
    /// Processing commands for a statement.
    ///
    fn stmt_command_loop(
        driver_stmt: &mut dyn DriverStatement,
        command_rx: crossbeam_channel::Receiver<Command>,
    ) -> Result<()> {
        loop {
            let command = command_rx.recv();
            match command {
                //
                // Execute a prepared statement.
                //
                Ok(Command::ExecutePreparedStatement { parameters, tx }) => {
                    let res = driver_stmt.execute(parameters);
                    send_response::<u64>(tx, res)?;
                }

                //
                // Query a prepared statement.
                //
                Ok(Command::QueryPreparedStatement { parameters, tx }) => match driver_stmt.query(parameters) {
                    Ok(mut iter) => {
                        send_response(tx, Ok(()))?;
                        Self::cursor_command_loop(&mut iter, command_rx.clone())?;
                    }
                    Err(e) => {
                        send_response(tx, Err(e))?;
                    }
                },

                Ok(Command::DropStatement { tx }) => {
                    //
                    // Drop a prepared statement (the caller is waiting for the response before it can re-use the connection).
                    //
                    send_response(tx, Ok(()))?;
                    break;
                }

                Ok(command) => {
                    //
                    // Unexpected command.
                    //
                    error!("Unexpected command: {}", command);
                    return Err(Error::InternalError {
                        error: format!("Unexpected command while processing a statement: {}", command).into(),
                    });
                }

                //
                // The channel is closed (connection is closed).
                //
                Err(e) => {
                    // This is not expected to happen because the connection is closed before the statement is dropped.
                    return Err(Error::InternalError { error: e.into() });
                }
            }
        }
        Ok(())
    }

    ///
    /// Processing commands for a cursor.
    ///
    fn cursor_command_loop(
        driver_iter: &mut dyn Iterator<
            Item = std::result::Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>>,
        >,
        command_rx: crossbeam_channel::Receiver<Command>,
    ) -> Result<()> {
        loop {
            let command = command_rx.recv();
            match command {
                //
                // Fetch the next record batch.
                //
                Ok(Command::FetchCursor { tx }) => {
                    match driver_iter.next() {
                        Some(Ok(batch)) => {
                            blocking_send_response_and_break_on_error!(tx, Ok(Some(batch)));
                        }
                        None => {
                            // The iterator is exhausted.
                            // We are not expecting to receive any more fetch commands for it but still we need to wait
                            // for the DropCursor command to break the loop.
                            blocking_send_response_and_break_on_error!(tx, Ok(None));
                        }
                        Some(Err(e)) => {
                            // An error occurred while fetching the next record batch.
                            // This is a fatal error for this query and we are not expecting to
                            // receive any more fetch commands for it but still we need to wait
                            // for the DropCursor command to break the loop.
                            error!("Error getting next record batch: {:?}", e);
                            blocking_send_response_and_break_on_error!(tx, Err(e));
                        }
                    }
                }

                //
                // Drop the cursor.
                //
                Ok(Command::DropCursor) => {
                    // The cursor is dropped, so we need to break the cursor loop.
                    break;
                }

                Ok(command) => {
                    //
                    // Unexpected command.
                    //
                    error!("Unexpected command: {}", command);
                    return Err(Error::InternalError {
                        error: format!("Unexpected command while processing a cursor: {}", command).into(),
                    });
                }

                //
                // The channel is closed (connection is closed).
                //
                Err(e) => {
                    // This is not expected to happen because the connection is closed before the statement is dropped.
                    return Err(Error::InternalError { error: e.into() });
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::Connection;
    use futures::StreamExt;
    use squill_core::{assert_ok, assert_some, assert_some_ok, params};

    #[tokio::test]
    async fn test_open() {
        assert!(Connection::open("unknown://").await.is_err());
        assert!(Connection::open("mock://").await.is_ok());
    }

    #[tokio::test]
    async fn test_prepare() {
        let mut conn = Connection::open("mock://").await.unwrap();
        assert_ok!(conn.prepare("SELECT 1").await);
        assert!(conn.prepare("XINSERT").await.is_err());
    }

    #[tokio::test]
    async fn test_execute() {
        let mut conn = assert_ok!(Connection::open("mock://").await);

        // Using string statement
        assert_ok!(conn.execute("INSERT 1", None).await);
        assert!(conn.execute("SELECT 1", None).await.is_err()); // SELECT is not allowed
        assert!(conn.execute("INSERT 1", params!(1)).await.is_err()); // bind parameters mismatch

        // Using prepared statement
        assert_ok!(assert_ok!(conn.prepare("INSERT 1").await).execute(None).await);
        assert!(assert_ok!(conn.prepare("SELECT 1").await).execute(None).await.is_err());
        assert!(assert_ok!(conn.prepare("INSERT 1").await).execute(params!(1)).await.is_err());
    }

    #[tokio::test]
    async fn test_query_rows() {
        let mut conn = assert_ok!(Connection::open("mock://").await);

        // Empty result
        let mut stmt = assert_ok!(conn.prepare("SELECT 0").await);
        let mut rows = assert_ok!(stmt.query_rows(None).await);
        assert!(rows.next().await.is_none());
        drop(rows);
        drop(stmt);

        // Some rows.
        let mut stmt = assert_ok!(conn.prepare("SELECT 2").await);
        let mut rows = assert_ok!(stmt.query_rows(None).await);
        assert_eq!(assert_some_ok!(rows.next().await).get::<_, i32>(0), 1);
        assert_eq!(assert_some_ok!(rows.next().await).get::<_, i32>(0), 2);
        assert!(rows.next().await.is_none());
        drop(rows);
        drop(stmt);

        // Error af the first iteration
        let mut stmt = assert_ok!(conn.prepare("SELECT -1").await);
        let mut rows = assert_ok!(stmt.query_rows(None).await);
        assert!(rows.next().await.unwrap().is_err());
    }

    #[tokio::test]
    async fn test_query_row() {
        let mut conn = assert_ok!(Connection::open("mock://").await);

        assert_eq!(assert_some!(assert_ok!(conn.query_row("SELECT 2", None).await)).get::<_, i32>(0), 1);
        assert_eq!(assert_some!(assert_ok!(conn.query_row("SELECT 1", None).await)).get::<_, i32>(0), 1);
        assert!(assert_ok!(conn.query_row("SELECT 0", None).await).is_none());
        assert!(conn.query_row("SELECT -1", None).await.is_err());
        assert!(conn.query_row("SELECT X", None).await.is_err());

        // using a prepared statement (that can be called multiple times)
        let mut stmt = conn.prepare("SELECT 1").await.unwrap();
        assert_eq!(assert_some!(assert_ok!(stmt.query_row(None).await)).get::<_, i32>(0), 1);
        assert_eq!(assert_some!(assert_ok!(stmt.query_row(None).await)).get::<_, i32>(0), 1);
    }

    #[tokio::test]
    async fn test_connection_query_map_row() {
        struct TestUser {
            id: i32,
            username: String,
        }

        let mut conn = assert_ok!(Connection::open("mock://").await);

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
    async fn test_query() {
        let mut conn = assert_ok!(Connection::open("mock://").await);

        let mut stmt = assert_ok!(conn.prepare("SELECT 1").await);
        assert!(stmt.query(params!("hello")).await.is_err());
        let mut iter = assert_ok!(stmt.query(None).await);
        assert!(iter.next().await.is_some());
        assert!(iter.next().await.is_none());
        drop(iter);
        drop(stmt);

        let mut stmt = assert_ok!(conn.prepare("INSERT 1").await);
        assert!(stmt.query(None).await.is_err());
        drop(stmt);

        // Testing not exhausting the iterator, if the iterator was not dropped
        let mut stmt = assert_ok!(conn.prepare("SELECT 1").await);
        let mut iter = assert_ok!(stmt.query(None).await);
        let _ = assert_some!(iter.next().await);
        drop(iter);
        drop(stmt);

        let mut stmt = assert_ok!(conn.prepare("SELECT 1").await);
        let mut iter = assert_ok!(stmt.query(None).await);
        let _ = assert_some!(iter.next().await);
        drop(iter);
        drop(stmt);

        // Test using Statement::query
        let mut stmt = conn.prepare("SELECT 1").await.unwrap();
        let mut iter = stmt.query(None).await.unwrap();
        let _ = assert_some!(iter.next().await);
    }
}
