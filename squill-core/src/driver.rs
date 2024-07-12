use crate::parameters::Parameters;
use arrow_array::RecordBatch;

#[cfg(any(test, feature = "mock"))]
use ctor::ctor;
#[cfg(any(test, feature = "mock"))]
use mockall::automock;

/// The error type that the drivers will use to return errors.
///
/// It's a pass-through error type that the drivers will use to return errors. Because each driver may have to deal with
/// specific error types coming from the underlying crate used to interact with the database, the drivers will have to
/// convert those errors to this error type.
///
/// It doesn't prevent the drivers from using {{crate::error::Error}} when appropriate but it should be converted into
/// this error type using {{crate::error::Error::into}}.
pub type DriverError = Box<dyn std::error::Error + Send + Sync>;

pub type Result<T> = std::result::Result<T, DriverError>;

#[cfg_attr(any(test, feature = "mock"), automock)]
pub trait DriverConnection {
    /// Get the name of the driver.
    ///
    /// The name of the driver should be one of the schemes used to register the driver with the factory but it's not
    /// enforced. This name if mostly intended for logging and debugging purposes.
    fn driver_name(&self) -> &str;

    /// Prepare a statement for execution.
    ///
    /// If the statement used parameters, the statement should be prepared with placeholders for the parameters. The
    /// placeholders themselves are depending on the driver implementation. For example, the placeholders could be
    /// `$1, $2, ...` when using PostgtreSQL or `?` when using SQLite and MySQL.
    fn prepare<'c, 's>(&'c self, statement: &str) -> Result<Box<dyn DriverStatement + 's>>
    where
        'c: 's;

    /// Close the connection.
    ///
    /// Since the connection may be borrowed, the connection should be closed when the last reference to the connection
    /// is dropped.
    fn close(self: Box<Self>) -> Result<()>;
}

/// A prepared statement ready to be executed.
///
/// A prepared statement can be executed multiple times with different parameters.
#[cfg_attr(any(test, feature = "mock"), automock)]
pub trait DriverStatement {
    /// Bind the parameters to the statement.
    ///
    /// The latest bound parameters will be used when the statement is executed via {{execute}} or {{query}}.
    /// The number of parameters must match the number of placeholders in the statement otherwise an error will be
    /// returned.
    fn bind(&mut self, parameters: Parameters) -> Result<()>;

    /// Execute the statement.
    ///
    /// Returns the number of rows affected by the statement.
    /// Executing a statement that starts with "SELECT" my return an error depending on the driver implementation.
    fn execute(&mut self) -> Result<u64>;

    /// Execute a `SELECT` statement.
    ///
    /// Returns an iterator over the record batches returned by the statement.
    fn query<'s>(&'s mut self) -> Result<Box<dyn Iterator<Item = Result<RecordBatch>> + 's>>;
}

#[cfg_attr(any(test, feature = "mock"), automock)]
pub trait DriverFactory: Sync + Send {
    /// Get the schemes associated with the driver.
    fn schemes(&self) -> &'static [&'static str];
    fn open(&self, uri: &str) -> Result<Box<dyn DriverConnection>>;
}

/// A factory for mocking a {{DriverConnection}}.
///
/// # Example
/// ```rust
/// use squill_core::connection::Connection;
/// use squill_core::params;
///
/// // This should return a mock connection
/// let conn = Connection::open("mock://").unwrap();
///
/// // Opening a connection with the URI "mock://?error" should return an error
/// assert!(Connection::open("mock://?error").is_err());
///
/// // Calling `prepare` should return a mock statement unless the statement is "XINSERT"
/// assert!(conn.prepare("XINSERT").is_err());
/// assert!(conn.prepare("SELECT 1").is_ok());
///
/// // Calling `bind` should return an error if the number of parameters does not match the number of placeholders
/// let mut stmt = conn.prepare("SELECT ?").unwrap();
/// assert!(stmt.bind(params!(1, 2)).is_err());
/// assert!(stmt.bind(params!(1)).is_ok());
///
/// // Calling `execute` should return an error if the statement starts with "SELECT"
/// let mut stmt = conn.prepare("INSERT").unwrap();
/// assert!(stmt.execute().is_ok());
/// let mut stmt = conn.prepare("SELECT 1").unwrap();
/// assert!(stmt.execute().is_err());
///
/// // Calling `query` should return an error if the statement does not start with "SELECT"
/// let mut stmt = conn.prepare("SELECT 1").unwrap();
/// assert!(stmt.query().is_ok());
/// let mut stmt = conn.prepare("INSERT 1").unwrap();
/// assert!(stmt.query().is_err());
/// ```
#[cfg(any(test, feature = "mock"))]
#[cfg(not(tarpaulin_include))]
impl MockDriverFactory {
    pub fn register_with_default(schemes: &'static [&'static str]) {
        let mut mock_factory = MockDriverFactory::default();
        mock_factory.expect_open().returning(|uri| match uri.contains("?error") {
            false => Ok(Box::new(MockDriverConnection::with_default())),
            true => Err("Invalid URI".into()),
        });
        mock_factory.register(schemes);
    }

    pub fn register(mut self, schemes: &'static [&'static str]) {
        self.expect_schemes().returning(move || schemes);
        crate::factory::Factory::register(Box::new(self));
    }
}

#[cfg(any(test, feature = "mock"))]
#[cfg(not(tarpaulin_include))]
impl MockDriverStatement {
    pub fn with_default(stmt: String) -> MockDriverStatement {
        let bind_stmt = stmt.clone();
        let query_stmt = stmt.clone();
        let execute_stmt = stmt.clone();
        let mut mock_statement = MockDriverStatement::new();
        mock_statement.expect_bind().returning(move |parameters| {
            match bind_stmt.matches('?').count() == parameters.len() {
                true => Ok(()),
                false => Err("Invalid parameter count".into()),
            }
        });
        mock_statement.expect_execute().returning(move || match execute_stmt.starts_with("SELECT ") {
            false => Ok(1),
            true => Err("Invalid statement".into()),
        });
        mock_statement.expect_query().returning(move || {
            match regex::Regex::new(r"^SELECT\s+([0-9]+)").unwrap().captures(query_stmt.as_str()) {
                Some(captures) => {
                    let count = captures.get(1).unwrap().as_str().parse::<u64>().unwrap();
                    if count == 0 {
                        return Ok(Box::new(std::iter::empty()));
                    }
                    let values: Vec<Option<i32>> = (1..=count).map(|n| Some(n as i32)).collect();
                    let record_batch = RecordBatch::try_new(
                        std::sync::Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                            "col0",
                            arrow_schema::DataType::Int32,
                            true,
                        )])),
                        vec![std::sync::Arc::new(arrow_array::Int32Array::from(values))],
                    )
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>);
                    Ok(Box::new(std::iter::once(record_batch)))
                }
                None => Err("Invalid statement".into()),
            }
        });
        mock_statement
    }
}

#[cfg(any(test, feature = "mock"))]
#[cfg(not(tarpaulin_include))]
impl MockDriverConnection {
    pub fn with_default() -> MockDriverConnection {
        let mut mock_connection = MockDriverConnection::default();
        mock_connection.expect_driver_name().return_const("mock".to_string());
        mock_connection.expect_close().returning(|| Ok(()));
        mock_connection.expect_prepare().returning(|stmt| match stmt {
            "XINSERT" => Err("Invalid statement".into()),
            _ => Ok(Box::new(MockDriverStatement::with_default(stmt.to_string()))),
        });
        mock_connection
    }
}

#[cfg(any(test, feature = "mock"))]
#[cfg(not(tarpaulin_include))]
#[ctor]
fn init() {
    MockDriverFactory::register_with_default(&["mock"]);
}
