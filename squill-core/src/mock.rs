use crate::driver::MockDriverConnection;
use crate::driver::MockDriverFactory;
use crate::driver::MockDriverStatement;
use crate::driver::Result;
use arrow_array::RecordBatch;

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
/// // Calling `execute` should return an error if the statement starts with "SELECT"
/// let mut stmt = conn.prepare("INSERT").unwrap();
/// assert!(stmt.execute(None).is_ok());
/// let mut stmt = conn.prepare("SELECT 1").unwrap();
/// assert!(stmt.execute(None).is_err());
///
/// // Calling `query` should return an error if the statement does not start with "SELECT" followed by a number
/// let mut stmt = conn.prepare("SELECT 1").unwrap(); // positive number returns a single batch with the number of records
/// assert!(stmt.query(None).is_ok());
/// let mut stmt = conn.prepare("SELECT 0").unwrap(); // return an empty iterator
/// let mut iter = stmt.query(None).unwrap();
/// assert!(iter.next().is_none());
/// let mut stmt = conn.prepare("SELECT -1").unwrap(); // negative number an iterator that will fail at the first iteration
/// let mut iter = stmt.query(None).unwrap();
/// assert!(iter.next().unwrap().is_err());
/// let mut stmt = conn.prepare("INSERT 1").unwrap(); // anything else returns an error
/// assert!(stmt.query(None).is_err());
/// ```
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

impl MockDriverStatement {
    pub fn with_default(stmt: String) -> MockDriverStatement {
        // let bind_stmt = stmt.clone();
        let query_stmt = stmt.clone();
        let execute_stmt = stmt.clone();
        let mut mock_statement = MockDriverStatement::new();
        /*
        mock_statement.expect_bind().returning(move |parameters| {
            match bind_stmt.matches('?').count().cmp(&parameters.len()) {
                std::cmp::Ordering::Equal => Ok(()),
                _ => Err("Invalid parameter count".into()),
            }
        });
        */
        mock_statement.expect_execute().returning(move |parameters| match execute_stmt.starts_with("SELECT ") {
            false => {
                if parameters.is_some() && execute_stmt.matches('?').count() != parameters.unwrap().len() {
                    return Err("Invalid parameter count".into());
                }
                Ok(1)
            }
            true => Err("Invalid statement".into()),
        });
        mock_statement.expect_query().returning(move |parameters| {
            if parameters.is_some() && query_stmt.matches('?').count() != parameters.unwrap().len() {
                return Err("Invalid parameter count".into());
            }
            match regex::Regex::new(r"^SELECT\s+(-?[0-9]+)").unwrap().captures(query_stmt.as_str()) {
                Some(captures) => {
                    let count = captures.get(1).unwrap().as_str().parse::<i64>().unwrap();
                    match count {
                        _ if count < 0 => {
                            // Fails at the first iteration
                            Ok(Box::new(std::iter::once(Err("Invalid count".into()) as Result<RecordBatch>)))
                        }
                        0 => {
                            // No records
                            Ok(Box::new(std::iter::empty()))
                        }
                        _ => {
                            // Returns a single batch that contains `count` of records
                            let ids: Vec<Option<i32>> = (1..=count).map(|n| Some(n as i32)).collect();
                            let usernames: Vec<Option<String>> =
                                (1..=count).map(|n| Some(format!("user{}", n))).collect();
                            let record_batch = RecordBatch::try_new(
                                std::sync::Arc::new(arrow_schema::Schema::new(vec![
                                    arrow_schema::Field::new("id", arrow_schema::DataType::Int32, true),
                                    arrow_schema::Field::new("username", arrow_schema::DataType::Utf8, true),
                                ])),
                                vec![
                                    std::sync::Arc::new(arrow_array::Int32Array::from(ids)),
                                    std::sync::Arc::new(arrow_array::StringArray::from(usernames)),
                                ],
                            )
                            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>);
                            Ok(Box::new(std::iter::once(record_batch)))
                        }
                    }
                }
                None => Err(format!("Invalid statement: {}", query_stmt).into()),
            }
        });
        mock_statement
    }
}

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

pub fn register_driver() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        MockDriverFactory::register_with_default(&["mock"]);
    });
}
