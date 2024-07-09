use crate::connection::Connection;
use crate::driver::DriverStatement;
use crate::parameters::Parameters;
use crate::Result;
use arrow_array::RecordBatch;

/// A prepared statement.
///
/// A statement is a query that has been prepared for execution. It can be bound with parameters and executed.
pub struct Statement<'c> {
    pub(crate) inner: Box<dyn DriverStatement + 'c>,
}

impl Statement<'_> {
    pub fn bind(&mut self, parameters: Parameters) -> Result<()> {
        self.inner.bind(parameters)
    }

    pub fn execute(&mut self) -> Result<u64> {
        self.inner.execute()
    }

    pub fn query<'s: 'i, 'i>(&'s mut self) -> Result<Box<dyn Iterator<Item = Result<RecordBatch>> + 'i>> {
        self.inner.query()
    }

    pub fn query_with_params<'s: 'i, 'i>(
        &'s mut self,
        parameters: Parameters,
    ) -> Result<Box<dyn Iterator<Item = Result<RecordBatch>> + 's>> {
        self.inner.bind(parameters)?;
        self.inner.query()
    }
}

pub trait IntoStatement<'s> {
    fn into_statement<'c: 's>(self, conn: &'c Connection) -> Result<Statement<'s>>;
}

impl<'s> IntoStatement<'s> for &str {
    fn into_statement<'c: 's>(self, conn: &'c Connection) -> Result<Statement<'s>> {
        conn.prepare(self)
    }
}

impl<'s> IntoStatement<'s> for String {
    fn into_statement<'c: 's>(self, conn: &'c Connection) -> Result<Statement<'s>> {
        conn.prepare(self)
    }
}

impl<'s> IntoStatement<'s> for Statement<'s> {
    fn into_statement<'c: 's>(self, _conn: &'c Connection) -> Result<Statement<'s>> {
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        cell::RefCell,
        sync::{Arc, Mutex},
    };

    use crate::{
        connection::Connection,
        driver::{MockDriverConnection, MockDriverFactory, MockDriverStatement},
        factory::Factory,
        params, Result,
    };

    #[test]
    fn test_statement() {
        #[derive(Default)]
        struct State {
            statement: String,
            bind_parameters: usize,
        }

        let state: Arc<Mutex<RefCell<State>>> = Arc::new(Mutex::new(RefCell::new(State::default())));
        let open_state = state.clone();

        let mut mock_factory = MockDriverFactory::new();
        mock_factory.expect_schemes().returning(|| &["mock-core-statement"]);
        mock_factory.expect_open().returning(move |_| {
            let prepare_state = open_state.clone();
            let mut mock_conn = MockDriverConnection::new();
            mock_conn.expect_prepare().returning(move |statement| {
                let bind_state = prepare_state.clone();
                prepare_state.lock().unwrap().borrow_mut().statement = statement.to_string();
                let mut mock_stmt = MockDriverStatement::new();
                mock_stmt.expect_bind().returning(move |parameters| {
                    bind_state.lock().unwrap().borrow_mut().bind_parameters = parameters.len();
                    Ok(())
                });
                mock_stmt.expect_execute().returning(|| Ok(0));
                mock_stmt.expect_query().returning(|| {
                    Ok(Box::new(std::iter::empty()) as Box<dyn Iterator<Item = Result<arrow_array::RecordBatch>>>)
                });
                Ok(Box::new(mock_stmt))
            });
            Ok(Box::new(mock_conn))
        });
        Factory::register(Box::new(mock_factory));

        let conn = Connection::open("mock-core-statement://").unwrap();
        let mut stmt = conn.prepare("CREATE TABLE test(id INT)").unwrap();
        assert_eq!(state.lock().unwrap().borrow().statement, "CREATE TABLE test(id INT)");
        assert!(stmt.execute().is_ok());

        let mut stmt = conn.prepare("SELECT * FROM employee WHERE id=? AND name=?").unwrap();
        assert_eq!(state.lock().unwrap().borrow().statement, "SELECT * FROM employee WHERE id=? AND name=?");
        assert!(stmt.bind(params!(1, "Alice")).is_ok());
        assert_eq!(state.lock().unwrap().borrow().bind_parameters, 2);
        let mut rows = stmt.query().unwrap();
        assert!(rows.next().is_none());

        Factory::unregister("mock-core-statement");
    }
}
