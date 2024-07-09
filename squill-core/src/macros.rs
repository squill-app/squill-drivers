#[macro_export]
macro_rules! params {
    () => {
        &[] as &[&dyn $crate::values::ToValue]
    };
    ($($param:expr),+ $(,)?) => {
        $crate::parameters::Parameters::from_slice(&[$(&$param as &dyn $crate::values::ToValue),+] as &[&dyn $crate::values::ToValue])
    };
}

#[macro_export]
macro_rules! execute {
    ($conn:expr, $command:expr $(, $rest:expr)*) => {{
        let bind_parameters: Vec<$crate::values::Value> = vec![
            $(
                $rest.into(),
            )*
        ];
        $conn.execute($command, $crate::parameters::Parameters::Positional(bind_parameters))
    }};
}

#[macro_export]
macro_rules! query_arrow {
    ($statement:expr $(, $rest:expr)*) => {{
        let bind_parameters: Vec<$crate::values::Value> = vec![
            $(
                $rest.into(),
            )*
        ];
        $statement.query_with_params($crate::parameters::Parameters::Positional(bind_parameters))
    }};
}

#[cfg(test)]
mod tests {
    use crate::driver::{MockDriverConnection, MockDriverFactory, MockDriverStatement};
    use crate::factory::Factory;
    use crate::Result;
    use crate::{connection::Connection, parameters::Parameters, values::Value};
    use crate::{execute, params, query_arrow};
    use std::cell::RefCell;
    use std::sync::{Arc, Mutex};

    #[test]
    fn test_params() {
        assert!(params!().is_empty());
        let parameters = params!(1, "hello world", 3.72);
        match parameters {
            Parameters::Positional(values) => {
                assert_eq!(values.len(), 3);
                assert_eq!(values[0], Value::Int32(1));
                assert_eq!(values[1], Value::String("hello world".into()));
                assert_eq!(values[2], Value::Float64(3.72));
            }
            _ => panic!("Expected positional parameters"),
        }
    }

    #[test]
    fn test_execute() {
        let mut mock_factory = MockDriverFactory::new();
        mock_factory.expect_schemes().returning(|| &["mock-core-macros"]);
        mock_factory.expect_open().returning(|_| {
            let mut mock_conn = MockDriverConnection::new();
            mock_conn.expect_prepare().returning(|_| {
                let state: Arc<Mutex<RefCell<usize>>> = Arc::new(Mutex::new(RefCell::new(0)));
                let mut mock_stmt = MockDriverStatement::new();
                let bind_state = state.clone();
                let execute_state = state.clone();
                mock_stmt.expect_bind().returning(move |parameters| {
                    *bind_state.lock().unwrap().borrow_mut() = parameters.len();
                    Ok(())
                });
                mock_stmt.expect_execute().returning(move || Ok(*execute_state.lock().unwrap().borrow() as u64));
                mock_stmt.expect_query().returning(|| {
                    Ok(Box::new(std::iter::empty()) as Box<dyn Iterator<Item = Result<arrow_array::RecordBatch>>>)
                });
                Ok(Box::new(mock_stmt))
            });
            Ok(Box::new(mock_conn))
        });
        Factory::register(Box::new(mock_factory));

        let conn = Connection::open("mock-core-macros://").unwrap();

        // execute! macro should bind the parameters and execute the statement
        assert_eq!(execute!(conn, "CREATE TABLE table (id INTEGER PRIMARY KEY, name TEXT)").unwrap(), 0);
        assert_eq!(execute!(conn, "INSERT INTO table (id, name) VALUES (?, ?)", 1, "hello world").unwrap(), 2);

        // query_arrow! macro should bind the parameters and query the statement
        let mut stmt = conn.prepare("SELECT * FROM table").unwrap();
        assert!(query_arrow!(stmt, 1, "hello world").is_ok());

        Factory::unregister("mock-core-macros");
    }
}
