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
macro_rules! query {
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
    use crate::{connection::Connection, parameters::Parameters, values::Value};
    use crate::{execute, params, query};

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
        let conn = Connection::open("mock://").unwrap();
        let _ = execute!(conn, "CREATE TABLE table (id INTEGER PRIMARY KEY, name TEXT)");
        let _ = execute!(conn, "INSERT INTO table (id, name) VALUES (?, ?)", 1, "hello world");
        let _ = execute!(conn, "INSERT INTO table (id, name) VALUES (2, 'goodbye world')");
    }

    #[test]
    fn test_query() {
        let conn = Connection::open("mock://").unwrap();
        let mut stmt = conn.prepare("SELECT * FROM table").unwrap();
        let _ = query!(stmt);

        //        let _ = query!(1, "SELECT * FROM table");
        //        let _ = query!(1, "SELECT * FROM table", 1, 2, 3);
    }
}
