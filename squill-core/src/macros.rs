#[macro_export]
macro_rules! params {
    () => {
        None
    };
    ($($param:expr),+ $(,)?) => {
        Some($crate::parameters::Parameters::from_slice(&[$(&$param as &dyn $crate::values::ToValue),+] as &[&dyn $crate::values::ToValue]))
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
        if bind_parameters.is_empty() {
            $conn.execute($command, None)
        }
        else {
            $conn.execute($command, Some($crate::parameters::Parameters::Positional(bind_parameters)))
        }

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
        $statement.query(Some($crate::parameters::Parameters::Positional(bind_parameters)))
    }};
}
