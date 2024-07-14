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
