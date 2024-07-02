#[macro_export]
macro_rules! bind {
    () => {
        &[] as &[&dyn squill_core::parameters::ToParameter]
    };
    ($($param:expr),+ $(,)?) => {
        $crate::parameters::Parameters::from_slice(&[$(&$param as &dyn $crate::parameters::ToParameter),+] as &[&dyn $crate::parameters::ToParameter])
    };
}

#[macro_export]
macro_rules! execute {
    ($obj:expr, $statement:expr $(, $rest:expr)*) => {
        {
        let bind_parameters: Vec<$crate::parameters::Parameter> = vec![
            $(
                $rest.into(),
            )*
        ];
        $obj.execute($statement.into(), $crate::parameters::Parameters::Positional(bind_parameters))
        }
    };
}

#[macro_export]
macro_rules! query {
    ($obj:expr, $statement:expr $(, $rest:expr)*) => {
        {
        let bind_parameters: Vec<$crate::parameters::Parameter> = vec![
            $(
                $rest.into(),
            )*
        ];
        $obj.query($statement.into(), $crate::parameters::Parameters::Positional(bind_parameters))
        }
    };
}
