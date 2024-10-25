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
macro_rules! query {
    ($statement:expr $(, $rest:expr)*) => {{
        let bind_parameters: Vec<$crate::values::Value> = vec![
            $(
                $rest.into(),
            )*
        ];
        $statement.query(Some($crate::parameters::Parameters::Positional(bind_parameters)))
    }};
}

/// Re-export the `assert_ok!` macro from the tokio-test crate.
///
/// This macro is used to assert that a `Result` is `Ok` and return the value inside the `Ok`.
#[macro_export]
macro_rules! assert_ok {
    ($($arg:tt)*) => {
        tokio_test::assert_ok!($($arg)*)
    };
}

#[macro_export]
macro_rules! assert_query_eq {
    ($conn:expr, $stmt:expr, $expr:expr, $expected:expr) => {{
        let mut stmt = tokio_test::assert_ok!($conn.prepare($stmt));
        let mut iter = tokio_test::assert_ok!(stmt.query(None));
        let batch = $crate::assert_some_ok!(iter.next());
        assert_eq!($expr(&batch), $expected);
        drop(iter);
        drop(stmt);
    }};
    (async $conn:expr, $stmt:expr, $expr:expr, $expected:expr) => {{
        let mut stmt = tokio_test::assert_ok!($conn.prepare($stmt).await);
        let mut iter = tokio_test::assert_ok!(stmt.query(None).await);
        let batch = $crate::assert_some_ok!(iter.next().await);
        assert_eq!($expr(&batch), $expected);
        drop(iter);
        drop(stmt);
    }};
}

#[macro_export]
macro_rules! assert_execute_eq {
    ($conn:expr, $query:expr, $expected:expr) => {
        assert_eq!(tokio_test::assert_ok!(tokio_test::assert_ok!($conn.prepare($query)).execute(None)), $expected);
    };
    ($conn:expr, $query:expr, $params:expr, $expected:expr) => {
        assert_eq!(
            tokio_test::assert_ok!(tokio_test::assert_ok!($conn.prepare($query))
                .execute(Some($crate::parameters::Parameters::from_slice($params)))),
            $expected
        );
    };
    (async $conn:expr, $query:expr, $expected:expr) => {
        assert_eq!(
            tokio_test::assert_ok!(tokio_test::assert_ok!($conn.prepare($query).await).execute(None).await),
            $expected
        );
    };
    (async $conn:expr, $query:expr, $params:expr, $expected:expr) => {
        assert_eq!(
            tokio_test::assert_ok!(
                tokio_test::assert_ok!($conn.prepare($query).await)
                    .execute(Some(Parameters::from_slice($params)))
                    .await
            ),
            $expected
        );
    };
}

#[macro_export]
macro_rules! assert_query_decode_eq {
    ($conn:expr, $stmt:expr, $datatype:ty, $expected:expr) => {
        $crate::assert_query_eq!(
            $conn,
            $stmt,
            |batch: &RecordBatch| <$datatype>::decode(batch.column(0), 0),
            $expected
        );
    };
}

/// Assert that the expression returning a Option<T> is Some and return the value.
#[macro_export]
macro_rules! assert_some {
    ($option:expr) => {
        match $option {
            Some(value) => value,
            None => panic!("Expected Some, found None"),
        }
    };
}

/// Assert that the expression returning a Option<Result<T, E>> is Some(Ok(value)) and return the value.
#[macro_export]
macro_rules! assert_some_ok {
    ($expr:expr) => {
        tokio_test::assert_ok!($crate::assert_some!($expr))
    };
}
