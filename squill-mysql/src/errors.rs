use squill_core::error::Error;

/// Convert a `mysql::Error` into a `squill_core::error::Error`.
pub(crate) fn driver_error(_mysql_error: mysql::Error) -> Error {
    let _s = _mysql_error.to_string();
    println!("mysql error: {}", _s);
    todo!()
}
