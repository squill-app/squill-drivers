use crate::statement::DuckDBStatement;
use squill_core::driver::{DriverConnection, DriverStatement, Result};
use std::cell::RefCell;
use std::rc::Rc;

use crate::{DuckDB, DRIVER_NAME};

impl DriverConnection for DuckDB {
    fn driver_name(&self) -> &str {
        DRIVER_NAME
    }

    fn prepare<'c: 's, 's>(&'c mut self, statement: &str) -> Result<Box<dyn DriverStatement + 's>> {
        Ok(Box::new(DuckDBStatement { inner: Rc::new(RefCell::new(self.conn.prepare(statement)?)) }))
    }

    fn close(self: std::boxed::Box<DuckDB>) -> Result<()> {
        let result = self.conn.close();
        match result {
            Ok(_) => Ok(()),
            Err((_connection, error)) => Err(error.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::IN_MEMORY_URI;
    use ctor::ctor;
    use squill_core::{assert_execute_eq, assert_ok, factory::Factory};
    use url::Url;

    #[ctor]
    fn before_all() {
        crate::register_driver();
    }

    #[test]
    fn test_open_memory() {
        assert_ok!(Factory::open(IN_MEMORY_URI));
    }

    #[test]
    fn test_open_file() {
        // create a new database file
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.db");
        assert_ok!(Factory::open(&format!("duckdb://{}", Factory::to_uri_path(&file_path))));

        // opening an invalid file should return an error
        // assert!(Factory::open(path_to_duckdb_uri("./invalid-path/invalid.db".into()).as_str()).is_err());
    }

    #[test]
    fn test_open_with_config() {
        let mut uri = Url::parse(IN_MEMORY_URI).unwrap();
        uri.query_pairs_mut().append_pair("max_memory", "2GB").append_pair("threads", "4");
        assert_ok!(Factory::open(uri.as_str()));
    }

    #[test]
    fn test_close() {
        let conn = assert_ok!(Factory::open(IN_MEMORY_URI));
        assert!(conn.close().is_ok());
    }

    #[test]
    fn test_execute() {
        let mut conn = assert_ok!(Factory::open(IN_MEMORY_URI));
        assert_execute_eq!(conn, "CREATE TABLE employees (id BIGINT, name VARCHAR(100))", 0);
        assert_execute_eq!(
            conn,
            r#"INSERT INTO employees (id, name)
               SELECT id::BIGINT AS id, 'Employees ' || id::BIGINT as name
                 FROM generate_series(1, 1000) AS series(id)"#,
            1000
        );
    }

    #[test]
    fn test_query() {
        let mut conn = assert_ok!(Factory::open(IN_MEMORY_URI));
        assert_execute_eq!(conn, "CREATE TABLE employees (id BIGINT, name VARCHAR(100))", 0);
        assert_execute_eq!(
            conn,
            r#"INSERT INTO employees (id, name) 
                    SELECT id::BIGINT AS id, 'Employees ' || id::BIGINT as name
                      FROM generate_series(1, 5000) AS series(id)"#,
            5000
        );
        let mut num_rows = 0;
        let mut stmt = assert_ok!(conn.prepare("SELECT * FROM employees"));
        let iter = assert_ok!(stmt.query(None));
        for item in iter {
            let record_batch = assert_ok!(item);
            num_rows += record_batch.num_rows();
        }
        assert_eq!(num_rows, 5000);
    }
}
