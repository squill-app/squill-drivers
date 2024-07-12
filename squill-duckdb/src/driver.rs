use crate::statement::DuckDBStatement;
use squill_core::driver::{DriverConnection, DriverStatement, Result};
use std::cell::RefCell;
use std::rc::Rc;

use crate::{DuckDB, DRIVER_DUCKDB};

impl DriverConnection for DuckDB {
    fn driver_name(&self) -> &str {
        DRIVER_DUCKDB
    }

    fn prepare<'c: 's, 's>(&'c self, statement: &str) -> Result<Box<dyn DriverStatement + 's>> {
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
    use squill_core::{connection::Connection, execute, factory::Factory, query_arrow};
    use std::path::Path;
    use url::Url;

    #[ctor]
    fn before_all() {
        crate::register_driver();
    }

    fn path_to_duckdb_uri(dir_path: &Path, db_file: &str) -> String {
        let path = if cfg!(target_os = "windows") {
            dir_path.canonicalize().unwrap().to_string_lossy().replace('\\', "/")
        } else {
            dir_path.canonicalize().unwrap().to_string_lossy().to_string()
        };
        format!("duckdb://{}/{}", path, db_file)
    }

    #[test]
    fn test_open_memory() {
        assert!(Factory::open(IN_MEMORY_URI).is_ok());
    }

    #[test]
    fn test_open_file() {
        // create a new database file
        let temp_dir = tempfile::tempdir().unwrap();
        assert!(Factory::open(path_to_duckdb_uri(temp_dir.path(), "test.db").as_str()).is_ok());

        // opening an invalid file should return an error
        // assert!(Factory::open(path_to_duckdb_uri("./invalid-path/invalid.db".into()).as_str()).is_err());
    }

    #[test]
    fn test_open_with_config() {
        let mut uri = Url::parse(IN_MEMORY_URI).unwrap();
        uri.query_pairs_mut().append_pair("max_memory", "2GB").append_pair("threads", "4");
        assert!(Factory::open(uri.as_str()).is_ok());
    }

    #[test]
    fn test_close() {
        let conn = Connection::open(IN_MEMORY_URI).unwrap();
        assert!(conn.close().is_ok());
    }

    #[test]
    fn test_execute() {
        let conn = Connection::open(IN_MEMORY_URI).unwrap();
        assert_eq!(execute!(conn, "CREATE TABLE employees (id BIGINT, name VARCHAR(100))").unwrap(), 0);
        assert_eq!(
            execute!(
                conn,
                r#"INSERT INTO employees (id, name)
                            SELECT id::BIGINT AS id, 'Employees ' || id::BIGINT as name
                              FROM generate_series(1, 1000) AS series(id)"#
            )
            .unwrap(),
            1000
        );
    }

    #[test]
    fn test_query() {
        let conn = Connection::open(IN_MEMORY_URI).unwrap();
        execute!(conn, "CREATE TABLE employees (id BIGINT, name VARCHAR(100))").unwrap();
        execute!(
            conn,
            r#"INSERT INTO employees (id, name) 
                    SELECT id::BIGINT AS id, 'Employees ' || id::BIGINT as name
                      FROM generate_series(1, 5000) AS series(id)"#
        )
        .unwrap();
        let mut num_rows = 0;
        let mut stmt = conn.prepare("SELECT * FROM employees").unwrap();
        let query = query_arrow!(stmt).unwrap();
        for result in query {
            assert!(result.is_ok());
            num_rows += result.unwrap().num_rows();
        }
        assert_eq!(num_rows, 5000);
    }
}
