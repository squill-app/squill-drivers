use arrow_array::RecordBatch;
use squill_core::parameters::Parameters;
use squill_core::drivers::DriverConnection;
use squill_core::Result;

use crate::{ DuckDB, DRIVER_DUCKDB };

impl DriverConnection for DuckDB {
    fn driver_name(&self) -> &str {
        DRIVER_DUCKDB
    }

    fn execute(&self, statement: String, parameters: Parameters) -> Result<u64> {
        let mut prepared_statement = self.prepare_and_bind(statement, parameters)?;
        match prepared_statement.raw_execute() {
            Ok(affected_rows) => Ok(affected_rows as u64),
            Err(e) => Err(Box::new(e)),
        }
    }

    fn query<'c>(
        &'c self,
        statement: String,
        parameters: Parameters
    ) -> Result<Box<dyn Iterator<Item = Result<RecordBatch>> + 'c>> {
        let mut prepared_statement = self.prepare_and_bind(statement, parameters)?;
        match prepared_statement.raw_execute() {
            Ok(_affected_rows) => {
                let duckdb_query = DuckDbQuery {
                    prepared_statement,
                };
                Ok(Box::new(duckdb_query))
            }
            Err(e) => Err(Box::new(e)),
        }
    }

    fn close(self: std::boxed::Box<DuckDB>) -> Result<()> {
        let result = self.conn.close();
        match result {
            Ok(_) => Ok(()),
            Err((_connection, e)) => Err(Box::new(e)),
        }
    }
}

struct DuckDbQuery<'conn> {
    prepared_statement: duckdb::Statement<'conn>,
}

impl<'conn> Iterator for DuckDbQuery<'conn> {
    type Item = Result<arrow_array::RecordBatch>;

    fn next(&mut self) -> Option<Result<arrow_array::RecordBatch>> {
        let step = self.prepared_statement.step();
        if step.is_some() {
            let batch_record = RecordBatch::from(step.unwrap());
            Some(Ok(batch_record))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use ctor::ctor;
    use url::Url;
    use std::path::Path;
    use squill_core::{ execute, query, factory::Factory };

    use crate::IN_MEMORY_URI;

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
        let conn = Factory::open(IN_MEMORY_URI).unwrap();
        assert!(conn.close().is_ok());
    }

    #[test]
    fn test_execute() {
        let conn = Factory::open(IN_MEMORY_URI).unwrap();
        assert_eq!(
            execute!(conn, "CREATE TABLE employees (id BIGINT, name VARCHAR(100))").unwrap(),
            0
        );
        assert_eq!(
            execute!(
                conn,
                r#"INSERT INTO employees (id, name)
                            SELECT id::BIGINT AS id, 'Employees ' || id::BIGINT as name
                              FROM generate_series(1, 1000) AS series(id)"#
            ).unwrap(),
            1000
        );
    }

    #[test]
    fn test_query() {
        let conn = Factory::open(IN_MEMORY_URI).unwrap();
        execute!(conn, "CREATE TABLE employees (id BIGINT, name VARCHAR(100))").unwrap();
        execute!(
            conn,
            r#"INSERT INTO employees (id, name) 
                    SELECT id::BIGINT AS id, 'Employees ' || id::BIGINT as name
                      FROM generate_series(1, 5000) AS series(id)"#
        ).unwrap();
        let mut num_rows = 0;
        let query = query!(conn, "SELECT * FROM employees").unwrap();
        for result in query {
            assert!(result.is_ok());
            num_rows += result.unwrap().num_rows();
        }
        assert_eq!(num_rows, 5000);
    }

    #[test]
    fn test_bindings() {
        let conn = Factory::open(IN_MEMORY_URI).unwrap();
        execute!(
            conn,
            "CREATE TABLE datatypes (
                i64 BIGINT,
                i32 INTEGER,
                i16 SMALLINT,
                i8 TINYINT,
                f64 DOUBLE,
                f32 FLOAT,
                str VARCHAR(100),
                bool BOOLEAN,
                u64 UBIGINT,
                u32 UINTEGER,
                u16 USMALLINT,
                u8 UTINYINT
        )"
        ).unwrap();
        assert_eq!(
            execute!(
                conn,
                "INSERT INTO datatypes (i64, i32, i16) VALUES (?, ?, ?)",
                i64::MAX,
                i32::MAX,
                i16::MAX
            ).unwrap(),
            1
        );
        let batch = query!(conn, "SELECT * FROM datatypes").unwrap().next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 12);
        assert_eq!(
            batch.column(0).as_any().downcast_ref::<arrow_array::Int64Array>().unwrap().value(0),
            i64::MAX
        );
    }
}
