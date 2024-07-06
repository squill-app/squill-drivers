use arrow_array::RecordBatch;
use squill_core::driver::DriverConnection;
use squill_core::parameters::Parameters;
use squill_core::Result;

use crate::{DuckDB, DRIVER_DUCKDB};

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
        parameters: Parameters,
    ) -> Result<Box<dyn Iterator<Item = Result<RecordBatch>> + 'c>> {
        let mut prepared_statement = self.prepare_and_bind(statement, parameters)?;
        match prepared_statement.raw_execute() {
            Ok(_affected_rows) => {
                let duckdb_query = DuckDbQuery { prepared_statement };
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
    use arrow_array::Array;
    use ctor::ctor;
    use rust_decimal::Decimal;
    use squill_core::{
        execute,
        factory::Factory,
        query,
        values::{TimeUnit, Value},
    };
    use std::path::Path;
    use url::Url;

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
        let conn = Factory::open(IN_MEMORY_URI).unwrap();
        execute!(conn, "CREATE TABLE employees (id BIGINT, name VARCHAR(100))").unwrap();
        execute!(
            conn,
            r#"INSERT INTO employees (id, name) 
                    SELECT id::BIGINT AS id, 'Employees ' || id::BIGINT as name
                      FROM generate_series(1, 5000) AS series(id)"#
        )
        .unwrap();
        let mut num_rows = 0;
        let query = query!(conn, "SELECT * FROM employees").unwrap();
        for result in query {
            assert!(result.is_ok());
            num_rows += result.unwrap().num_rows();
        }
        assert_eq!(num_rows, 5000);
    }

    #[test]
    fn test_binding_primitive_types() {
        let conn = Factory::open(IN_MEMORY_URI).unwrap();

        // Note: u128 is intentionally omitted because DuckDB does not support UInt128 yet for some reason.
        // https://github.com/duckdb/duckdb-rs/issues/273
        let batch = query!(
            conn,
            r#"SELECT 
            /*  0 */ ?::BOOLEAN,
            /*  1 */ ?::TINYINT,
            /*  2 */ ?::SMALLINT,
            /*  3 */ ?::INTEGER,
            /*  4 */ ?::BIGINT,
            /*  5 */ ?::HUGEINT,
            /*  6 */ ?::UTINYINT,
            /*  7 */ ?::USMALLINT,
            /*  8 */ ?::UINTEGER,
            /*  9 */ ?::UBIGINT,
            /* 10 */ 0, /* placeholder for ?::UHUGEINT */
            /* 11 */ ?::FLOAT,
            /* 12 */ ?::DOUBLE,
            /* 13 */ ?::VARCHAR(100),
            /* 14 */ ?::BLOB
            "#,
            /*  0 */ true,
            /*  1 */ i8::MAX,
            /*  2 */ i16::MAX,
            /*  3 */ i32::MAX,
            /*  4 */ i64::MAX,
            /*  5 */ i128::MAX,
            /*  6 */ u8::MAX,
            /*  7 */ u16::MAX,
            /*  8 */ u32::MAX,
            /*  9 */ u64::MAX,
            /* 10 */ /* u128::MAX, */
            /* 11 */ f32::MAX,
            /* 12 */ f64::MAX,
            /* 13 */ "Hello, World!",
            /* 14 */ vec![0xde, 0xad, 0xbe, 0xef]
        )
        .unwrap()
        .next()
        .unwrap()
        .unwrap();

        // 0 - BOOLEAN
        assert!(batch.column(0).as_any().downcast_ref::<arrow_array::BooleanArray>().unwrap().value(0));

        // 1 - TINYINT
        assert_eq!(batch.column(1).as_any().downcast_ref::<arrow_array::Int8Array>().unwrap().value(0), i8::MAX);

        // 2 - SMALLINT
        assert_eq!(batch.column(2).as_any().downcast_ref::<arrow_array::Int16Array>().unwrap().value(0), i16::MAX);

        // 3 - INTEGER
        assert_eq!(batch.column(3).as_any().downcast_ref::<arrow_array::Int32Array>().unwrap().value(0), i32::MAX);

        // 4- BIGINT
        assert_eq!(batch.column(4).as_any().downcast_ref::<arrow_array::Int64Array>().unwrap().value(0), i64::MAX);

        // 5 - HUGEINT
        assert_eq!(
            batch.column(5).as_any().downcast_ref::<arrow_array::Decimal128Array>().unwrap().value(0),
            i128::MAX
        );

        // 6 - UTINYINT
        assert_eq!(batch.column(6).as_any().downcast_ref::<arrow_array::UInt8Array>().unwrap().value(0), u8::MAX);

        // 7 - USMALLINT
        assert_eq!(batch.column(7).as_any().downcast_ref::<arrow_array::UInt16Array>().unwrap().value(0), u16::MAX);

        // 8 - UINTEGER
        assert_eq!(batch.column(8).as_any().downcast_ref::<arrow_array::UInt32Array>().unwrap().value(0), u32::MAX);

        // 9 - UBIGINT
        assert_eq!(batch.column(9).as_any().downcast_ref::<arrow_array::UInt64Array>().unwrap().value(0), u64::MAX);

        // 10 - UHUGEINT
        // Not tested because DuckDB does not support UInt128 yet for some reason.
        // assert_eq!(batch.column(10).as_any().downcast_ref::<arrow_array::UInt128Array>().unwrap().value(0), u128::MAX);

        // 11 - FLOAT
        assert_eq!(batch.column(11).as_any().downcast_ref::<arrow_array::Float32Array>().unwrap().value(0), f32::MAX);

        // 12 - DOUBLE
        assert_eq!(batch.column(12).as_any().downcast_ref::<arrow_array::Float64Array>().unwrap().value(0), f64::MAX);

        // 13 - VARCHAR
        assert_eq!(
            batch.column(13).as_any().downcast_ref::<arrow_array::StringArray>().unwrap().value(0),
            "Hello, World!"
        );

        // 14 - BLOB
        assert_eq!(
            batch.column(14).as_any().downcast_ref::<arrow_array::BinaryArray>().unwrap().value(0),
            vec![0xde, 0xad, 0xbe, 0xef]
        );
    }

    #[test]
    fn test_binding_datetime_types() {
        let conn = Factory::open(IN_MEMORY_URI).unwrap();
        let batch = query!(
            conn,
            r#"SELECT 
            /* 0 */ '2014/11/01'::DATE,
            /* 1 */ ?::DATE,
            /* 2 */ '2024-07-03 08:56:05.716022-07'::TIMESTAMP WITH TIME ZONE,
            /* 3 */ ?::TIMESTAMP WITH TIME ZONE,
            /* 4 */ '2024-07-03 08:56:05.716022'::TIMESTAMP,
            /* 5 */ ?::TIMESTAMP,
            /* 6 */ '11:30:00.123456'::TIME,
            /* 7 */ ?::TIME,
            /* 8 */ '1 year 5 days 12 mins 13 seconds 8 microseconds'::INTERVAL,
            /* 9 */ ?::INTERVAL
            "#,
            /* 0 - binding for DATE tested on #1 */
            /* 1 */ Value::Date32(16375),
            /* 2 - binding for TIMESTAMP WITH TIME ZONE tested on #3 */
            /* 3 */
            chrono::DateTime::parse_from_rfc3339("2024-07-03T08:56:05.716022-08:00").unwrap(),
            /* 4 - binding for TIMESTAMP tested on #5 */
            /* 5 */
            chrono::DateTime::parse_from_rfc3339("2024-07-03T08:56:05.716022Z").unwrap(),
            /* 6 - binding on TIME tested on #7 */
            /* 7 */
            Value::Time64(TimeUnit::Second, 11 * 3600 + 30 * 60 + 10),
            /* 8 - binding on INTERVAL tested on #9 */
            /* 9 */
            Value::Interval { months: 7, days: 3, nanos: 72_101_202_000 }
        )
        .unwrap()
        .next()
        .unwrap()
        .unwrap();

        let _schema = format!("{:?}", batch.schema().field(8));
        let _debug = format!("{:?}", batch.column(8));

        // 0 - DATE (no binding)
        assert_eq!(
            batch.column(0).as_any().downcast_ref::<arrow_array::Date32Array>().unwrap().value(0),
            16375 /* # of days since 1970-01-01 */
        );

        // 1 - DATE (binding)
        assert_eq!(batch.column(1).as_any().downcast_ref::<arrow_array::Date32Array>().unwrap().value(0), 16375);

        // 2 - TIMESTAMP WITH TIME ZONE (no binding)
        assert_eq!(
            batch.column(2).as_any().downcast_ref::<arrow_array::TimestampMicrosecondArray>().unwrap().value(0),
            chrono::DateTime::parse_from_rfc3339("2024-07-03T15:56:05.716022Z").unwrap().timestamp_micros()
        );

        // 3 - TIMESTAMP WITH TIME ZONE (binding)
        assert_eq!(
            batch.column(3).as_any().downcast_ref::<arrow_array::TimestampMicrosecondArray>().unwrap().value(0),
            chrono::DateTime::parse_from_rfc3339("2024-07-03T08:56:05.716022-08:00").unwrap().timestamp_micros()
        );

        // 4 - TIMESTAMP (no binding)
        assert_eq!(
            batch.column(4).as_any().downcast_ref::<arrow_array::TimestampMicrosecondArray>().unwrap().value(0),
            chrono::DateTime::parse_from_rfc3339("2024-07-03T08:56:05.716022Z").unwrap().timestamp_micros()
        );

        // 5 - TIMESTAMP (binding)
        assert_eq!(
            batch.column(5).as_any().downcast_ref::<arrow_array::TimestampMicrosecondArray>().unwrap().value(0),
            chrono::DateTime::parse_from_rfc3339("2024-07-03T08:56:05.716022Z").unwrap().timestamp_micros()
        );

        // 6 - TIME (no binding)
        assert_eq!(
            batch.column(6).as_any().downcast_ref::<arrow_array::Time64MicrosecondArray>().unwrap().value(0),
            11 * 3600 * 1_000_000 + 30 * 60 * 1_000_000 + 123456
        );

        // 7 - TIME (binding)
        assert_eq!(
            batch.column(7).as_any().downcast_ref::<arrow_array::Time64MicrosecondArray>().unwrap().value(0),
            (11 * 3600 + 30 * 60 + 10) * 1_000_000
        );

        /*
         * The following test is commended until next DuckDB release.
         * https://github.com/duckdb/duckdb-rs/issues/350

        // 8 - INTERVAL (no binding)
        let (months, days, nanos) = IntervalMonthDayNanoType::to_parts(
            batch.column(8).as_any().downcast_ref::<arrow_array::IntervalMonthDayNanoArray>().unwrap().value(0),
        );
        assert_eq!(months, 12);
        assert_eq!(days, 5);
        assert_eq!(nanos, 12 * 60 * 60 * 1_000_000_000 + 13 * 1_000_000 + 8);

        // 9 - INTERVAL (binding)
        let (months, days, nanos) = IntervalMonthDayNanoType::to_parts(
            batch.column(9).as_any().downcast_ref::<arrow_array::IntervalMonthDayNanoArray>().unwrap().value(0),
        );
        assert_eq!(months, 7);
        assert_eq!(days, 3);
        assert_eq!(nanos, 72_101_202_303);

        **/
    }

    #[test]
    fn test_binding_other_types() {
        let conn = Factory::open(IN_MEMORY_URI).unwrap();
        let batch = query!(
            conn,
            r#"SELECT 
            /* 0 */ '11011110101011011011111011101111'::BIT /* See https://github.com/duckdb/duckdb-rs/issues/349 */,
            /* 1 */ 12.99::DECIMAL(10, 2),
            /* 2 */ ?::DECIMAL(10, 2),
            /* 3 */ '5843cded-a32a-428d-8194-97ee1b949eb9'::UUID,
            /* 4 */ ?::UUID
            "#,
            /* 0 BIT (no binding) */
            /* 1 DECIMAL (no binding) */
            /* 2 */
            Value::Decimal(Decimal::new(2099, 2)),
            /* 3 UUID (no binding) */
            /* 4 */
            uuid::Uuid::parse_str("0e089c07-8654-4aab-9c25-4f3c44590251").unwrap()
        )
        .unwrap()
        .next()
        .unwrap()
        .unwrap();

        let _schema = format!("{:?}", batch.schema().field(3));
        let _debug = format!("{:?}", batch.column(3));

        // 0 - BIT
        // Not tested because because of a bug in DuckDB.
        // See https://github.com/duckdb/duckdb-rs/issues/349
        // assert_eq!(batch.column(15).as_any().downcast_ref::<arrow_array::BinaryArray>().unwrap().value(0), vec![0xde, 0xad, 0xbe, 0xef]);

        // 1 - DECIMAL (no binding)
        assert_eq!(batch.column(1).as_any().downcast_ref::<arrow_array::Decimal128Array>().unwrap().value(0), 1299);

        // 2 - DECIMAL (binding)
        assert_eq!(batch.column(2).as_any().downcast_ref::<arrow_array::Decimal128Array>().unwrap().value(0), 2099);

        // 3 - UUID (no binding)
        assert_eq!(
            batch.column(3).as_any().downcast_ref::<arrow_array::StringArray>().unwrap().value(0),
            "5843cded-a32a-428d-8194-97ee1b949eb9"
        );

        // 3 - UUID (binding)
        assert_eq!(
            batch.column(4).as_any().downcast_ref::<arrow_array::StringArray>().unwrap().value(0),
            "0e089c07-8654-4aab-9c25-4f3c44590251"
        );
    }

    #[test]
    fn test_binding_null() {
        let conn = Factory::open(IN_MEMORY_URI).unwrap();
        let batch = query!(
            conn,
            r#"SELECT 
            /* 0 */ NULL,
            /* 1 */ ?,
            /* 2 */ ?
            "#,
            /* 0 NULL (no binding) */
            /* 1 */ Value::Null,
            /* 2 */ None::<i32>
        )
        .unwrap()
        .next()
        .unwrap()
        .unwrap();

        let _schema = format!("{:?}", batch.schema().field(0));
        let _debug = format!("{:?}", batch.column(0));
        assert!(batch.column(0).as_any().downcast_ref::<arrow_array::Int32Array>().unwrap().is_null(0));
        assert!(batch.column(1).as_any().downcast_ref::<arrow_array::Int32Array>().unwrap().is_null(0));
        assert!(batch.column(2).as_any().downcast_ref::<arrow_array::Int32Array>().unwrap().is_null(0));
    }
}
