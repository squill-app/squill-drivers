use squill_core::{driver::DriverOptionsRef, factory::Factory};

mod driver;
mod errors;
mod factory;
mod statement;
mod value;

/// The name of the driver for SQLite.
pub const DRIVER_NAME: &str = "sqlite";

/// Special filename for an in-memory databases.
/// https://www.sqlite.org/inmemorydb.html
pub const IN_MEMORY_SPECIAL_FILENAME: &str = ":memory:";

/// URI for an in-memory databases.
///
/// # Example
/// ```rust
/// use squill_core::factory::Factory;
/// # use squill_sqlite::IN_MEMORY_URI;
/// let conn = Factory::open(IN_MEMORY_URI);
/// ```
pub const IN_MEMORY_URI: &str = "sqlite::memory:";

pub(crate) struct Sqlite {
    conn: rusqlite::Connection,
    options: DriverOptionsRef,
}

pub fn register_driver() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        Factory::register(Box::new(factory::SqliteFactory {}));
    });
}

#[cfg(test)]
mod sqlite_tests {
    use crate::IN_MEMORY_URI;
    use arrow_array::RecordBatch;
    use ctor::ctor;
    use squill_core::decode::{self, Decode};
    use squill_core::factory::Factory;
    use squill_core::{assert_execute_eq, assert_ok, assert_query_decode_eq, assert_some};

    #[ctor]
    fn before_all() {
        crate::register_driver();
    }

    #[test]
    fn test_factory() {
        // memory database
        assert_ok!(Factory::open("sqlite::memory:"));
        assert_ok!(Factory::open("sqlite:memdb1?mode=memory&cache=shared"));

        // file database
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.db");

        // trying to open a file that does not exist in read-only should fail
        assert!(Factory::open(&format!("sqlite://{}?mode=ro", Factory::to_uri_path(&file_path))).is_err());
        // trying to open a file that does not exist in read-write should create it
        assert_ok!(Factory::open(&format!("sqlite://{}?mode=rwc", Factory::to_uri_path(&file_path))));
        // now that the file exists, opening it in read-only should work
        assert_ok!(Factory::open(&format!("sqlite://{}?mode=ro", Factory::to_uri_path(&file_path))));
    }

    #[test]
    fn test_basics() {
        let conn = assert_ok!(Factory::open(IN_MEMORY_URI));
        assert_eq!(conn.driver_name(), "sqlite");
        assert_ok!(conn.close());
    }

    #[test]
    fn test_iterator() {
        let mut conn = assert_ok!(Factory::open(IN_MEMORY_URI));
        assert_execute_eq!(conn, "CREATE TABLE test (int INTEGER, text TEXT, real REAL, blob BLOB)", 0);
        assert_execute_eq!(conn, "INSERT INTO test (int, text, real, blob) VALUES (1, 'text', 1.1, x'01')", 1);
        assert_execute_eq!(conn, "INSERT INTO test (int, text, real, blob) VALUES (2, NULL, NULL, NULL)", 1);
        let mut stmt = assert_ok!(conn.prepare("SELECT int, text, real, blob FROM test ORDER BY int"));
        let mut iter = assert_ok!(stmt.query(None));
        let batch = assert_ok!(assert_some!(iter.next()));

        assert_eq!(batch.schema().fields().len(), 4);
        assert_eq!(batch.num_rows(), 2);

        // first row
        assert_eq!(i64::decode(&batch.column(0), 0), 1);
        assert_eq!(String::decode(&batch.column(1), 0), "text");
        assert_eq!(f64::decode(&batch.column(2), 0), 1.1);
        assert_eq!(Vec::<u8>::decode(&batch.column(3), 0), vec![1]);

        // second row
        assert_eq!(i64::decode(&batch.column(0), 1), 2);
        assert!(batch.column(1).is_null(1));
        assert!(batch.column(2).is_null(1));
        assert!(batch.column(3).is_null(1));

        assert!(iter.next().is_none());
    }

    // In Sqlite, when using expressions in the SELECT clause, the column type is always at the time of the prepare
    // statement and the current implementation of the driver will eventually infer the type of the column when it get
    // non null values from the column. This test is to ensure that the driver can handle such cases.
    #[test]
    fn test_expressions() {
        let mut conn = assert_ok!(Factory::open(IN_MEMORY_URI));
        let mut stmt = assert_ok!(conn.prepare("SELECT 1, 'Welcome', 'いらっしゃいませ', 1.1, x'01', NULL"));
        let mut iter = assert_ok!(stmt.query(None));
        let batch = assert_ok!(assert_some!(iter.next()));
        assert_eq!(batch.schema().fields().len(), 6);
        assert_eq!(*batch.schema().fields()[0].data_type(), arrow_schema::DataType::Int64);
        assert_eq!(*batch.schema().fields()[1].data_type(), arrow_schema::DataType::Utf8);
        assert_eq!(*batch.schema().fields()[2].data_type(), arrow_schema::DataType::Utf8);
        assert_eq!(*batch.schema().fields()[3].data_type(), arrow_schema::DataType::Float64);
        assert_eq!(*batch.schema().fields()[4].data_type(), arrow_schema::DataType::Binary);
        assert_eq!(*batch.schema().fields()[5].data_type(), arrow_schema::DataType::Null);
        assert_eq!(i64::decode(&batch.column(0), 0), 1);
        assert_eq!(String::decode(&batch.column(1), 0), "Welcome");
        assert_eq!(String::decode(&batch.column(2), 0), "いらっしゃいませ");
        assert_eq!(f64::decode(&batch.column(3), 0), 1.1);
        assert_eq!(Vec::<u8>::decode(&batch.column(4), 0), vec![1]);
        assert!(decode::is_null(batch.column(5), 0));
    }

    #[test]
    fn test_timestamp() {
        let mut conn = assert_ok!(Factory::open(IN_MEMORY_URI));
        let mut stmt = assert_ok!(conn.prepare("SELECT datetime('2023-01-01 10:00:00')"));
        let mut iter = assert_ok!(stmt.query(None));
        let batch = assert_ok!(assert_some!(iter.next()));
        assert_eq!(batch.schema().fields().len(), 1);
        assert_eq!(*batch.schema().fields()[0].data_type(), arrow_schema::DataType::Utf8);
        assert_eq!(String::decode(&batch.column(0), 0), "2023-01-01 10:00:00");
    }

    #[test]
    fn test_boolean() {
        let mut conn = assert_ok!(Factory::open(IN_MEMORY_URI));
        let mut stmt = assert_ok!(conn.prepare("SELECT TRUE, FALSE"));
        let mut iter = assert_ok!(stmt.query(None));
        let batch = assert_ok!(assert_some!(iter.next()));
        assert_eq!(batch.schema().fields().len(), 2);
        assert_eq!(*batch.schema().fields()[0].data_type(), arrow_schema::DataType::Int64);
        assert!(bool::decode(&batch.column(0), 0));
        assert!(!bool::decode(&batch.column(1), 0));
    }

    #[test]
    fn test_schema() {
        let mut conn = assert_ok!(Factory::open(IN_MEMORY_URI));
        assert_execute_eq!(conn, "CREATE TABLE test_schema (int INTEGER, text TEXT, real REAL, blob BLOB)", 0);
        let mut stmt =
            assert_ok!(conn.prepare("SELECT int, text, real, blob, 'hello' AS expr FROM test_schema WHERE 1=2"));
        let mut iter = assert_ok!(stmt.query(None));
        assert!(iter.next().is_none());
        drop(iter);
        let schema = stmt.schema();
        assert_eq!(schema.fields().len(), 5);
        assert_eq!(*schema.fields()[0].data_type(), arrow_schema::DataType::Int64);
        assert_eq!(*schema.fields()[1].data_type(), arrow_schema::DataType::Utf8);
        assert_eq!(*schema.fields()[2].data_type(), arrow_schema::DataType::Float64);
        assert_eq!(*schema.fields()[3].data_type(), arrow_schema::DataType::Binary);
        assert_eq!(*schema.fields()[4].data_type(), arrow_schema::DataType::Null);
        assert_eq!(schema.fields()[0].name(), "int");
        assert_eq!(schema.fields()[1].name(), "text");
        assert_eq!(schema.fields()[2].name(), "real");
        assert_eq!(schema.fields()[3].name(), "blob");
        assert_eq!(schema.fields()[4].name(), "expr");
    }

    #[test]
    fn test_bind() {
        // TODO: test blob
        // let blob: Vec<u8> = vec![0x00, 0x01, 0x42];
        let mut conn = assert_ok!(Factory::open(IN_MEMORY_URI));
        assert_execute_eq!(conn, "CREATE TABLE test_integer (value INTEGER)", 0);
        assert_execute_eq!(conn, "CREATE TABLE test_text (value VARCHAR)", 0);
        assert_execute_eq!(conn, "CREATE TABLE test_real (value REAL)", 0);
        assert_execute_eq!(conn, "CREATE TABLE test_blob (value BLOB)", 0);
        assert_execute_eq!(conn, "INSERT INTO test_integer (value) VALUES (?)", &[&42i64], 1);
        assert_execute_eq!(conn, "INSERT INTO test_text (value) VALUES (?)", &[&"hello"], 1);
        assert_execute_eq!(conn, "INSERT INTO test_real (value) VALUES (?)", &[&42.2f64], 1);
        //        assert_execute_eq!(conn, "INSERT INTO test_blob (value) VALUES (?)", &[&blob], 1);
        assert_query_decode_eq!(conn, "SELECT value FROM test_integer", i64, 42);
        assert_query_decode_eq!(conn, "SELECT value FROM test_text", String, "hello");
        assert_query_decode_eq!(conn, "SELECT value FROM test_real", f64, 42.2);
        //        assert_query_decode_eq!(conn, "SELECT value FROM test_real", Vec<u8>, blob);
    }
}
