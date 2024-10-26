#![forbid(unsafe_code)]

pub mod connection;
pub mod statement;

pub use connection::Connection;
pub use statement::Statement;

#[cfg(test)]
mod blocking_tests {
    use crate::connection::Connection;
    use squill_core::error::Error;

    #[test]
    fn test_query_rows() {
        let mut conn = Connection::open("mock://").unwrap();
        let mut stmt = conn.prepare("SELECT 2").unwrap();
        let mut rows = stmt.query_rows(None).unwrap();
        assert_eq!(rows.next().unwrap().unwrap().get::<_, i32>(0), 1);
        let row = rows.next().unwrap().unwrap();
        assert!(!row.is_null(0));
        assert_eq!(row.get::<&str, i32>("id"), 2);
        assert_eq!(row.get::<&str, String>("username"), "user2");
        assert!(rows.next().is_none());
    }

    #[test]
    fn test_try_get() {
        let mut conn = Connection::open("mock://").unwrap();
        let row = conn.query_row("SELECT 1", None).unwrap().unwrap();
        assert_eq!(row.try_get::<_, i32>(0).unwrap(), 1);
        assert!(matches!(row.try_get::<_, i64>(0), Err(Error::InvalidType { expected: _, actual: _ })));
        assert!(matches!(row.try_get::<_, i32>(7), Err(Error::OutOfBounds { index: _ })));
    }
}
