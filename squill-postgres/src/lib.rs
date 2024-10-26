use squill_core::factory::Factory;

/// The name of the driver for PostgreSQL.
pub const DRIVER_NAME: &str = "postgres";

mod driver;
mod errors;
mod factory;
mod values;

pub fn register_driver() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        Factory::register(Box::new(factory::PostgresFactory {}));
    });
}

#[cfg(test)]
mod postgres_tests {
    use ctor::ctor;
    use squill_core::decode::Decode;
    use squill_core::{assert_execute_eq, assert_some_ok, factory::Factory};
    use tokio_test::assert_ok;

    #[ctor]
    fn before_all() {
        crate::register_driver();
    }

    #[test]
    fn test_factory() {
        let ci_database_uri = env!("CI_POSTGRES_URI");
        assert_ok!(Factory::open(ci_database_uri));
    }

    #[test]
    fn test_execute() {
        let mut conn = assert_ok!(Factory::open(env!("CI_POSTGRES_URI")));
        assert_execute_eq!(conn, "CREATE TEMPORARY TABLE ci_test (id INTEGER PRIMARY KEY, name TEXT)", 0);
        assert_execute_eq!(conn, "INSERT INTO ci_test (id, name) VALUES (1, NULL)", 1);
        assert_execute_eq!(conn, "INSERT INTO ci_test (id, name) VALUES (2, NULL)", 1);
        assert_execute_eq!(conn, "DELETE FROM ci_test WHERE id IN (1, 2)", 2);
    }

    #[test]
    fn test_query() {
        let mut conn = assert_ok!(Factory::open(env!("CI_POSTGRES_URI")));
        let mut stmt = assert_ok!(conn.prepare("SELECT 1 AS col_one, 2 AS col_two"));
        let mut rows = assert_ok!(stmt.query(None));
        let record_batch = assert_some_ok!(rows.next());
        assert_eq!(record_batch.schema().fields().len(), 2);
        assert_eq!(record_batch.schema().field(0).name(), "col_one");
        assert_eq!(record_batch.schema().field(1).name(), "col_two");
        assert_eq!(record_batch.num_rows(), 1);
        assert_eq!(i32::decode(&record_batch.column(0), 0), 1);
        assert_eq!(i32::decode(&record_batch.column(1), 0), 2);
    }
}
