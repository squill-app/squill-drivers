use squill_core::factory::Factory;

/// The name of the driver for MySQL.
pub const DRIVER_NAME: &str = "mysql";

mod driver;
mod errors;
mod factory;

pub fn register_driver() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        Factory::register(Box::new(factory::MySqlFactory {}));
    });
}

#[cfg(test)]
mod mysql_tests {
    use ctor::ctor;
    use squill_core::decode::Decode;
    use squill_core::factory::Factory;
    use squill_core::{assert_execute_eq, assert_ok, assert_some_ok};

    #[ctor]
    fn before_all() {
        crate::register_driver();
    }

    #[test]
    fn test_factory() {
        assert_ok!(Factory::open(env!("CI_MYSQL_URI")));
        assert!(Factory::open("mysql://no_user:password@localhost:3307").is_err());
        assert!(Factory::open("mysql://").is_err());
    }

    #[test]
    fn test_basics() {
        let conn = assert_ok!(Factory::open(env!("CI_MYSQL_URI")));
        assert_eq!(conn.driver_name(), "mysql");
        assert_ok!(conn.close());
    }

    #[test]
    fn test_execute() {
        let mut conn = assert_ok!(Factory::open(env!("CI_MYSQL_URI")));
        assert_execute_eq!(conn, "CREATE TEMPORARY TABLE ci_test (id INTEGER PRIMARY KEY, name TEXT)", 0);
        assert_execute_eq!(conn, "INSERT INTO ci_test (id, name) VALUES (1, NULL)", 1);
        assert_execute_eq!(conn, "INSERT INTO ci_test (id, name) VALUES (2, NULL)", 1);
        assert_execute_eq!(conn, "DELETE FROM ci_test WHERE id IN (1, 2)", 2);
    }

    #[test]
    fn test_ping() {
        let mut conn = assert_ok!(Factory::open(env!("CI_MYSQL_URI")));
        assert_ok!(conn.ping());
    }

    #[test]
    fn test_query() {
        let mut conn = assert_ok!(Factory::open(env!("CI_MYSQL_URI")));
        let mut stmt = assert_ok!(conn.prepare("SELECT 1 AS col_one, 2 AS col_two"));
        let mut rows = assert_ok!(stmt.query(None));
        let record_batch = assert_some_ok!(rows.next());
        assert_eq!(record_batch.schema().fields().len(), 2);
        assert_eq!(record_batch.schema().field(0).name(), "col_one");
        assert_eq!(record_batch.schema().field(1).name(), "col_two");
        assert_eq!(record_batch.num_rows(), 1);
        assert_eq!(i64::decode(&record_batch.column(0), 0), 1);
        assert_eq!(i64::decode(&record_batch.column(1), 0), 2);
    }
}
