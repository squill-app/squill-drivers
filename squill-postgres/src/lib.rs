use squill_core::factory::Factory;

/// The name of the driver for PostgreSQL.
pub const DRIVER_NAME: &str = "postgres";

mod driver;
mod errors;
mod factory;
mod statement;
mod values;

pub(crate) struct Postgres {
    pub(crate) client: postgres::Client,
}

pub fn register_driver() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        Factory::register(Box::new(factory::PostgresFactory {}));
    });
}

#[cfg(test)]
mod postgres_tests {
    use ctor::ctor;
    use squill_core::factory::Factory;
    use tokio_test::assert_ok;

    macro_rules! assert_execute {
        ($conn:expr, $query:expr, $expected:expr) => {
            assert_eq!(assert_ok!(assert_ok!($conn.prepare($query)).execute(None)), $expected);
        };
    }

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
        let ci_database_uri = env!("CI_POSTGRES_URI");
        let mut conn = assert_ok!(Factory::open(ci_database_uri));
        assert_execute!(conn, "CREATE TEMPORARY TABLE ci_test (id INTEGER PRIMARY KEY, name TEXT)", 0);
        assert_execute!(conn, "INSERT INTO ci_test (id, name) VALUES (1, NULL)", 1);
        assert_execute!(conn, "INSERT INTO ci_test (id, name) VALUES (2, NULL)", 1);
        assert_execute!(conn, "DELETE FROM ci_test WHERE id IN (1, 2)", 2);
    }

    #[test]
    fn test_query() {
        let ci_database_uri = env!("CI_POSTGRES_URI");
        let mut conn = assert_ok!(Factory::open(ci_database_uri));
        let mut stmt = conn.prepare("SELECT 1 AS one, 2 AS two").unwrap();
        let mut rows = stmt.query(None).unwrap();
        let batch = rows.next().unwrap().unwrap();
        println!("{:?}", batch);
        drop(rows);
        drop(stmt);

        let mut stmt = conn
            .prepare("SELECT id, NULL as unknown, 'hello' as text, CASE  WHEN id < 2 THEN NULL ELSE 42 END AS age FROM generate_series(0, 5) AS id;")
            .unwrap();
        let mut rows = stmt.query(None).unwrap();
        let batch = rows.next().unwrap().unwrap();
        println!("{:?}", batch);
    }
}
