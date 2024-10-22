use squill_core::factory::Factory;
use std::{cell::RefCell, rc::Rc};

/// The name of the driver for PostgreSQL.
pub const DRIVER_NAME: &str = "postgres";

mod driver;
mod errors;
mod factory;
mod statement;

pub(crate) struct Postgres {
    pub(crate) client: Rc<RefCell<postgres::Client>>,
}

pub fn register_driver() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        Factory::register(Box::new(factory::PostgresFactory {}));
    });
}

#[cfg(test)]
mod tests {
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
    fn test_postgres_factory() {
        let ci_database_uri = env!("CI_POSTGRES_URI");
        assert_ok!(Factory::open(ci_database_uri));
    }

    #[test]
    fn test_postgres_execute() {
        let ci_database_uri = env!("CI_POSTGRES_URI");
        let conn = assert_ok!(Factory::open(ci_database_uri));
        assert_execute!(conn, "CREATE TEMPORARY TABLE ci_test (id INTEGER PRIMARY KEY, name TEXT)", 0);
        assert_execute!(conn, "INSERT INTO ci_test (id, name) VALUES (1, NULL)", 1);
        assert_execute!(conn, "INSERT INTO ci_test (id, name) VALUES (2, NULL)", 1);
        assert_execute!(conn, "DELETE FROM ci_test WHERE id IN (1, 2)", 2);
    }
}
