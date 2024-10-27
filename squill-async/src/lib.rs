pub mod connection;
pub mod statement;
pub mod streams;

pub use connection::Connection;
pub use statement::Statement;
pub use streams::RecordBatchStream;
pub use streams::RowStream;

#[cfg(test)]
mod async_tests {
    use crate::Connection;
    use futures::StreamExt;
    use squill_core::{assert_ok, assert_ok_some, assert_some_ok};

    #[tokio::test]
    async fn test_statement_query_map_row() {
        let mut conn = assert_ok!(Connection::open("mock://").await);
        struct TestUser {
            id: i32,
            username: String,
        }

        // some rows
        let mut stmt = assert_ok!(conn.prepare("SELECT 1").await);
        let user = assert_ok_some!(
            stmt.query_map_row(None, |row| Ok(TestUser { id: row.get::<_, _>(0), username: row.get::<_, _>(1) })).await
        );
        assert_eq!(user.id, 1);
        assert_eq!(user.username, "user1");
        drop(stmt);

        // no rows
        let mut stmt = assert_ok!(conn.prepare("SELECT 0").await);
        assert!(assert_ok!(
            stmt.query_map_row(None, |row| { Ok(TestUser { id: row.get::<_, _>(0), username: row.get::<_, _>(1) }) })
                .await
        )
        .is_none());
        drop(stmt);

        // error by the driver
        let mut stmt = assert_ok!(conn.prepare("SELECT -1").await);
        assert!(stmt
            .query_map_row(None, |row| { Ok(TestUser { id: row.get::<_, _>(0), username: row.get::<_, _>(1) }) })
            .await
            .is_err());
        drop(stmt);

        // error by the mapping function
        let mut stmt = assert_ok!(conn.prepare("SELECT 1").await);
        assert!(stmt
            .query_map_row(None, |row| {
                if row.get::<_, i32>(0) == 1 {
                    Err("Error".into())
                } else {
                    Ok(TestUser { id: row.get::<_, _>(0), username: row.get::<_, _>(1) })
                }
            })
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_statement_schema() {
        let mut conn = assert_ok!(Connection::open("mock://").await);
        let mut stmt = assert_ok!(conn.prepare("SELECT 1").await);
        assert_some_ok!(assert_ok!(stmt.query(None).await).next().await);
        let schema = assert_ok!(stmt.schema().await);
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "username");
    }
}
