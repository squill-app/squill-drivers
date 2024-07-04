use crate::drivers::DriverConnection;
use crate::factory::Factory;
use crate::parameters::Parameters;
use crate::Result;
use arrow_array::RecordBatch;

pub struct Connection {
    inner: Box<dyn DriverConnection>,
}

impl Connection {
    pub fn open(uri: &str) -> Result<Self> {
        let inner = Factory::open(uri)?;
        Ok(Self { inner })
    }

    pub fn driver_name(&self) -> &str {
        self.inner.driver_name()
    }

    pub fn execute(&self, statement: String, parameters: Parameters) -> Result<u64> {
        self.inner.execute(statement, parameters)
    }

    pub fn query<'c>(
        &'c self,
        statement: String,
        parameters: Parameters,
    ) -> Result<Box<dyn Iterator<Item = Result<RecordBatch>> + 'c>> {
        self.inner.query(statement, parameters)
    }

    pub fn close(self) -> crate::Result<()> {
        self.inner.close()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{execute, query};

    #[test]
    fn test_connection() {
        let conn = Connection::open("mock://").unwrap();
        assert_eq!(conn.driver_name(), "mock");
        assert_eq!(execute!(conn, "CREATE TABLE employee (id BIGINT)").unwrap(), 0);
        assert!(query!(conn, "SELECT * employee").unwrap().next().is_none());
        assert!(conn.close().is_ok());
    }
}
