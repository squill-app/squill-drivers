use crate::decode;
use crate::{decode::Decode, Error, Result};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use std::sync::Arc;

pub struct Rows<'i> {
    last_record_batch: Option<Arc<RecordBatch>>,
    iterator: Box<dyn Iterator<Item = Result<RecordBatch>> + 'i>,
    index_in_batch: usize,
}

impl<'i> From<Box<dyn Iterator<Item = Result<RecordBatch>> + 'i>> for Rows<'i> {
    fn from(iterator: Box<dyn Iterator<Item = Result<RecordBatch>> + 'i>) -> Self {
        Rows { last_record_batch: None, iterator, index_in_batch: 0 }
    }
}

/// A row returned by a query.
pub struct Row {
    record_batch: Arc<RecordBatch>,
    index_in_batch: usize,
}

impl Row {
    /// Get the description of the row.
    pub fn schema(&self) -> SchemaRef {
        self.record_batch.schema()
    }

    /// Get the number of columns in the row.
    pub fn num_columns(&self) -> usize {
        self.record_batch.num_columns()
    }

    /// Check if the value of a column from its index is null.
    ///
    /// # Panics
    /// Panics if the column index is out of bounds.
    pub fn is_null<T: ColumnIndex>(&self, index: T) -> bool {
        match index.index(self.record_batch.schema()) {
            Ok(index) => decode::is_null(self.record_batch.column(index), self.index_in_batch),
            Err(e) => panic!("{}", e),
        }
    }

    /// Get a value from a column by its index.
    ///
    /// The index of the column can be either a 0-based index or the name of the column.
    ///
    /// # Panics
    /// Panics if the column index is out of bounds or if the type is not the expected one.
    pub fn get<I: ColumnIndex, T: Decode>(&self, index: I) -> T {
        match index.index(self.record_batch.schema()) {
            Ok(index) => T::decode(self.record_batch.column(index), self.index_in_batch),
            Err(e) => panic!("{}", e),
        }
    }
}

/// An iterator over the rows returned by a query.
impl<'i> Iterator for Rows<'i> {
    type Item = Result<Row>;

    fn next(&mut self) -> Option<Result<Row>> {
        if self.last_record_batch.is_none()
            || self.index_in_batch >= self.last_record_batch.as_ref().unwrap().num_rows()
        {
            self.last_record_batch = match self.iterator.next() {
                Some(Ok(record_batch)) => Some(Arc::new(record_batch)),
                Some(Err(e)) => {
                    return Some(Err(e));
                }
                None => {
                    return None;
                }
            };
            self.index_in_batch = 0;
        }
        let row = Row { record_batch: self.last_record_batch.clone().unwrap(), index_in_batch: self.index_in_batch };
        self.index_in_batch += 1;
        Some(Ok(row))
    }
}

/// A trait implemented by types that can index into columns of a row.
pub trait ColumnIndex {
    fn index(&self, schema: SchemaRef) -> Result<usize>;
}

/// A trait to get a value from a column.
impl ColumnIndex for usize {
    fn index(&self, schema: SchemaRef) -> Result<usize> {
        if *self >= schema.fields.len() {
            Err(Error::OutOfBounds { index: *self })?;
        }
        Ok(*self)
    }
}

impl ColumnIndex for &str {
    fn index(&self, schema: SchemaRef) -> Result<usize> {
        match schema.index_of(self) {
            Ok(index) => Ok(index),
            Err(_e) => Err(Error::NotFound),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::connection::Connection;

    #[test]
    fn test_query_rows() {
        let conn = Connection::open("mock://").unwrap();
        let mut stmt = conn.prepare("SELECT 2").unwrap();
        let mut rows = conn.query_rows(&mut stmt, None).unwrap();
        assert_eq!(rows.next().unwrap().unwrap().get::<_, i32>(0), 1);
        let row = rows.next().unwrap().unwrap();
        assert!(!row.is_null(0));
        assert_eq!(row.get::<&str, i32>("col0"), 2);
        assert!(rows.next().is_none());
    }
}
