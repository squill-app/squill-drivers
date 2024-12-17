use crate::decode;
use crate::{decode::Decode, Error, Result};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use std::sync::Arc;

/// A row returned by a query.
pub struct Row {
    record_batch: Arc<RecordBatch>,
    index_in_batch: usize,
}

/// A row in a [`RecordBatch`] returned by a query.
impl Row {
    /// Create a new row.
    ///
    /// Users are not expected to call this function directly as it's intended to be only used by the library.
    pub fn new(record_batch: Arc<RecordBatch>, index_in_batch: usize) -> Self {
        Row { record_batch, index_in_batch }
    }

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
    /// Panics if the column index is out of bounds (`usize` index) or not found (`&str` index).
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
    /// Panics if the column index is out of bounds (`usize` index) or not found (`&str` index) or if the type is not
    /// the expected one.
    pub fn get<I: ColumnIndex, T: Decode>(&self, index: I) -> T {
        match index.index(self.record_batch.schema()) {
            Ok(index) => T::decode(self.record_batch.column(index), self.index_in_batch),
            Err(e) => panic!("{}", e),
        }
    }

    pub fn get_nullable<I: ColumnIndex, T: Decode>(&self, index: I) -> Option<T> {
        match index.index(self.record_batch.schema()) {
            Ok(index) => {
                if decode::is_null(self.record_batch.column(index), self.index_in_batch) {
                    return None;
                }
                Some(T::decode(self.record_batch.column(index), self.index_in_batch))
            }
            Err(e) => panic!("{}", e),
        }
    }

    /// Get a value from a column by its index.
    ///
    /// The index of the column can be either a 0-based index or the name of the column.
    /// This method returns an error if the column index is out of bounds, or if the type is not the expected one, or if
    /// the value is null.
    pub fn try_get<I: ColumnIndex, T: Decode>(&self, index: I) -> Result<T> {
        let index = index.index(self.record_batch.schema())?;
        T::try_decode(self.record_batch.column(index), self.index_in_batch)
    }

    pub fn try_get_nullable<I: ColumnIndex, T: Decode>(&self, index: I) -> Result<Option<T>> {
        let index = index.index(self.record_batch.schema())?;
        if decode::is_null(self.record_batch.column(index), self.index_in_batch) {
            return Ok(None);
        }
        Ok(Some(T::try_decode(self.record_batch.column(index), self.index_in_batch)?))
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
