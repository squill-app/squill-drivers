use crate::{Error, Result};
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
            Ok(index) => self.record_batch.column(index).is_null(self.index_in_batch),
            Err(e) => panic!("{}", e),
        }
    }

    /// Get a value from a column by its index.
    ///
    /// The index of the column can be either a 0-based index or the name of the column.
    ///
    /// # Panics
    /// Panics if the column index is out of bounds or if the type is not the expected one.
    pub fn get<I: ColumnIndex, T: GetColumn>(&self, index: I) -> T {
        match index.index(self.record_batch.schema()) {
            Ok(index) => T::get_column(&self.record_batch, self.index_in_batch, index),
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

pub trait GetColumn {
    fn get_column(record_batch: &RecordBatch, index_in_batch: usize, index: usize) -> Self;
}

macro_rules! impl_get_column {
    ($type:ty, $array_type:ident) => {
        impl GetColumn for $type {
            fn get_column(record_batch: &RecordBatch, index_in_batch: usize, index: usize) -> Self {
                record_batch
                    .column(index)
                    .as_any()
                    .downcast_ref::<arrow_array::$array_type>()
                    .unwrap()
                    .value(index_in_batch)
                    .into()
            }
        }
    };
}

impl_get_column!(i8, Int8Array);
impl_get_column!(i16, Int16Array);
impl_get_column!(i32, Int32Array);
impl_get_column!(i64, Int64Array);
impl_get_column!(u8, UInt8Array);
impl_get_column!(u16, UInt16Array);
impl_get_column!(u32, UInt32Array);
impl_get_column!(u64, UInt64Array);
impl_get_column!(f32, Float32Array);
impl_get_column!(f64, Float64Array);
impl_get_column!(bool, BooleanArray);
impl_get_column!(String, StringArray);

#[cfg(test)]
mod tests {
    use crate::{connection::Connection, NO_PARAM};

    use super::*;
    use arrow_array::RecordBatch;

    #[test]
    fn test_query_rows() {
        let conn = Connection::open("mock://").unwrap();
        let mut stmt = conn.prepare("SELECT 2").unwrap();
        let mut rows = conn.query_rows(&mut stmt, NO_PARAM).unwrap();
        assert_eq!(rows.next().unwrap().unwrap().get::<usize, i32>(0), 1);
        let row = rows.next().unwrap().unwrap();
        assert!(!row.is_null(0));
        assert_eq!(row.get::<&str, i32>("col0"), 2);
        assert!(rows.next().is_none());
    }

    #[test]
    fn test_get_row_value() {
        macro_rules! test_get_row_value {
            ($data_type:ident, $arrow_data_type:ident, $array_type:ident, $value:expr) => {
                let record_batch = RecordBatch::try_new(
                    Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                        "col0",
                        arrow_schema::DataType::$arrow_data_type,
                        false,
                    )])),
                    vec![Arc::new(arrow_array::$array_type::from(vec![$value]))],
                )
                .unwrap();

                assert_eq!($data_type::get_column(&record_batch, 0, 0), $value);
            };
        }
        test_get_row_value!(i8, Int8, Int8Array, i8::MAX);
        test_get_row_value!(i16, Int16, Int16Array, i16::MAX);
        test_get_row_value!(i32, Int32, Int32Array, i32::MAX);
        test_get_row_value!(i64, Int64, Int64Array, i64::MAX);
        test_get_row_value!(u8, UInt8, UInt8Array, u8::MAX);
        test_get_row_value!(u16, UInt16, UInt16Array, u16::MAX);
        test_get_row_value!(u32, UInt32, UInt32Array, u32::MAX);
        test_get_row_value!(u64, UInt64, UInt64Array, u64::MAX);
        test_get_row_value!(f32, Float32, Float32Array, f32::MAX);
        test_get_row_value!(f64, Float64, Float64Array, f64::MAX);
        test_get_row_value!(bool, Boolean, BooleanArray, true);
        test_get_row_value!(String, Utf8, StringArray, "Hello, World!".to_string());
    }
}
