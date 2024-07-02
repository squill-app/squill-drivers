use std::sync::Arc;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use crate::Result;

pub struct Rows<'i> {
    last_record_batch: Option<Arc<RecordBatch>>,
    iterator: Box<dyn Iterator<Item = Result<RecordBatch>> + 'i>,
    index_in_batch: usize,
}

impl<'i> From<Box<dyn Iterator<Item = Result<RecordBatch>> + 'i>> for Rows<'i> {
    fn from(iterator: Box<dyn Iterator<Item = Result<RecordBatch>> + 'i>) -> Self {
        Rows {
            last_record_batch: None,
            iterator,
            index_in_batch: 0,
        }
    }
}

pub struct Row {
    record_batch: Arc<RecordBatch>,
    index_in_batch: usize,
}

impl Row {
    /// Get the description of the row.
    pub fn schema(&self) -> SchemaRef {
        self.record_batch.schema()
    }

    /// Get the description of a column from its index.
    ///
    /// # Panics
    /// Panics if the column index is out of bounds.
    pub fn describe_column(&self, index: usize) -> arrow_schema::Field {
        self.schema().field(index).clone()
    }

    /// Get the number of columns in the row.
    pub fn num_columns(&self) -> usize {
        self.schema().fields().len()
    }

    /// Get the value of a column from its index.
    ///
    /// The index of the column is 0-based.
    ///
    /// # Panics
    /// Panics if the column index is out of bounds or if the type is not the expected one.
    pub fn column<T: GetColumn>(&self, index: usize) -> T {
        T::get_column(&self.record_batch, self.index_in_batch, index)
    }

    /// Get the value of a column from its name.
    ///
    /// The name of the column is case-sensitive.
    /// # Panics
    /// Panics if the column name is not found or if the type is not the expected one.
    pub fn column_by_name<T: GetColumn>(&self, name: &str) -> T {
        T::get_column_by_name(&self.record_batch, self.index_in_batch, name)
    }

    /// Check if the value of a column from its index is null.
    ///
    /// # Panics
    /// Panics if the column index is out of bounds.
    pub fn is_null(&self, index: usize) -> bool {
        self.record_batch.column(index).is_null(self.index_in_batch)
    }
}

pub trait GetColumn {
    fn get_column(record_batch: &RecordBatch, index_in_batch: usize, index: usize) -> Self;
    fn get_column_by_name(record_batch: &RecordBatch, index_in_batch: usize, name: &str) -> Self;
}

macro_rules! impl_get_column {
    ($type:ty, $array_type:ident) => {
        impl GetColumn for $type {
            fn get_column(record_batch: &RecordBatch, index_in_batch: usize, index: usize) -> Self {
                record_batch.column(index).as_any().downcast_ref::<arrow_array::$array_type>().unwrap().value(index_in_batch)
            }
            fn get_column_by_name(record_batch: &RecordBatch, index_in_batch: usize, name: &str) -> Self {
                match record_batch.schema().index_of(name) {
                    Ok(index) => Self::get_column(record_batch, index_in_batch, index),
                    Err(_e) => panic!("Column '{}' not found", name),
                }
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

impl GetColumn for String {
    fn get_column(record_batch: &RecordBatch, index_in_batch: usize, column: usize) -> Self {
        record_batch
            .column(column)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap()
            .value(index_in_batch)
            .into()
    }
    fn get_column_by_name(record_batch: &RecordBatch, index_in_batch: usize, name: &str) -> Self {
        match record_batch.schema().index_of(name) {
            Ok(index) => Self::get_column(record_batch, index_in_batch, index),
            Err(_e) => panic!("Column '{}' not found", name),
        }
    }
}

impl<'i> Iterator for Rows<'i> {
    type Item = Result<Row>;

    fn next(&mut self) -> Option<Result<Row>> {
        if
            self.last_record_batch.is_none() ||
            self.index_in_batch >= self.last_record_batch.as_ref().unwrap().num_rows()
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

#[cfg(test)]
mod tests {
    use std::{ error::Error, sync::Arc };
    use arrow_array::RecordBatch;
    use super::*;

    #[test]
    fn test_iterator_for_rows() {
        let schema = Arc::new(
            arrow_schema::Schema::new(vec![arrow_schema::Field::new("col0", arrow_schema::DataType::Int32, true)])
        );
        let batch_records = vec![
            RecordBatch::try_new(schema.clone(), vec![Arc::new(arrow_array::Int32Array::from(vec![1, 2]))]),
            RecordBatch::try_new(schema.clone(), vec![Arc::new(arrow_array::Int32Array::from(vec![Some(3), None]))])
        ];

        let boxed_iterator: Box<dyn Iterator<Item = Result<RecordBatch>>> = Box::new(
            batch_records.into_iter().map(|result| {
                result.map_err(|arrow_error| {
                    // Convert `arrow_error` into the appropriate error type expected by
                    // `Result<RecordBatch, Box<dyn Error + Send + Sync>>`
                    Box::new(arrow_error) as Box<dyn Error + Send + Sync>
                })
            })
        );
        let mut expected_value: i32 = 1;
        for row in Rows::from(boxed_iterator) {
            assert!(row.is_ok());
            if expected_value != 4 {
                assert_eq!(row.unwrap().column::<i32>(0), expected_value);
                expected_value += 1;
            } else {
                assert!(row.unwrap().is_null(0));
            }
        }
    }

    #[test]
    fn test_get_row_value() {
        macro_rules! test_get_row_value {
            ($data_type:ident, $arrow_data_type:ident, $array_type:ident, $value:expr) => {
        let record_batch = RecordBatch::try_new(
            Arc::new(
                arrow_schema::Schema::new(vec![arrow_schema::Field::new("col0", arrow_schema::DataType::$arrow_data_type, false)])
            ),
            vec![Arc::new(arrow_array::$array_type::from(vec![$value]))]
        ).unwrap();

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
