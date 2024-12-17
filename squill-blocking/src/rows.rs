use arrow_array::RecordBatch;
use squill_core::row::Row;
use squill_core::Result;
use std::sync::Arc;

/// A Collection of rows returned by a query.
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

/// An iterator over the rows returned by a query.
impl<'i> Iterator for Rows<'i> {
    type Item = Result<Row>;

    fn next(&mut self) -> Option<Result<Row>> {
        if self.last_record_batch.is_none() {
            // First call or we've exhausted the last batch.
            self.last_record_batch = match self.iterator.next() {
                Some(Ok(record_batch)) => {
                    // There is a batch available.
                    self.index_in_batch = 0;
                    Some(Arc::new(record_batch))
                }
                Some(Err(e)) => {
                    // AN error occurred while fetching the next batch.
                    return Some(Err(e));
                }
                // No more batches available.
                None => None,
            };
        }
        match &self.last_record_batch {
            None => None,
            Some(last_record_batch) => {
                let row = Row::new(last_record_batch.clone(), self.index_in_batch);
                self.index_in_batch += 1;
                if self.index_in_batch >= last_record_batch.num_rows() {
                    // we've exhausted the current batch
                    self.last_record_batch = None;
                }
                Some(Ok(row))
            }
        }
    }
}
