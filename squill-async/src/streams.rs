use crate::connection::Command;
use arrow_array::RecordBatch;
use futures::Stream;
use squill_core::driver;
use squill_core::rows::Row;
use squill_core::Error;
use squill_core::Result;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;

/// A non-blocking stream of Arrow's record batches.
pub struct RecordBatchStream<'s> {
    command_sent: bool,
    command_tx: crossbeam_channel::Sender<Command>,
    poll_tx: tokio::sync::mpsc::Sender<driver::Result<Option<arrow_array::RecordBatch>>>,
    poll_rx: tokio::sync::mpsc::Receiver<driver::Result<Option<arrow_array::RecordBatch>>>,
    phantom: std::marker::PhantomData<&'s ()>,
}

impl<'s> RecordBatchStream<'s> {
    pub(crate) fn new(command_tx: crossbeam_channel::Sender<Command>) -> Self {
        let (poll_tx, poll_rx) = tokio::sync::mpsc::channel(1);
        Self { command_sent: false, poll_tx, poll_rx, command_tx, phantom: std::marker::PhantomData }
    }

    fn fetch_cursor(&self, tx: tokio::sync::mpsc::Sender<driver::Result<Option<RecordBatch>>>) -> Result<()> {
        if let Err(e) = self.command_tx.send(Command::FetchCursor { tx }) {
            return Err(Error::InternalError { error: e.into() });
        }
        Ok(())
    }

    fn drop_cursor(&self) -> Result<()> {
        if let Err(e) = self.command_tx.send(Command::DropCursor) {
            return Err(Error::DriverError { error: e.into() });
        }
        Ok(())
    }
}

impl<'s> Stream for RecordBatchStream<'s> {
    type Item = Result<arrow_array::RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut std::task::Context) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if !this.command_sent {
            this.fetch_cursor(this.poll_tx.clone())?;
            this.command_sent = true;
        }

        match Pin::new(&mut this.poll_rx).poll_recv(cx) {
            Poll::Ready(Some(result)) => {
                this.command_sent = false; // Reset the flag for the next fetch
                match result {
                    Ok(Some(batch)) => Poll::Ready(Some(Ok(batch))),
                    Ok(None) => Poll::Ready(None),
                    Err(error) => Poll::Ready(Some(Err(Error::DriverError { error }))),
                }
            }
            Poll::Ready(None) => Poll::Ready(None), // Channel closed
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Release the statement when the stream is dropped.
///
/// While a stream exists, the connection can only fetch records from that stream and cannot perform other operations.
/// So it is important to drop the cursor when the stream is dropped letting the connection to perform other operations.
/// In normal conditions, the stream should be dropped when the records are consumed but if the user doesn't want/need
/// to consume all the records, he can use the {{Drop}} trait on {{RecordBatchStream}} to release the cursor.
impl Drop for RecordBatchStream<'_> {
    fn drop(&mut self) {
        let _ = self.drop_cursor();
    }
}

/// A non-blocking stream of rows.
pub struct RowStream<'i> {
    // The iterator used to poll the RecordBatch.
    iterator: RecordBatchStream<'i>,

    // The last record batch that was polled.
    last_record_batch: Option<Arc<RecordBatch>>,

    // The index of the next row to poll in the last record batch.
    index_in_batch: usize,
}

impl<'i> From<RecordBatchStream<'i>> for RowStream<'i> {
    fn from(iterator: RecordBatchStream<'i>) -> Self {
        RowStream { last_record_batch: None, iterator, index_in_batch: 0 }
    }
}

impl<'i> Stream for RowStream<'i> {
    type Item = Result<Row>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut std::task::Context) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.last_record_batch.is_none() {
            // First call or we've exhausted the last batch.
            this.last_record_batch = match Pin::new(&mut this.iterator).poll_next(cx) {
                std::task::Poll::Ready(Some(Ok(record_batch))) => {
                    this.index_in_batch = 0;
                    Some(Arc::new(record_batch))
                }
                std::task::Poll::Ready(Some(Err(error))) => return std::task::Poll::Ready(Some(Err(error))),
                std::task::Poll::Ready(None) => None,
                std::task::Poll::Pending => return std::task::Poll::Pending,
            }
        }

        match &this.last_record_batch {
            None => std::task::Poll::Ready(None),
            Some(last_record_batch) => {
                let row = Row::new(last_record_batch.clone(), this.index_in_batch);
                this.index_in_batch += 1;
                if this.index_in_batch >= last_record_batch.num_rows() {
                    this.last_record_batch = None;
                }
                std::task::Poll::Ready(Some(Ok(row)))
            }
        }
    }
}
