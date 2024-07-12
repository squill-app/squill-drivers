use futures::Stream;
use squill_core::driver;
use squill_core::Error;
use squill_core::Result;
use std::pin::Pin;
use std::task::Poll;

use crate::Connection;

pub struct RecordBatchStream<'conn> {
    command_sent: bool,
    poll_tx: tokio::sync::mpsc::Sender<driver::Result<Option<arrow_array::RecordBatch>>>,
    poll_rx: tokio::sync::mpsc::Receiver<driver::Result<Option<arrow_array::RecordBatch>>>,
    conn: &'conn Connection,
}

impl<'conn> RecordBatchStream<'conn> {
    pub fn new(conn: &'conn Connection) -> Self {
        let (poll_tx, poll_rx) = tokio::sync::mpsc::channel(1);
        Self { command_sent: false, poll_tx, poll_rx, conn }
    }
}

impl<'conn> Stream for RecordBatchStream<'conn> {
    type Item = Result<arrow_array::RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut std::task::Context) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if !this.command_sent {
            this.conn.fetch_cursor(this.poll_tx.clone())?;
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
        let _ = self.conn.drop_cursor();
    }
}
