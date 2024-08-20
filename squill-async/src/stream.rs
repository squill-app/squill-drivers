use crate::connection::Command;
use arrow_array::RecordBatch;
use futures::Stream;
use squill_core::driver;
use squill_core::Error;
use squill_core::Result;
use std::pin::Pin;
use std::task::Poll;

pub struct RecordBatchStream<'conn> {
    command_sent: bool,
    command_tx: crossbeam_channel::Sender<Command>,
    poll_tx: tokio::sync::mpsc::Sender<driver::Result<Option<arrow_array::RecordBatch>>>,
    poll_rx: tokio::sync::mpsc::Receiver<driver::Result<Option<arrow_array::RecordBatch>>>,
    phantom: std::marker::PhantomData<&'conn ()>,
}

impl<'conn> RecordBatchStream<'conn> {
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

impl<'conn> Stream for RecordBatchStream<'conn> {
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
