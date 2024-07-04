use futures::Stream;
use squill_core::Result;
use std::pin::Pin;
use std::task::Poll;

use crate::Connection;

pub struct RecordBatchStream<'conn> {
    command_sent: bool,
    next_tx: tokio::sync::mpsc::Sender<Result<Option<arrow_array::RecordBatch>>>,
    next_rx: tokio::sync::mpsc::Receiver<Result<Option<arrow_array::RecordBatch>>>,
    conn: &'conn Connection,
}

impl<'conn> RecordBatchStream<'conn> {
    pub fn new(conn: &'conn Connection) -> Self {
        let (next_tx, next_rx) = tokio::sync::mpsc::channel(1);
        Self { command_sent: false, next_tx, next_rx, conn }
    }
}

impl<'conn> Stream for RecordBatchStream<'conn> {
    type Item = Result<arrow_array::RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut std::task::Context) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if !this.command_sent {
            this.conn.fetch(this.next_tx.clone())?;
            this.command_sent = true;
        }

        match Pin::new(&mut this.next_rx).poll_recv(cx) {
            Poll::Ready(Some(result)) => {
                this.command_sent = false; // Reset the flag for the next fetch
                match result {
                    Ok(Some(batch)) => Poll::Ready(Some(Ok(batch))),
                    Ok(None) => Poll::Ready(None),
                    Err(e) => Poll::Ready(Some(Err(e))),
                }
            }
            Poll::Ready(None) => Poll::Ready(None), // Channel closed
            Poll::Pending => Poll::Pending,
        }
    }
}
