use std::borrow::{Borrow, BorrowMut};
use std::cell::{Ref, RefCell, RefMut};
use std::cmp::min;
use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use futures::io::Error;
use log::debug;
use thiserror::Error;
use tokio::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadBuf};
use tokio::prelude::{AsyncBufRead, AsyncRead, AsyncWrite};

use crate::types::*;

const BUFFER_SIZE:usize = 8192; // Power of two allows modulo to become a mask operation.
const BUFFER_SIZE_U64:u64 = BUFFER_SIZE as u64;
pub type Byte = u8;
pub type StreamPosition = u64;


/// A Broadcaster is a data stream which supports a writer and many readers.
///   Note there is no flow control and the writer never yields.
///   Each reader
///     o Supports the AsyncRead interface.
///     o Is created with the broadcaster.new_reader() method.
///     o Reads new data as it is written to the broadcaster.
///     o Returns io::Error if it is too slow and falls behind the writer.
pub type Broadcaster = RcCell<BroadcasterInner>;


#[derive(Debug)]
pub struct BroadcasterInner {
    position: StreamPosition,
    buffer: [Byte; BUFFER_SIZE],
    readers: Vec<BroadcastReader>,
    end_of_file: bool,
}



/// Reads data from a broadcast stream.
/// Note it is owned by both the BroadcastStream and by caller who is reading data.
///    so it is implemented as a shared cell to ensure an adequate lifetime.
pub type BroadcastReader = RcCell<BroadcastReaderInner>;
#[derive(Debug)]
pub struct BroadcastReaderInner {
    position: StreamPosition,
    broadcaster: Broadcaster,
    waker: Option<Waker>
}


impl Broadcaster {
    /// Create a new broadcaster.
    pub(crate) fn new() -> Broadcaster {
        RcCell::wrap(BroadcasterInner::new())
    }

    /// Close the broadcast stream and consume the reference to it.
    ///    Note the readers may still have references it.
    ///    As the readers go out of scope, they will drop their references as well.
    fn close(mut self) {
        //let mut streamx = self.borrow_mut();
        let mut broadcaster = self.borrow_refmut();
        broadcaster.end_of_file = true;
        broadcaster.wakeup_readers();
    }

    /// Create a new reader for reading the broadcaster's data.
    fn new_reader(&mut self) -> BroadcastReader {
        BroadcastReader::new(self)
    }
}


impl AsyncWrite for Broadcaster {

    /// Place data in the stream and return immediately.
    ///    Any readers will be woken up by copy_in.
    ///    Note there is no back pressure; slow readers will receive an error.
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, io::Error>> {

        // Temporarily borrow the broadcaster.
        let mut broadcaster = self.deref().borrow_refmut();

        // Copy a chunk of the data into the buffer.  May not be a complete copy if reaches end of buffer.
        let bytes_copied = broadcaster.copy_in(buf);

        Poll::Ready(Ok(bytes_copied))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // Temporarily borrow the broadcaster.
        let mut broadcaster = self.deref().borrow_refmut();

        // Make not it is now at end of file.
        broadcaster.end_of_file = true;
        broadcaster.wakeup_readers();

        Poll::Ready(Ok(()))
    }
}


impl BroadcastReader {

    /// Create a new broadcast reader which is associated with the broadcaster.
    pub fn new(broadcaster: &Broadcaster) -> Self {
        let inner = BroadcastReaderInner {
            position: 0,
            broadcaster: broadcaster.clone_ref(),
            waker: None
        };
        BroadcastReader::wrap(inner)
    }
}


impl AsyncRead for BroadcastReader {

    /// Interface to pass unread data from the broadcaster to the reader, assuming there is data.
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {

        // Borrow the inner broadcaster and reader for the duration of this function.
        //   Assign them to regular '&' borrows so we don't do conflicting RefCell borrows.
        //   TODO: is there a RefMut<> variation which coerces to & without counting as an additional borrow?
        let mut readerx:RefMut<BroadcastReaderInner> = self.deref().borrow_refmut();
        let mut reader = readerx.deref_mut();
        let mut broadcastx = reader.broadcaster.borrow_refmut();
        let mut broadcast = broadcastx.deref_mut();


        // Try to find the correct data which is ready to broadcast.
        let result = broadcast.index_out(reader.position);
        match result {

            // Case: no new data arrived and not shutdown.  Go back to sleep.
            Some( (a, 0usize, false)) => {

                // If we were not already sleeping ...
                if let None = reader.waker {

                    // Save the waker needed to wake us up later.
                    reader.waker = Some(cx.waker().clone());

                    // Add self to the wakeup queue.
                    //   (if we were already in list, then we would have a waker)
                    broadcast.readers.push(self.clone_ref());
                }

                // Let caller know we are not finished.
                Poll::Pending
            },

            // Case: Data or end-of-file. Return data and wake self up.
            Some( (start, len, _)) => {

                // Copy data to the output buffer, being careful to not overflow the output buffer..
                let len = min(len, buf.remaining());
                buf.put_slice(&broadcast.buffer[start..start+len]);

                // Advance our position in the broadcast stream.
                reader.position += len as StreamPosition;

                // Wake up if we were sleeping and clear the waker.
                //   Note: if we had been sleeping, we were removed from the broadcaster's wakeup
                //         list before we were awakened.
                if let Some(waker) = reader.waker.take() {
                    waker.wake();
                }
                Poll::Ready(Ok(()))
            }

            // Case: None means the writer has overflowed a slow reader.  Reset to current position and Return an Error.
            None => {
                reader.position = broadcast.position;
                let error = io::Error::new(io::ErrorKind::Other,
                                           "Reader was too slow and missed broadcast data");
                Poll::Ready(Err(error))
            }
        }
    }
}


// AsyncBufRead causes problems.
// Since it returns a reference to a byte slice and we can only borrow the internal buffer for a short time.
// We probably want to send the byte slice out through an async write, so we end up holing it too long.
// For now, skip AsyncBufRead and avoid potential conflict by copying data to the caller.



impl BroadcasterInner {

    /// Create a new Broadcast stream.
    fn new()-> BroadcasterInner {
        BroadcasterInner {
        position: 0,
        buffer: [0u8; 8*1024],
        readers: vec![],
        end_of_file: false
    }}

    /// Copy bytes into the broadcast buffer, returning the number of bytes copied.
    ///   Note it may have to be called repeatedly to ensure all the bytes are copied in.
    fn copy_in(&mut self, data: &[Byte]) -> usize {

        // Truncate the data so it fits into the buffer without wrapping around.
        let start = (self.position % BUFFER_SIZE_U64) as usize;
        let len = min(BUFFER_SIZE-start, data.len());

        // Copy the data into the buffer.
        self.buffer[start..start+len].copy_from_slice(&data[..len]);
        self.position += len as StreamPosition;

        // Wake up any readers who are waiting for data.
        self.wakeup_readers();

        // Return number of bytes copied.
        len
    }


    /// Return the start index and length of data ready to be copied out of the broadcast buffer.
    ///   Outputs Option<tuple>: (start_index, length, end_of_file)
    ///   Note we output index instead of a slice since we may temporarily lose our "borrow" of the underlying buffer.
    ///   TODO: figure out lifetimes to enable slice instead of indices.
    pub fn index_out(&self, position: StreamPosition) -> Option<(usize, usize, bool)> {

        // Check to see if buffer overflowed.
        if self.position > position+BUFFER_SIZE_U64 {
            None

        // Otherwise ...
        } else {

            // Get the starting index and length of the contiguous data in the buffer.
            let start = (position % BUFFER_SIZE_U64)  as usize;
            let len = min(BUFFER_SIZE-start, (self.position - position) as usize);

            // Return the indices of the relevant data.
            Some((start, len, self.end_of_file))
        }
    }


    /// Wake up readers, emptying the list of pending read requests.
    ///  When completed, all the readers will be woken, their "waker" field set to None, and the
    ///  readers list will be empty.
    fn wakeup_readers(&mut self) {

        // Move the sleeping readers out of the read list.
        //   They will be dropped at the end of this function, decrementing their reference counts.
        let readers = self.readers.split_off(0);

        // For each of the sleeping readers ...
        for reader in readers.iter() {
            let mut rdr = reader.borrow_refmut();

            // Take the waker (leaving None) and wake it up.
            assert!(rdr.waker.is_some(), "BroadcastReaders on a sleep list must have a waker");
            let waker = rdr.waker.take().unwrap();
            waker.wake();
        }
    }
}



#[cfg(test)]

use crate::aw;

mod test_broadcaster_inner {
    use super::*;

    #[test]
    pub fn test_broadcaster_empty() {
        let mut broadcaster = BroadcasterInner::new();

        // Verify an empty write returns an empty result.
        assert_eq!(0usize, broadcaster.copy_in(&[0u8; 0]));
        assert_eq!(Some((0usize, 0usize, false)), broadcaster.index_out(0));
    }

    #[test]
    fn test_read_write() {
        let mut broadcaster = BroadcasterInner::new();

        // Verify we get back the data we wrote.
        let hello_data = "Hello, World".as_bytes();
        assert_eq!(hello_data.len(), broadcaster.copy_in(hello_data));
        let out = broadcaster.index_out(0);
        assert_eq!(Some((0usize, hello_data.len(), false)), out);
    }

    #[test]
    fn test_buffer_wrap() {
        let mut broadcaster = BroadcasterInner::new();
        let part_1 = &[255u8; BUFFER_SIZE / 2 + BUFFER_SIZE / 3];
        let part_2 = &[244u8; BUFFER_SIZE / 2];

        // Add enough data to overfill the buffer. TODO: make each byte different.
        //   We should only fill to the end of the buffer.
        let len1 = broadcaster.copy_in(part_1);
        let len2 = broadcaster.copy_in(part_2);
        assert_eq!(part_1.len(), len1);
        assert!(len1 + len2 == BUFFER_SIZE);  // We should have filled the entire buffer.

        // A new reader at this point should return all the bytes.
        assert_eq!(Some((0, len1 + len2, false)), broadcaster.index_out(0));

        // Write the remaining bytes from part2. Part 2 should be completely written.
        let len3 = broadcaster.copy_in(&part_2[len2..]);
        assert_eq!(part_2.len(), len2 + len3);

        // A slow reader should overflow.
        assert_eq!(None, broadcaster.index_out(0));

        // A fast reader who got part1 should now get all of part2 in two pieces.
        let (start2, len2, eof1) = broadcaster.index_out(len1 as StreamPosition).unwrap();
        let (start3, len3, eof2) = broadcaster.index_out((len1 + len2) as StreamPosition).unwrap();
        assert_eq!(part_2.len(),len2 + len3 );
        assert!(!eof1 & !eof2);
    }

}



#[cfg(test)]

mod test_broadcaster {
    use futures::task::noop_waker_ref;

    use super::*;

    /// Test the broadcaster asynchronously writing to two readers.
    #[test]
    fn test_broadcaster_async() {
        let mut broadcast = Broadcaster::new();
        let mut reader1 = broadcast.new_reader();
        let mut reader2 = broadcast.new_reader();
        let mut buffer1 = [0u8; 1024];
        let mut buffer2 = [0u8; 1024];

        // Write a byte and read it back.
        let result = aw! {broadcast.write(&[1u8])};
        assert_eq!(1usize, result.unwrap());
        let result = aw! {reader1.read(&mut buffer1)};
        assert_eq!(1usize, result.unwrap());
        assert_eq!(buffer1[0], 1u8);
        let result = aw! {reader2.read(&mut buffer2)};
        assert_eq!(1usize, result.unwrap());
        assert_eq!(buffer2[0], 1u8);

        // Overflow the broadcast buffer. Should get Error when reading.
        let mut buffer = [5u8; 1024];
        for i in 1..=64 {
            let result = aw! {broadcast.write_all(&buffer)};
            assert!(result.is_ok());
        }
        let result = aw! {reader1.read(&mut buffer1)};
        assert!(result.is_err());
        let result = aw! {reader2.read(&mut buffer2)};
        assert!(result.is_err());

        // After an on overflow, we should see new data as it arrives.
        let result = aw! {broadcast.write(&[6u8])};
        let result = aw! {reader1.read(&mut buffer1)};
        assert_eq!(1usize, result.unwrap());
        assert_eq!(6u8, buffer1[0]);
        let result = aw! {reader2.read(&mut buffer2)};
        assert_eq!(1usize, result.unwrap());
        assert_eq!(6u8, buffer2[0]);

        // Write a  byte and have the first reader read it.
        let result = aw! {broadcast.write(&[7u8])};
        assert_eq!(1usize, result.unwrap());
        let result = aw! {reader1.read(&mut buffer1)};
        assert_eq!(1usize, result.unwrap());
        assert_eq!(7u8, buffer1[0]);

        // Close the braodcaster. The readers are still alive, but we can no longer access the broadcaster.
        broadcast.close();

        // The second reader should still see that last byte.
        let result = aw! {reader2.read(&mut buffer2)};
        assert_eq!(1usize, result.unwrap());
        assert_eq!(7u8, buffer2[0]);

        // Verify we get end-of-file. We should see a zero length read on both readers.
        let result = aw! {reader1.read(&mut buffer1)};
        assert_eq!(0usize, result.unwrap());
        let result = aw! {reader2.read(&mut buffer2)};
        assert_eq!(0usize, result.unwrap());
    }



}