pub mod test;

use std::cmp::{min};
use std::pin::Pin;
use futures::task::{Waker, noop_waker, Context, Poll};
use futures::Future;
use std::rc::Rc;
use std::cell::RefCell;

const BUFFER_SIZE:usize = 8192; // Power of two allows modulo to become a mask operation.
const BUFFER_SIZE_U64:u64 = BUFFER_SIZE as u64;
pub type Byte = u8;
pub type StreamPosition = u64;
pub type ByteSlice<'a> = &'a [Byte];


/// A broadcast stream which can be referenced by multiple tasks.
pub struct Broadcaster {
    shared_stream: SharedStream
}

impl  Broadcaster {

    pub fn new()->Broadcaster {
        Broadcaster{ shared_stream: Rc::new(RefCell::new(BroadcastStream::new()))}
    }

    /// Return a Future which reads data from the stream.
    pub fn read(&mut self, position: StreamPosition) -> ReadFuture {
        // TODO: optimize case where data is already present.
        ReadFuture{position, stream:self.shared_stream.clone(), waker:None }
    }


    /// Place data in the stream and return immediately.
    ///    Any readers will be woken up by copy_in.
    pub fn write(&mut self, data: ByteSlice) {
        let mut stream = self.shared_stream.borrow_mut();

        // Copy pieces of data into the buffer until finished.
        let mut start_pos:usize  = 0;
        while start_pos < data.len() {
            start_pos += stream.copy_in(&data[start_pos..]);
        }
    }
}

// TODO: create type with "new" constructor.  Maybe "Shared<BroadcastStream>".
type SharedStream = Rc<RefCell<BroadcastStream>>;

pub struct BroadcastStream
{
    position: StreamPosition,
    buffer: [Byte; BUFFER_SIZE], // Want the structure to be boxed (not on stack)
    read_futures: Vec<ReadFuture>
}



impl BroadcastStream {

    /// Create a new shared portion of a broadcaster.
    fn new() -> BroadcastStream {
        BroadcastStream {
            position:0,
            buffer: [0u8; BUFFER_SIZE],
            read_futures:Vec::new()
        }
    }

    /// Copy bytes into the buffer, returning number of bytes copied.
    ///   Note it may have to be called repeatedly until all the bytes are copied.
    fn copy_in(&mut self, data: ByteSlice) -> usize {

        // Truncate the data so it fits into the buffer without wrapping around.
        let start = (self.position % BUFFER_SIZE_U64) as usize;
        let end = min(start + data.len(), BUFFER_SIZE);

        // Copy the data into the buffer.
        self.buffer[start..end].copy_from_slice(&data[..end-start]);
        self.position += (end-start) as StreamPosition;

        // Wake up any readers who are waiting for data.
        self.wakeup_readers();

        // Return number of bytes copied.
        end-start
    }


    /// Return the start and end buffer positions of data to be copied out of the stream buffer.
    /// Note we don't actually copy the data, but allow the caller to refer to the stream's buffer.
    fn copy_out(&mut self, position: StreamPosition) -> Option<(usize, usize)> {

        // Get the buffer locations where data starts and stops. May wrap around.
        let start = (position % BUFFER_SIZE_U64)  as usize;
        let end = (self.position % BUFFER_SIZE_U64) as usize;

        // CASE: buffer overflowed.
        if self.position >= position+BUFFER_SIZE_U64 {
            None

        // CASE: desired data is contiguous in buffer.
        } else if start <= end {
            Some((start, end))

        // OTHERWISE: desired data wrapped around. Return the contiguous portion.
        } else {
            Some((start, BUFFER_SIZE))
        }
    }


    /// Wake up read requests, emptying out the list of pending read requests.
    fn wakeup_readers(&mut self) {
        let read_futures = self.read_futures.split_off(0);
        for mut read_future in read_futures {
            read_future.wake();
        }
    }
}



pub struct ReadFuture {
    position: StreamPosition,
    stream:  SharedStream,
    waker: Option<Waker>
}

impl ReadFuture {
    fn wake(&mut self) {
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }
}


impl Future for ReadFuture {
    type Output = Option<(usize, usize)>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {

        // See if there is data to copy out of the broadcaster buffer.
        let t = self.stream.borrow_mut().copy_out(self.position);

        match t {
            // CASE: no new data.
            Some((a, b)) if a == b => {
                self.waker = Some(cx.waker().clone());
                Poll::Pending
            },

            // Otherwise: new data. arrived. Note output is None if buffer overflowed.
            output => {
                self.wake();
                Poll::Ready(output)
            }
        }
    }
}