
use std::collections::HashMap;
use std::env::Args;
use std::fmt::Display;
use std::path::{Path, PathBuf};
use std::str::from_utf8;

use log::*;
use thiserror::Error;
use tokio as runtime;
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::{AsyncRead, AsyncWrite, io};
use tokio::prelude::io::AsyncReadExt;
use tokio::time::Duration;
use tokio_io_timeout::TimeoutStream;

use Error::*;

#[cfg(test)]
use crate::aw;
use tokio::macros::support::{Future, Pin};
use std::rc::Rc;
use crate::types::{RcCell, RcCellExt};
use std::cell::RefCell;

/// AsyncIO is a trait object which supports AsyncRead and AsyncWrite.
///   Because of asynchronous function lifetime issues, it is reference counted.
pub type AsyncIO = RcCell<dyn AsyncIOTrait>;
pub trait AsyncIOTrait: AsyncRead+AsyncWrite+Unpin {}
impl <T> AsyncIOTrait for T where T:AsyncRead+AsyncWrite+Unpin {}


pub type Result<T> = std::result::Result<T, Error>;

pub type RoutingTable = RcCell<RoutingTableInner>;
pub struct RoutingTableInner {
    routes: Vec<(String, String, WrappedService)>,
}

pub type WrappedService = Box<dyn Fn(AsyncIO, Request)->Pin<Box<dyn Future<Output=()>>>>;
trait Service<FUN, FUTURE>: Fn(AsyncIO, Request)->FUTURE {
    fn wrap(f:FUN)->WrappedService;
}

impl<FUN, FUTURE> Service<FUN, FUTURE> for FUN
    where FUTURE:Future<Output=()>,
          FUN:Fn(AsyncIO, Request)->FUTURE {
    fn wrap(f:FUN) -> WrappedService {
        Box::new(|a, b| Box::pin(f(a.clone_ref, b)))
    }
}


// A "Service" is a function which, given a connection and a request, returns a Future.
//    Note the function and the future are parameterized.
//    To be useful, the function and its result must be wrapped in dynamic boxes.

pub fn wrap_service<C, F>(f:C) -> WrappedService
    where F:Future<Output=()>, C:FnOnce(&mut AsyncIO, &Request)->F {
    Box::new(|a:&AsyncIO, b| Box::pin(f(a.clone_ref(), b)))
}

impl RoutingTable {

    pub fn lookup(&self, method:&str, path:&str) -> Option<&WrappedService> {
        let inner = self.borrow_ref();
        for (m, p, f) in inner.routes.iter() {
            if m == method && path.starts_with(p) {
                return Some(f);
            }
        }
        None
    }

    pub fn new() -> RoutingTable {
        RoutingTable::wrap(RoutingTableInner { routes: Vec::new() })
    }

    pub fn push(&mut self, method: &str, path: &str, func: WrappedService) {
        let mut inner = self.borrow_refmut();
        inner.routes.push((method.to_string(), path.to_string(), func));
    }
}



pub(crate) struct NanoServer {}
impl NanoServer {

    /// Listen for an HTTP connection
    pub async fn listen_loop(routes:RoutingTable, addr: &str) {
        let result:Result<()> = try {

            // Bind to the specified address.
            let listener = TcpListener::bind(addr).await?;

            // Spawn a new task for each incoming connection.
            //   Note: we use a local task so to we can use non-threaded RcCell.
            //   Note: we don't limit the number of incoming connections, which could
            //         result in running out of resources.
            while let (stream, ip_addr) = listener.accept().await? {

                // Convert the stream to an AsyncIO trait object with timeout.
                let mut stream = TimeoutStream::new(stream);
                stream.set_read_timeout(Some(Duration::new(10, 0)));
                let mut stream = AsyncIO::new(stream) as AsyncIO;

                // Make our own local reference to the routing table.
                let routes = routes.clone_ref();

                // Spawn an asynchronous task to handle the new connection.
                runtime::task::spawn_local(async move {
                    Self::route_incoming(stream, routes).await;
                });
            }
        };

        // log a server error message if an error occurred.
        //  Note: some errors should be recovered. eg. remote connection went away while in queue, ...
        if let Err(e) = result {
            error!("NanoServer: Problem listening on addr:{} - {}", addr, e);
        }
    }



    /// Route the request according to the http method and the url path.
    ///   Consumes stream and routine table.
    pub async fn route_incoming(mut stream:AsyncIO, routes: RoutingTable) {

        // Start by reading the request.
        match read_request(&mut stream).await {
            Err(e) => error!("NanoServer unable to read http header {}", e),
            Ok(request) => {

                // If successfully read header, match to one of the configured paths.
                //  TODO: use a wildcard or set of methods in the routing table.
                match routes.lookup(&request.method, &request.path) {

                    // If match found, dispatch the function
                    Some(func) => func(&mut stream, &request).await,

                    // Otherwise, give a 404 error.
                    None => Self::not_found(&mut stream, &request).await,
                }
            }
        }
    }

    async fn not_found(stream:&mut AsyncIO, request: &Request) {
        // write out a 404 message.
    }
}

// Note: saving as String rather than more efficient &str
//   - owning (not borrowing) strings make lifetime analysis much simpler.
//  Alternatives - caller allocates buffer and passes it down (needs `static to survive awaits)
//               - buffer is part of Request structures (Request can't be moved.)

pub struct Request {
    pub method: String,
    pub path: String,
    pub version: String,
    pub header: HashMap<String, String>,
}

pub struct Response{

}


/// Display a vector as a bracketed list.
fn vec_to_string<T:Display>(v: &Vec<T>) -> String {
    let strings: Vec<String> = v.iter().map(|t|t.to_string()).collect();
    "[".to_string() + &strings.join(", ") + "]"
}


/// Read the http request line and the http header.
///   Returns a vector of strings from the request line and a K,V map for the header.
pub async fn read_request<T:AsyncRead+Unpin>(stream:&mut T) -> Result<Request> {

    // Read a block of data into the  buffer. Block ends with a blank line.
    //   Note: the sender will be waiting for a response, so we can be sure the block
    //   will end with a blank line.
    let mut buf = [0u8; 4*1024];
    let mut len = 0;
    while !(len > 4 && &buf[len-4..len] == "\r\n\r\n".as_bytes() ) {

        // Buffer overflowed if we have no room left.
        if len >= buf.len() {
            return Err(BadHeader("Buffer overflowed (4K)".into()));
        }

        // Read next batch of bytes. Error if end of file.
        let this_len = stream.read(&mut buf[len..]).await?;
        if this_len == 0 {
            return Err(BadHeader("End of file".into()));
        }

        // Update nr of bytes successfully read.
        len += this_len;
    }

    // Split the buffer into lines of text.
    let mut lines = from_utf8(&buf[..len])?.split("\r\n");

    // The first line is the http request.
    let request_line = lines.next().ok_or(BadHeader("End of file".into()))?;
    let mut fields = request_line.split_ascii_whitespace();
    let method = fields.next().ok_or(BadHeader("HTTP request missing method".into()))?;
    let path = fields.next().ok_or(BadHeader("HTTP request missing url".into()))?;
    let version = fields.next().ok_or(BadHeader("HTTP request missing http version".into()))?;

    // Subsequent lines are the header.

    // Do for each line until an empty line encountered
    let mut header: HashMap<String, String> = HashMap::new();
    for line in lines {
        if line.trim().len() == 0 {break;}

        // Split the line into key:value pairs and add to the header.
        let (k,v) = line.split_once(":")
            .ok_or(BadHeader(line.into()))?;
        if header.insert(k.trim().into(), v.trim().into()).is_some() {
            Err(DuplicateHeader(line.into()))?
        }
    }

    Ok(Request{method: method.to_string(), path:path.to_string(), version:version.to_string(), header })
}


fn parse_header_line(s: &str) -> Result<(&str, &str)> {
    // If the line has an "=", then split it.
    if let Some(index) = s.find(":") {
        Ok( (&s[..index-1], &s[index..]) )
    } else {
        Err(BadHeader(s.to_string()))
    }
}


#[derive(Debug,Error)]
pub enum Error {
    #[error("Invalid NTRIP request: {0}")]
    InvalidNtripRequest(String),
    #[error("Invalid NTRIP header: {0}")]
    BadHeader(String),
    #[error("Duplicate http header line {0}")]
    DuplicateHeader(String),
    #[error("Bad utf8 character in http header")]
    StrError(#[from]std::str::Utf8Error),
    #[error("Problem reading http header")]
    IoError(#[from]io::Error),
}


mod test_http_request {
    use std::cmp::min;

    use futures::task::{Context, Poll};
    use tokio::io::ReadBuf;
    use tokio::macros::support::Pin;
    use tokio::prelude::AsyncWrite;

    use crate::http::{read_request, Request};
    use crate::types::*;

    use super::*;

    #[test]
    fn test_header() {
        // Simple request with no header info.
        let mut stream = TestStream::new(&["SOURCE First HTTP/1.0\r\n\r\n"]);
        let mut buf = [0u8; 4*1024];

        // Read it in and validate.
        let request = aw!{read_request(&mut stream)}.unwrap();
        assert_eq!("SOURCE", request.method);
        assert_eq!("First", request.path);
        assert_eq!("HTTP/1.0", request.version);
        assert!(request.header.is_empty());

        // Empty request - should be error.
        let mut stream = TestStream::new(&[]);
        let r = aw!{read_request(&mut stream)};
        match r {
            Err(e) =>         assert_eq!("Invalid NTRIP header: End of file",  e.to_string()),
            _ => assert!(false, "Should have returned an error")
        }


        // Test stream with a header split across two reads. A third read would generate an error.
        let mut stream = TestStream::new(&[
            "SOURCE   Fir", "st    HTTP/1.1\r\nKey  :  Value\r\n\r\n", ""
        ]);

        // Read the header (two reads) and valiate.
        let request = aw!{read_request(&mut stream)}.unwrap();
        assert_eq!("SOURCE", request.method);
        assert_eq!("First", request.path);
        assert_eq!("HTTP/1.1", request.version);
        assert_eq!("Value", request.header.get("Key").unwrap())
    }


    /// A test stream which provides input and accepts output.
    /// It does not need to be efficient in memory allocation.
    /// For convenience, the interface routines take strings instead of bytes.
    /// An empty string means return io::Error().
    struct TestStream {
        next: usize,
        input: Vec<Vec<u8>>,
        output: Vec<Vec<u8>>,
    }
    impl TestStream {
        fn new(strings: &[&str]) -> TestStream {

            // Convert the input strings to byte vectors.
            let input = strings.iter()
                .map(|s|(*s).as_bytes().to_vec())
                .collect();

            TestStream{next:0, input, output:vec![vec![0u8;0]]}
        }

        fn reset(&mut self) {
            self.next = 0;
            self.output = vec![vec![0u8;0]];
        }

        fn get_output(&self) -> Vec<String>{
            // Convert each byte vector to a string, aborting on error.
            self.output.iter()
                .map(|b|String::from_utf8(b.clone()).unwrap())
                .collect()
        }
    }

    impl AsyncRead for TestStream {
        fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
            let mut stream = self.get_mut();


            // If end of data, return OK without setting bytes.
            let result = if stream.next >= stream.input.len() {
                Ok(())

                // Otherwise, ...
            } else {

                // Get the next result from the test data.
                let data = &stream.input[stream.next];
                stream.next = stream.next + 1;

                // if empty data, return an io:Error()
                if data.is_empty() {
                    Err(io::Error::new(io::ErrorKind::Other, "TestStream generated error"))

                    // otherwise, valid data. Copy it into the reader's buffer.
                    //    Will abort if receiving buffer is too small, but OK in a test program.
                } else {
                    buf.put_slice(data);
                    Ok(())
                }
            };

            Poll::Ready(result)
        }
    }

    impl AsyncWrite for TestStream {
        fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
            let mut stream = self.get_mut();
            stream.output.push(buf.to_vec());
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }


}


