use std::fmt::{Display};
use tokio::time::Duration;
use tokio_io_timeout::TimeoutStream;
use std::str::{from_utf8};

use super::*;
use crate::types::*;
use crate::mount_table::MountTable;

type Result<T> = std::result::Result<T, Error>;
use self::Error::*;
use crate::http::Request;
use crate::http;


/// Listen for an NTRIP connection
pub async fn listen_for_ntrip(addr: &str, mt: &MountTable) {
    let result:Result<()> = try {

        // Create a listener on the specified address.
        let listener = TcpListener::bind(addr).await?;

        // Spawn a new task for each incoming connection.
        //   Note: we use a local task so our SharedCell works with Rc and RefCell.
        //   Note: we don't limit the number of incoming connections, which could
        //         result in running out of resources.
        while let (stream, ip_addr) = listener.accept().await? {

            // Start by putting a timeout wrapper around the tcp stream.
            let mut stream = TimeoutStream::new(stream);
            stream.set_read_timeout(Some(Duration::new(10, 0)));

            // Process the stream as ntrip.
            runtime::task::spawn_local(process_ntrip(stream, mt.clone_ref()));
        }
    };

    // log a message if an error occurred.
    if let Err(e) = result {
        error!("Problem listening on addr:{} - {}", addr, e);
    }
}


/// Task to process an Ntrip connection.
///   Note: consumes stream and mount table - both must survive as long as the task.
async fn process_ntrip<T>(mut stream:T, mt:MountTable)
where T:AsyncRead+AsyncWrite+Unpin {    // Why Unpin? It must survive across async await.

    // With proper error handling ...
    let result:Result<()> = try {

        // Read the http header.
        let request = http::read_request(&mut stream).await?;

        match &request.method[..] {
            "SOURCE" => source::process_source(&mut stream, &request, &mt).await?,
            "GET" => client::process_get(stream, &request, &mt ).await?,
            other => Err(InvalidNtripMethod(other.into()))?
        }
    };

    // If there were errors, then log an error message.
    if let Err(e) = result {
        error!("Connection closed. {}", e);  // Be nice to get peer address.
    }
}


#[derive(Debug,Error)]
pub enum Error {
    #[error("Invalid NTRIP request: {0}")]
    InvalidNtripMethod(String),
    #[error("Bad HTTP header for NTRIP")]
    HttpError(#[from]http::Error),
    #[error("Listener couldn't process connection")]
    IoError(#[from]io::Error),
    #[error(transparent)]
    ClientError(#[from]client::Error),
    #[error(transparent)]
    ServerError(#[from]source::Error),
}
