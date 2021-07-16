

use std::collections::HashMap;
use log::*;
use runtime::net::{TcpListener, TcpStream};
use thiserror::Error;
use tokio as runtime;
use tokio::io;
use tokio::prelude::*;
use tokio::stream::Stream;
use std::cell::RefMut;
use std::borrow::BorrowMut;

use super::mount_table::*;
use super::types::*;
use super::http;
use super::http::{Request, RoutingTable, NanoServer, WrappedService, AsyncIO};


type StringMap = HashMap<String, String>;

mod caster {
    use super::*;
    use futures::Future;

    type Result<T> = std::result::Result<T, Error>;
    static MOUNT_TABLE:MountTable = MountTable::new();

    /*pub fn bind_mt<'b, T:'b>(func: impl 'static+Fn(& AsyncIO, &Request, MountTable)->T, mt: &MountTable)->impl Fn(&AsyncIO, &Request)->T {
        let mt = mt.clone_ref();
        let curried = move |stream:&mut AsyncIO, request:&Request| {let mt = mt.clone_ref(); func(stream, request, mt.clone_ref())};
        curried
    }*/


    pub async fn run_caster(addr: &str, mount_file:&str) {
        // Read the mount table.
        let mt = MountTable::new();

        let cpg = http::wrap_service(|x,y|client::process_get(x.clone_ref(),y));
        //let cpg:WrappedService = Box::new(|a, b| Box::pin(client::process_get(a, b))) as WrappedService;

        // Set up the http routing.
        let mut rt = RoutingTable::new();
        rt.push("GET", "/", cpg);
        //rt.push("SOURCE", "/", source::process_source.into());

    }



    enum Error {

    }

}

mod client {
    use super::*;
    type Result<T> = std::result::Result<T, Error>;

    /// Note: need to own MountTable so not limited by its lifetime.
    pub async fn process_get<'a>(stream:&'static mut AsyncIO, request: &'a Request) {
        if request.version != "HTTP/1.1" {
            Err(NtripBadVersion(request.version.to_string()))
        } else {
            Ok(())
        }; // TODO: error handling.
    }

    use self::Error::*;
    use crate::http::Request;

    #[derive(Debug,Error)]
    pub enum Error {
        #[error("Http version '{0}' is not recognized, only HTTP/1.1")]
        NtripBadVersion(String),
        #[error("NTRIP listener unable to accept connection")]
        IoError{#[from]source:io::Error},
    }
}

mod source {
    use super::*;
    type Result<T> = std::result::Result<T,Error>;


    pub async fn process_source(stream:AsyncIO, request:Request, mt: &MountTable) -> Result<()> {
        // TODO: create try block, write http error response if error.

        // Get the mountpoint and activate a braodcaster. Quick, temporary borrow of the mount table.
        let mut broadcaster = mt.create_broadcaster(&request.path)?;  // Need to send http error message back

        // Start receiving data. Would love to write directly to broadcaster buffer, but will generate borrow conflicts.
        let mut buf = [0u8; 4*1024];   // Be nice to read directly into broadcasters buffer.
        loop {
            let len = stream.read(&mut buf).await?;
            if len == 0 { break; }
            broadcaster.write_all(&buf[..len]).await?;
        }

        Ok(())
    }

    use self::Error::*;
    use crate::broadcaster::BroadcasterInner;
    use crate::mount_table;
    use crate::http::Request;

    #[derive(Debug,Error)]
    pub enum Error {
        #[error("I/O error while starting NTRIP connection.")]
        IoError{#[from]source: io::Error},
        #[error("Mount Table error while starting NTRIP connection.")]
        MountTableError{#[from]source: mount_table::Error},
    }
}
