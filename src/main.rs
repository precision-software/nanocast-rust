#![feature(try_blocks)]
#![feature(str_split_once)]
#![feature(array_methods)]
#![feature(in_band_lifetimes)]
#![feature(async_closure)]

mod ntrip;
mod mount_table;
mod broadcaster;
mod types;
mod http;
mod broadcaster;
mod TaskGroup;
mod Broadcaster;
mod broadcaster;

use log::debug;
use mount_table::MountTable;

use crate::types::RcCell;


#[tokio::main]
async fn main() {
    env_logger::init();
    log::debug!("Starting - trial debug message\n");
    log::error!("Starting - trial error message\n");
    let mt = MountTable::new();

    let addr = "0.0.0.0:9999";
    //listen_for_ntrip(addr, &mt).await;
}
