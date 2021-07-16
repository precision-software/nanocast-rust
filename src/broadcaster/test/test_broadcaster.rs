use super::super::{Broadcaster,ReadFuture};
use futures::task::{Waker, RawWaker};

pub fn test() {

    let mut broadcaster = Broadcaster::new();

    let future = broadcaster.read(0);

    broadcaster.write(&[1u8]);


}


struct TestWaker {
    awake: bool
}

impl TestWaker {
    pub fn new() -> TestWaker {
        TestWaker{awake: false}
    }
}

impl RawWaker for TestWaker {

}
