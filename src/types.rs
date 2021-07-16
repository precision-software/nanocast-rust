use std::cell::{RefCell, Ref, RefMut};
use std::rc::{Rc,Weak};
use std::borrow::{Borrow, BorrowMut};
use std::ops::Deref;


/// Share references to a mutable object.
///   Captures the object and coordinates "borrows" as long as some reference is alive.
// Renamed borrow, clone, borrow_mut and undo the corresponding traits.
//    The problem is &RcCell was getting the wrong version of these routines.
//    It seems the reference conversions mess things up, or at least hide the problems.
// TODO: Verify this really is the problem.
#[derive(Debug)]
pub struct RcCell<T> (Rc<RefCell<T>>);

///   Note the primary functions "clone_ref(), borrow_ref() and borrow_refmut()" are implemented
///   in an extension trait rather than the type itself. Allows user to implement methods directly
///   on the new type.
pub trait RcCellExt<T> {
    fn wrap(obj: T) -> RcCell<T>;
    fn clone_ref(&self) -> RcCell<T>;
    fn borrow_ref(&self) -> Ref<T>;
    fn borrow_refmut(&self) -> RefMut<T>;
    fn downgrade(&self) -> WeakCell<T>;
}
 impl <T> RcCellExt<T> for RcCell<T> {
     fn wrap(obj: T) -> RcCell<T> { RcCell(Rc::new(RefCell::new(obj))) }
     fn clone_ref(&self) -> RcCell<T> { RcCell(self.0.clone()) }
     fn borrow_ref(&self) -> Ref<T> { self.0.as_ref().borrow() }
     fn borrow_refmut(&self) -> RefMut<T> { self.0.as_ref().borrow_mut() }
     fn downgrade(&self) -> WeakCell<T> { WeakCell(Rc::downgrade(&self.0)) }
 }


/// A weak reference counting version of RcCell.
pub struct WeakCell<T> (Weak<RefCell<T>>);
pub trait WeakCellExt<T> {
    fn upgrade(&self) -> Option<RcCell<T>>;
    fn new() -> WeakCell<T>;
}

impl <T> WeakCellExt<T> for WeakCell<T> {
    fn upgrade(&self) -> Option<RcCell<T>> {
        self.0.upgrade().map(|rc| RcCell(rc))
    }
    fn new() -> WeakCell<T> {
        WeakCell(Weak::new())
    }
}




/// Macro to synchronously await an async block.
#[cfg(test)]
#[macro_export]
macro_rules! aw {
    ($e:expr) => {
        tokio_test::block_on($e)
    };
 }
