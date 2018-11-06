
/// The end of a heterogeneous type list.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct Nil;
/// Buildling block of a heterogeneous type list.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct Cons<H, T> {
    /// Value of this element of the type list.
    pub head: H,
    /// Remaining elements of the type list.
    pub tail: T,
}

/// Helper function to construct a [Cons](struct.Cons.html) list.
pub fn cons<H, T>(head: H, tail: T) -> Cons<H, T> {
    Cons {
        head,
        tail
    }
}

/// Trait for adding a new element to the front of a [heterogeneous list](struct.Cons.html).
pub trait Prepend {
    /// Add an element to the front of this heterogeneous list.
    fn prepend<H>(self, head: H) -> Cons<H, Self> where Self: Sized {
        cons(head, self)
    }
}
impl<H, T> Prepend for Cons<H, T> {}
impl Prepend for Nil {}

pub trait ConsMap: Prepend {}
impl<T> ConsMap for T where T: Prepend {}

/// Trait for adding a new element to the end of a [heterogeneous list](struct.Cons.html).
pub trait Append<U> {
    /// New data type that for the lst after appending this element.
    type Appended;
    /// Add an element to the end of this heterogeneous list.
    fn append(self, elem: U) -> Self::Appended;
}
impl<U> Append<U> for Nil {
    type Appended = Cons<U, Nil>;
    fn append(self, elem: U) -> Cons<U, Nil> {
        cons(elem, Nil)
    }
}
impl<U, H, T> Append<U> for Cons<H, T> where T: Append<U> {
    type Appended = Cons<H, T::Appended>;
    fn append(self, elem: U) -> Cons<H, T::Appended> {
        cons(self.head, self.tail.append(elem))
    }
}

#[macro_export]
macro_rules! map {
    (@continue($elems:expr)($($output:tt)*) ) => {
        $($output)*
    };
    (@continue($elems:expr)($($output:tt)*) [$($f0:tt)*] $([$($f:tt)*])*) => {
        map![@continue($elems.tail)($($output)*.prepend(($($f0)*)(&$elems.head))) $([$($f)*])*]
    };
    ($elems:expr, $([$($f:tt)*])*) => {{
        #[allow(unused_imports)]
        use $crate::cons::Prepend;
        map![@continue($elems)(Nil) $([$($f)*])*]
    }}
}

