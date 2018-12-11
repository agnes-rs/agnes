use std::ops::Add;

use typenum::{Add1, UTerm, Unsigned, B1};

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
    Cons { head, tail }
}

/// Trait for adding a new element to the front of a [heterogeneous list](struct.Cons.html).
pub trait PushFront {
    /// Add an element to the front of this heterogeneous list.
    fn push_front<H>(self, head: H) -> Cons<H, Self>
    where
        Self: Sized,
    {
        cons(head, self)
    }
}

impl<H, T> PushFront for Cons<H, T> {}
impl PushFront for Nil {}

/// Trait for adding a new element to the end of a [heterogeneous list](struct.Cons.html).
pub trait PushBack<U> {
    /// New data type that for the list after appending an element.
    type Output;
    /// Add an element to the end of this heterogeneous list.
    fn push_back(self, elem: U) -> Self::Output;
}
impl<U> PushBack<U> for Nil {
    type Output = Cons<U, Nil>;
    fn push_back(self, elem: U) -> Cons<U, Nil> {
        cons(elem, Nil)
    }
}
impl<U, H, T> PushBack<U> for Cons<H, T>
where
    T: PushBack<U>,
{
    type Output = Cons<H, T::Output>;
    fn push_back(self, elem: U) -> Cons<H, T::Output> {
        cons(self.head, self.tail.push_back(elem))
    }
}

/// Trait for adding a list to the end of a [heterogeneous list](struct.Cons.html).
pub trait Append<List> {
    /// New data type that for the list after appending a list.
    type Appended;
    /// Add a list to the end of this heterogeneous list.
    fn append(self, list: List) -> Self::Appended;
}
impl<List> Append<List> for Nil {
    type Appended = List;
    fn append(self, list: List) -> List {
        list
    }
}
impl<List, H, T> Append<List> for Cons<H, T>
where
    T: Append<List>,
{
    type Appended = Cons<H, T::Appended>;
    fn append(self, list: List) -> Cons<H, T::Appended> {
        cons(self.head, self.tail.append(list))
    }
}

// #[macro_export]
// macro_rules! map {
//     (@continue($elems:expr)($($output:tt)*) ) => {
//         $($output)*
//     };
//     (@continue($elems:expr)($($output:tt)*) [$($f0:tt)*] $([$($f:tt)*])*) => {
//         map![@continue($elems.tail)($($output)*.prepend(($($f0)*)(&$elems.head))) $([$($f)*])*]
//     };
//     ($elems:expr, $([$($f:tt)*])*) => {{
//         #[allow(unused_imports)]
//         use $crate::cons::Prepend;
//         map![@continue($elems)(Nil) $([$($f)*])*]
//     }}
// }

// doesn't work
// #[macro_export]
// macro_rules! apply_to {
//     (@step(Nil)($value:ident)($($f:tt)*)) => {};
//     (@step($elems:expr)($value:ident)($($f:tt)*)) => {{
//         |$value| {$($f)*}($value);
//         apply_to![@step($elems.tail)($value)($($f)*)]
//     }};
//     ($elems:expr; |$value:ident| $($f:tt)*) => {{
//         apply_to![@step($elems)($value)($($f)*)]
//     }}
// }

pub trait Len {
    type Len: Unsigned;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn len(&self) -> usize {
        Self::Len::to_usize()
    }
}

impl Len for Nil {
    type Len = UTerm;
}
impl<Head, Tail> Len for Cons<Head, Tail>
where
    Tail: Len,
    <Tail as Len>::Len: Add<B1>,
    <<Tail as Len>::Len as Add<B1>>::Output: Unsigned,
{
    type Len = Add1<<Tail as Len>::Len>;
}

#[macro_export]
macro_rules! length {
    ($list:ty) => {
        <<$list as $crate::cons::Len>::Len as typenum::Unsigned>::USIZE
    };
}
