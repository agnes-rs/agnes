/*!
The `MapFn` trait provides a framework for functions that apply to a single element in the data
structure.

The `FieldMapFn` trait provides a framework for functions that apply to a field (column) of data in
the data structure.
*/
use std::marker::PhantomData;

use masked::{MaybeNa, IntoMaybeNa};
use error::*;
use field::{DataType, FieldIdent};
use view::DataView;
use select::Selection;
use store::{DataStore, AddDataVec};
use access::{DataIndex, FieldData};

/// Trait for applying a `MapFn` (single-element mapping function) to all elements of a data
/// structure.
pub trait Apply {
    /// Apply a `MapFn` to this data structure.
    fn apply<F: MapFn>(&self, f: &mut F) -> Result<Vec<F::Output>>;
}

/// Trait for applying a `MapFn` (single-element mapping function) to  all elements of a a specific
/// field of a data structure.
pub trait ApplyTo {
    /// Apply a `MapFn` to a specific field of this data structure.
    fn apply_to<F: MapFn>(&self, f: &mut F, ident: &FieldIdent) -> Result<Vec<F::Output>>;
}

/// Trait for applying a `MapFn` (single-element mapping function) to a specific field at a selected
/// index.
pub trait ApplyToElem {
    /// Apply a `MapFn` to the specific field and index of this data structure.
    fn apply_to_elem<F: MapFn>(&self, f: &mut F, ident: &FieldIdent, idx: usize)
        -> Result<F::Output>;
}

/// Trait for applying a `FieldMapFn` (whole-field mapping function) to a data structure.
pub trait FieldApply {
    /// Apply a `FieldMapFn` to this data structure.
    fn field_apply<F: FieldMapFn>(&self, f: &mut F) -> Result<F::Output>;
}

/// Trait for applying a `FieldMapFn` (whole-field mapping function) to a specific field of a
/// data structure.
pub trait FieldApplyTo {
    /// Apply a `FieldMapFn` to specified field on this data structure.
    fn field_apply_to<F: FieldMapFn>(&self, f: &mut F, ident: &FieldIdent) -> Result<F::Output>;
}

/// Struct to hold the result of a `map` call on a data structure, using a `MapFn`, used for lazy
/// evaluation.
#[derive(Debug, Clone)]
pub struct Map<'a, D: 'a + Apply, F: MapFn> {
    data: &'a D,
    f: F,
    name: String,
}
impl<'a, D: 'a + Apply, F: MapFn> Map<'a, D, F> {
    /// Create a new `Map` structure from data structure and function, providing the name (if
    /// necessary) of the resultant mapped data field.
    pub fn new<N: Into<Option<String>>>(data: &'a D, f: F, name: N) -> Map<'a, D, F> {
        Map {
            data,
            f,
            name: name.into().unwrap_or("Mapped".into())
        }
    }
    /// Compose this `MapFn` with another `MapFn`.
    pub fn map<G: MapFn>(self, g: G) -> Map<'a, D, Composed<F, G>>
        where G: ApplyToDatum<<F::Output as IntoMaybeNa>::DType>
    {
        Map::new(self.data, Composed { f: self.f, g }, self.name)
    }
    /// Set the name of the field that will be produced by this `Map`.
    pub fn name<S: AsRef<str>>(self, new_name: S) -> Map<'a, D, F> {
        Map::new(self.data, self.f, new_name.as_ref().to_string())
    }
    /// Evaluate this structure's `MapFn` on the associated data structure.
    pub fn collect<B: FromMap<F::Output>>(self) -> Result<B> {
        B::from_map(self)
    }
}
impl<'a, 'b, D: 'a + ApplyTo> Apply for Selection<'a, D> {
    fn apply<F: MapFn>(&self, f: &mut F) -> Result<Vec<F::Output>> {
        self.data.apply_to(f, &self.ident)
    }
}
impl<'a, D: ApplyTo> Selection<'a, D> {
    /// Apply a `MapFn` to this selection (to be lazy evaluated).
    pub fn map<F: MapFn>(&self, f: F) -> Map<Self, F> {
        Map::new(self, f, None)
    }
}

/// Trait for conversion from a `Map` structure. Usually called from `map.collect()`.
pub trait FromMap<A: IntoMaybeNa>: Sized {
    /// Convert a `Map` structure into the resultant data structure.
    fn from_map<'a, D: 'a + Apply, F>(map: Map<'a, D, F>) -> Result<Self>
        where F: MapFn<Output=A>;
}
impl<A: IntoMaybeNa> FromMap<A> for Vec<A> {
    fn from_map<'a, D: 'a + Apply, F>(mut map: Map<'a, D, F>) -> Result<Vec<A>>
        where F: MapFn<Output=A>
    {
        map.data.apply(&mut map.f)
    }
}
impl<A: IntoMaybeNa> FromMap<A> for DataView
    where DataStore: AddDataVec<A::DType>
{
    fn from_map<'a, D: 'a + Apply, F>(map: Map<'a, D, F>) -> Result<DataView>
        where F: MapFn<Output=A>
    {
        let field_name = map.name.clone();
        let mut mapped_data_vec = map.collect::<Vec<_>>()?;
        let data_vec = mapped_data_vec.drain(..)
            .map(|value| value.into_maybena()).collect();
        let mut ds = DataStore::empty();
        ds.add_data_vec(field_name.into(), data_vec);
        Ok(ds.into())
    }
}

/// Creates a MapFn that computes f(g(x)).
pub struct Composed<F: MapFn, G: MapFn> {
    f: F,
    g: G,
}
impl<F: MapFn, G: MapFn> MapFn for Composed<F, G>
    where G: ApplyToDatum<<F::Output as IntoMaybeNa>::DType>
{
    type Output = <G as ApplyToDatum<<F::Output as IntoMaybeNa>::DType>>::Output;

    fn apply_unsigned(&mut self, value: MaybeNa<&u64>) -> Self::Output {
        self.g.apply_to_datum(self.f.apply_unsigned(value).into_maybena().as_ref())
    }
    fn apply_signed(&mut self, value: MaybeNa<&i64>) -> Self::Output {
        self.g.apply_to_datum(self.f.apply_signed(value).into_maybena().as_ref())
    }
    fn apply_text(&mut self, value: MaybeNa<&String>) -> Self::Output {
        self.g.apply_to_datum(self.f.apply_text(value).into_maybena().as_ref())
    }
    fn apply_boolean(&mut self, value: MaybeNa<&bool>) -> Self::Output {
        self.g.apply_to_datum(self.f.apply_boolean(value).into_maybena().as_ref())
    }
    fn apply_float(&mut self, value: MaybeNa<&f64>) -> Self::Output {
        self.g.apply_to_datum(self.f.apply_float(value).into_maybena().as_ref())
    }
}


/// Trait for a type-dependent function that applies to a specific element.
pub trait MapFn {
    /// The desired output of this function.
    type Output: IntoMaybeNa;

    /// The method to use when working with unsigned (`u64`) data.
    fn apply_unsigned(&mut self, value: MaybeNa<&u64>) -> Self::Output;
    /// The method to use when working with signed (`i64`) data.
    fn apply_signed(&mut self, value: MaybeNa<&i64>) -> Self::Output;
    /// The method to use when working with text (`String`) data.
    fn apply_text(&mut self, value: MaybeNa<&String>) -> Self::Output;
    /// The method to use when working with boolean (`bool`) data.
    fn apply_boolean(&mut self, value: MaybeNa<&bool>) -> Self::Output;
    /// The method to use when working with floating-point (`f64`) data.
    fn apply_float(&mut self, value: MaybeNa<&f64>) -> Self::Output;
}

#[macro_export]
macro_rules! map_fn {
    // Using a prexisting type
    ($map_fn_ty:ty, Output = $output:ty; $($rest:tt)*) => {
        impl MapFn for $map_fn_ty {
            type Output = $output;
            map_fn_impl!($($rest)*);
        }
    };
    // create a type (private, no generics)
    ($(#[$meta_attr:meta])* $map_fn_ty:ident {
        type Output = $output:ty;
        $($attrs:tt)*
    } $($rest:tt)*) => {
        $(#[$meta_attr])*
        struct $map_fn_ty {
            $($attrs)*
        }
        impl MapFn for $map_fn_ty {
            type Output = $output;
            map_fn_impl!($($rest)*);
        }
    };
    // create a type (private, with generics)
    ($(#[$meta_attr:meta])* $map_fn_ty:ident<($($generics:tt)*)> {
        type Output = $output:ty;
        $($attrs:tt)*
    } $($rest:tt)*) => {
        $(#[$meta_attr])*
        struct $map_fn_ty<$($generics)*> {
            $($attrs)*
        }
        impl<$($generics)*> MapFn for $map_fn_ty<$($generics)*> {
            type Output = $output;
            map_fn_impl!($($rest)*);
        }
    };
    // create a type (private, with generics + bounds)
    ($(#[$meta_attr:meta])* $map_fn_ty:ident<($($generics:tt)*)> where ($($bounds:tt)*) {
        type Output = $output:ty;
        $($attrs:tt)*
    } $($rest:tt)*) => {
        $(#[$meta_attr])*
        struct $map_fn_ty<$($generics)*> {
            $($attrs)*
        }
        impl<$($generics)*> MapFn for $map_fn_ty<$($generics)*> where $($bounds)* {
            type Output = $output;
            map_fn_impl!($($rest)*);
        }
    };
    // create a type (public, no generics)
    ($(#[$meta_attr:meta])* pub $map_fn_ty:ident {
        type Output = $output:ty;
        $($attrs:tt)*
    } $($rest:tt)*) => {
        $(#[$meta_attr])*
        pub struct $map_fn_ty {
            $($attrs)*
        }
        impl MapFn for $map_fn_ty {
            type Output = $output;
            map_fn_impl!($($rest)*);
        }
    };
    // create a type (public, with generics)
    ($(#[$meta_attr:meta])* pub $map_fn_ty:ident<($($generics:tt)*)> {
        type Output = $output:ty;
        $($attrs:tt)*
    } $($rest:tt)*) => {
        $(#[$meta_attr])*
        pub struct $map_fn_ty<$($generics)*> {
            $($attrs)*
        }
        impl<$($generics)*> MapFn for $map_fn_ty<$($generics)*> {
            type Output = $output;
            map_fn_impl!($($rest)*);
        }
    };
    // create a type (public, with generics + bounds)
    ($(#[$meta_attr:meta])* pub $map_fn_ty:ident<($($generics:tt)*)> where ($($bounds:tt)*) {
        type Output = $output:ty;
        $($attrs:tt)*
    } $($rest:tt)*) => {
        $(#[$meta_attr])*
        pub struct $map_fn_ty<$($generics)*> {
            $($attrs)*
        }
        impl<$($generics)*> MapFn for $map_fn_ty<$($generics)*> where $($bounds)* {
            type Output = $output;
            map_fn_impl!($($rest)*);
        }
    };
}
#[macro_export]
macro_rules! map_fn_impl {
    (fn [$dtype1:tt]($self:ident, $value:ident) { $($body:tt)* } $($rest:tt)*) => (
        map_fn_impl!(fn $dtype1($self, $value) { $($body)* });
        map_fn_impl!($($rest)*);
    );
    (fn [$dtype1:tt, $dtype2:tt]($self:ident, $value:ident) { $($body:tt)* } $($rest:tt)*) => (
        map_fn_impl!(fn $dtype1($self, $value) { $($body)* });
        map_fn_impl!(fn $dtype2($self, $value) { $($body)* });
        map_fn_impl!($($rest)*);
    );
    (fn [$dtype1:tt, $dtype2:tt, $dtype3:tt]($self:ident, $value:ident)
            { $($body:tt)* } $($rest:tt)*) =>
    (
        map_fn_impl!(fn $dtype1($self, $value) { $($body)* });
        map_fn_impl!(fn $dtype2($self, $value) { $($body)* });
        map_fn_impl!(fn $dtype3($self, $value) { $($body)* });
        map_fn_impl!($($rest)*);
    );
    (fn [$dtype1:tt, $dtype2:tt, $dtype3:tt, $dtype4:tt]($self:ident, $value:ident)
            { $($body:tt)* } $($rest:tt)*) =>
    (
        map_fn_impl!(fn $dtype1($self, $value) { $($body)* });
        map_fn_impl!(fn $dtype2($self, $value) { $($body)* });
        map_fn_impl!(fn $dtype3($self, $value) { $($body)* });
        map_fn_impl!(fn $dtype4($self, $value) { $($body)* });
        map_fn_impl!($($rest)*);
    );
    (fn [$dtype1:tt, $dtype2:tt, $dtype3:tt, $dtype4:tt, $dtype5:tt]($self:ident, $value:ident)
            { $($body:tt)* }) =>
    (
        map_fn_impl!(fn $dtype1($self, $value) { $($body)* });
        map_fn_impl!(fn $dtype2($self, $value) { $($body)* });
        map_fn_impl!(fn $dtype3($self, $value) { $($body)* });
        map_fn_impl!(fn $dtype4($self, $value) { $($body)* });
        map_fn_impl!(fn $dtype4($self, $value) { $($body)* });
    );
    (fn all($self:ident, $value:ident) { $($body:tt)* }) => (
        map_fn_impl!(
            fn unsigned($self, $value) { $($body)* }
            fn signed($self, $value) { $($body)* }
            fn text($self, $value) { $($body)* }
            fn boolean($self, $value) { $($body)* }
            fn float($self, $value) { $($body)* }
        );
    );
    (fn unsigned($self:ident, $value:ident) { $($body:tt)* } $($rest:tt)*) => (
        fn apply_unsigned(&mut $self, $value: MaybeNa<&u64>) -> Self::Output {
            $($body)*
        }
        map_fn_impl!($($rest)*);
    );
    (fn signed($self:ident, $value:ident) { $($body:tt)* } $($rest:tt)*) => (
        fn apply_signed(&mut $self, $value: MaybeNa<&i64>) -> Self::Output {
            $($body)*
        }
        map_fn_impl!($($rest)*);
    );
    (fn text($self:ident, $value:ident) { $($body:tt)* } $($rest:tt)*) => (
        fn apply_text(&mut $self, $value: MaybeNa<&String>) -> Self::Output {
            $($body)*
        }
        map_fn_impl!($($rest)*);
    );
    (fn boolean($self:ident, $value:ident) { $($body:tt)* } $($rest:tt)*) => (
        fn apply_boolean(&mut $self, $value: MaybeNa<&bool>) -> Self::Output {
            $($body)*
        }
        map_fn_impl!($($rest)*);
    );
    (fn float($self:ident, $value:ident) { $($body:tt)* } $($rest:tt)*) => (
        fn apply_float(&mut $self, $value: MaybeNa<&f64>) -> Self::Output {
            $($body)*
        }
        map_fn_impl!($($rest)*);
    );
    (fn [$dtype1:tt]($self:ident, _) { $($body:tt)* } $($rest:tt)*) => (
        map_fn_impl!(fn $dtype1($self, _) { $($body)* });
        map_fn_impl!($($rest)*);
    );
    (fn [$dtype1:tt, $dtype2:tt]($self:ident, _) { $($body:tt)* } $($rest:tt)*) => (
        map_fn_impl!(fn $dtype1($self, _) { $($body)* });
        map_fn_impl!(fn $dtype2($self, _) { $($body)* });
        map_fn_impl!($($rest)*);
    );
    (fn [$dtype1:tt, $dtype2:tt, $dtype3:tt]($self:ident, _)
            { $($body:tt)* } $($rest:tt)*) =>
    (
        map_fn_impl!(fn $dtype1($self, _) { $($body)* });
        map_fn_impl!(fn $dtype2($self, _) { $($body)* });
        map_fn_impl!(fn $dtype3($self, _) { $($body)* });
        map_fn_impl!($($rest)*);
    );
    (fn [$dtype1:tt, $dtype2:tt, $dtype3:tt, $dtype4:tt]($self:ident, _)
            { $($body:tt)* } $($rest:tt)*) =>
    (
        map_fn_impl!(fn $dtype1($self, _) { $($body)* });
        map_fn_impl!(fn $dtype2($self, _) { $($body)* });
        map_fn_impl!(fn $dtype3($self, _) { $($body)* });
        map_fn_impl!(fn $dtype4($self, _) { $($body)* });
        map_fn_impl!($($rest)*);
    );
    (fn [$dtype1:tt, $dtype2:tt, $dtype3:tt, $dtype4:tt, $dtype5:tt]($self:ident, _)
            { $($body:tt)* }) =>
    (
        map_fn_impl!(fn $dtype1($self, _) { $($body)* });
        map_fn_impl!(fn $dtype2($self, _) { $($body)* });
        map_fn_impl!(fn $dtype3($self, _) { $($body)* });
        map_fn_impl!(fn $dtype4($self, _) { $($body)* });
        map_fn_impl!(fn $dtype4($self, _) { $($body)* });
    );
    (fn all($self:ident, _) { $($body:tt)* }) => (
        map_fn_impl!(
            fn unsigned($self, _) { $($body)* }
            fn signed($self, _) { $($body)* }
            fn text($self, _) { $($body)* }
            fn boolean($self, _) { $($body)* }
            fn float($self, _) { $($body)* }
        );
    );
    (fn unsigned($self:ident, _) { $($body:tt)* } $($rest:tt)*) => (
        fn apply_unsigned(&mut $self, _: MaybeNa<&u64>) -> Self::Output {
            $($body)*
        }
        map_fn_impl!($($rest)*);
    );
    (fn signed($self:ident, _) { $($body:tt)* } $($rest:tt)*) => (
        fn apply_signed(&mut $self, _: MaybeNa<&i64>) -> Self::Output {
            $($body)*
        }
        map_fn_impl!($($rest)*);
    );
    (fn text($self:ident, _) { $($body:tt)* } $($rest:tt)*) => (
        fn apply_text(&mut $self, _: MaybeNa<&String>) -> Self::Output {
            $($body)*
        }
        map_fn_impl!($($rest)*);
    );
    (fn boolean($self:ident, _) { $($body:tt)* } $($rest:tt)*) => (
        fn apply_boolean(&mut $self, _: MaybeNa<&bool>) -> Self::Output {
            $($body)*
        }
        map_fn_impl!($($rest)*);
    );
    (fn float($self:ident, _) { $($body:tt)* } $($rest:tt)*) => (
        fn apply_float(&mut $self, _: MaybeNa<&f64>) -> Self::Output {
            $($body)*
        }
        map_fn_impl!($($rest)*);
    );
    () => ()
}


macro_rules! tpanic {
    ($actual_ty_str:expr, $expected_ty_str:expr) => {
        panic!("type error: type-specific map function expecting type '{}' applied to data \
            of type '{}'", $actual_ty_str, $expected_ty_str);
    }
}

/// Trait for applying an arbitrary function (implementing the trait `Fn`) to every data element
/// in a data structure.
pub trait ApplyUnchecked<T: DataType> {
    /// Apply the provided function to each data element in this data structure. Panics if applied
    /// to a data element of a type the function is not expecting.
    fn apply_unchecked<F, S, Output>(&self, state: S, f: F) -> Result<Vec<Output>>
        where F: Fn(MaybeNa<&T>, &mut S) -> Output, Output: IntoMaybeNa;
}
impl<U> ApplyUnchecked<u64> for U where U: Apply {
    fn apply_unchecked<F, S, Output>(&self, state: S, f: F) -> Result<Vec<Output>>
        where F: Fn(MaybeNa<&u64>, &mut S) -> Output, Output: IntoMaybeNa
    {
        map_fn![
            UncheckedUnsignedFn<(F, S, Output)>
                where (F: Fn(MaybeNa<&u64>, &mut S) -> Output, Output: IntoMaybeNa)
            {
                type Output = Output;
                f: F,
                state: S,
                phantom: PhantomData<Output>
            }
            fn unsigned(self, value) { (self.f)(value, &mut self.state) }
            fn signed(self, _) { tpanic!["signed", "unsigned"]; }
            fn text(self, _) { tpanic!["text", "unsigned"]; }
            fn boolean(self, _) { tpanic!["boolean", "unsigned"]; }
            fn float(self, _) { tpanic!["float", "unsigned"]; }
        ];
        self.apply(&mut UncheckedUnsignedFn { f, state, phantom: PhantomData })
    }
}
impl<U> ApplyUnchecked<i64> for U where U: Apply {
    fn apply_unchecked<F, S, Output>(&self, state: S, f: F) -> Result<Vec<Output>>
        where F: Fn(MaybeNa<&i64>, &mut S) -> Output, Output: IntoMaybeNa
    {
        map_fn![
            UncheckedSignedFn<(F, S, Output)>
                where (F: Fn(MaybeNa<&i64>, &mut S) -> Output, Output: IntoMaybeNa)
            {
                type Output = Output;
                f: F,
                state: S,
                phantom: PhantomData<Output>
            }
            fn unsigned(self, _) { tpanic!["unsigned", "signed"]; }
            fn signed(self, value) { (self.f)(value, &mut self.state) }
            fn text(self, _) { tpanic!["text", "signed"]; }
            fn boolean(self, _) { tpanic!["boolean", "signed"]; }
            fn float(self, _) { tpanic!["float", "signed"]; }
        ];
        self.apply(&mut UncheckedSignedFn { f, state, phantom: PhantomData })
    }
}
impl<U> ApplyUnchecked<String> for U where U: Apply {
    fn apply_unchecked<F, S, Output>(&self, state: S, f: F) -> Result<Vec<Output>>
        where F: Fn(MaybeNa<&String>, &mut S) -> Output, Output: IntoMaybeNa
    {
        map_fn![
            UncheckedTextFn<(F, S, Output)>
                where (F: Fn(MaybeNa<&String>, &mut S) -> Output, Output: IntoMaybeNa)
            {
                type Output = Output;
                f: F,
                state: S,
                phantom: PhantomData<Output>
            }
            fn unsigned(self, _) { tpanic!["unsigned", "text"]; }
            fn signed(self, _) { tpanic!["signed", "text"] }
            fn text(self, value) { (self.f)(value, &mut self.state) }
            fn boolean(self, _) { tpanic!["boolean", "text"]; }
            fn float(self, _) { tpanic!["float", "text"]; }
        ];
        self.apply(&mut UncheckedTextFn { f, state, phantom: PhantomData })
    }
}
impl<U> ApplyUnchecked<bool> for U where U: Apply {
    fn apply_unchecked<F, S, Output>(&self, state: S, f: F) -> Result<Vec<Output>>
        where F: Fn(MaybeNa<&bool>, &mut S) -> Output, Output: IntoMaybeNa
    {
        map_fn![
            UncheckedBoolFn<(F, S, Output)>
                where (F: Fn(MaybeNa<&bool>, &mut S) -> Output, Output: IntoMaybeNa)
            {
                type Output = Output;
                f: F,
                state: S,
                phantom: PhantomData<Output>
            }
            fn unsigned(self, _) { tpanic!["unsigned", "boolean"]; }
            fn signed(self, _) { tpanic!["signed", "boolean"] }
            fn text(self, _) { tpanic!["text", "boolean"]; }
            fn boolean(self, value) { (self.f)(value, &mut self.state) }
            fn float(self, _) { tpanic!["float", "boolean"]; }
        ];
        self.apply(&mut UncheckedBoolFn { f, state, phantom: PhantomData })
    }
}
impl<U> ApplyUnchecked<f64> for U where U: Apply {
    fn apply_unchecked<F, S, Output>(&self, state: S, f: F) -> Result<Vec<Output>>
        where F: Fn(MaybeNa<&f64>, &mut S) -> Output, Output: IntoMaybeNa
    {
        map_fn![
            UncheckedFloatFn<(F, S, Output)>
                where (F: Fn(MaybeNa<&f64>, &mut S) -> Output, Output: IntoMaybeNa)
            {
                type Output = Output;
                f: F,
                state: S,
                phantom: PhantomData<Output>
            }
            fn unsigned(self, _) { tpanic!["unsigned", "float"]; }
            fn signed(self, _) { tpanic!["signed", "float"] }
            fn text(self, _) { tpanic!["text", "float"]; }
            fn boolean(self, _) { tpanic!["boolean", "float"]; }
            fn float(self, value) { (self.f)(value, &mut self.state) }
        ];
        self.apply(&mut UncheckedFloatFn { f, state, phantom: PhantomData })
    }
}

/// Trait for structures that can be applied to a single `MaybeNa` value, resulting in a specific
/// output.
pub trait ApplyToDatum<T: DataType> {
    /// The output when this type is applied to a datum.
    type Output: IntoMaybeNa;
    /// Apply this type to a datum.
    fn apply_to_datum(&mut self, value: MaybeNa<&T>) -> Self::Output;
}
macro_rules! impl_apply_datum {
    ($($dtype:ty, $f:tt);*) => {$(

impl<T> ApplyToDatum<$dtype> for T where T: MapFn {
    type Output = <Self as MapFn>::Output;
    fn apply_to_datum(&mut self, value: MaybeNa<&$dtype>) -> Self::Output {
        self.$f(value)
    }
}

    )*}
}
impl_apply_datum!(
    u64,    apply_unsigned;
    i64,    apply_signed;
    String, apply_text;
    bool,   apply_boolean;
    f64,    apply_float
);


/// Trait for a type-dependent function that applies to a single field.
pub trait FieldMapFn {
    /// The desired output of this function.
    type Output;
    /// The method to use when working with unsigned (`u64`) data.
    fn apply_unsigned<T: DataIndex<u64>>(&mut self, field: &T) -> Self::Output;
    /// The method to use when working with signed (`i64`) data.
    fn apply_signed<T: DataIndex<i64>>(&mut self, field: &T) -> Self::Output;
    /// The method to use when working with text (`String`) data.
    fn apply_text<T: DataIndex<String>>(&mut self, field: &T) -> Self::Output;
    /// The method to use when working with boolean (`bool`) data.
    fn apply_boolean<T: DataIndex<bool>>(&mut self, field: &T) -> Self::Output;
    /// The method to use when working with floating-point (`f64`) data.
    fn apply_float<T: DataIndex<f64>>(&mut self, field: &T) -> Self::Output;
}

#[macro_export]
macro_rules! field_map_fn {
    // Using a prexisting type
    ($map_fn_ty:ty, Output = $output:ty; $($rest:tt)*) => {
        impl FieldMapFn for $map_fn_ty {
            type Output = $output;
            field_map_fn_impl!($($rest)*);
        }
    };
    // create a type (private, no generics)
    ($(#[$meta_attr:meta])* $map_fn_ty:ident {
        type Output = $output:ty;
        $($attr:ident: $attr_ty:ty),*
    } $($rest:tt)*) => {
        $(#[$meta_attr])*
        struct $map_fn_ty {
            $($attr: $attr_ty),*
        }
        impl FieldMapFn for $map_fn_ty {
            type Output = $output;
            field_map_fn_impl!($($rest)*);
        }
    };
    // create a type (private, with generics)
    ($(#[$meta_attr:meta])* $map_fn_ty:ident<($($generics:tt)*)> {
        type Output = $output:ty;
        $($attr:ident: $attr_ty:ty),*
    } $($rest:tt)*) => {
        $(#[$meta_attr])*
        struct $map_fn_ty<$($generics)*> {
            $($attr: $attr_ty),*
        }
        impl<$($generics)*> FieldMapFn for $map_fn_ty<$($generics)*> {
            type Output = $output;
            field_map_fn_impl!($($rest)*);
        }
    };
    // create a type (private, with generics + bounds)
    ($(#[$meta_attr:meta])* $map_fn_ty:ident<($($generics:tt)*)> where ($($bounds:tt)*) {
        type Output = $output:ty;
        $($attr:ident: $attr_ty:ty),*
    } $($rest:tt)*) => {
        $(#[$meta_attr])*
        struct $map_fn_ty<$($generics)*> {
            $($attr: $attr_ty),*
        }
        impl<$($generics)*> FieldMapFn for $map_fn_ty<$($generics)*> where $($bounds)* {
            type Output = $output;
            field_map_fn_impl!($($rest)*);
        }
    };
    // create a type (public, no generics)
    ($(#[$meta_attr:meta])* pub $map_fn_ty:ident {
        type Output = $output:ty;
        $($attr:ident: $attr_ty:ty),*
    } $($rest:tt)*) => {
        $(#[$meta_attr])*
        pub struct $map_fn_ty {
            $($attr: $attr_ty),*
        }
        impl FieldMapFn for $map_fn_ty {
            type Output = $output;
            field_map_fn_impl!($($rest)*);
        }
    };
    // create a type (public, with generics)
    ($(#[$meta_attr:meta])* pub $map_fn_ty:ident<($($generics:tt)*)> {
        type Output = $output:ty;
        $($attr:ident: $attr_ty:ty),*
    } $($rest:tt)*) => {
        $(#[$meta_attr])*
        pub struct $map_fn_ty<$($generics)*> {
            $($attr: $attr_ty),*
        }
        impl<$($generics)*> FieldMapFn for $map_fn_ty<$($generics)*> {
            type Output = $output;
            field_map_fn_impl!($($rest)*);
        }
    };
    // create a type (public, with generics + bounds)
    ($(#[$meta_attr:meta])* pub $map_fn_ty:ident<($($generics:tt)*)> where ($($bounds:tt)*) {
        type Output = $output:ty;
        $($attr:ident: $attr_ty:ty),*
    } $($rest:tt)*) => {
        $(#[$meta_attr])*
        pub struct $map_fn_ty<$($generics)*> {
            $($attr: $attr_ty),*
        }
        impl<$($generics)*> FieldMapFn for $map_fn_ty<$($generics)*> where $($bounds)* {
            type Output = $output;
            field_map_fn_impl!($($rest)*);
        }
    };
}
#[macro_export]
macro_rules! field_map_fn_impl {
    (fn [$dtype1:tt](self, $field:ident) { $($body:tt)* } $($rest:tt)*) => (
        field_map_fn_impl!(fn $dtype1(self, $field) { $($body)* });
        field_map_fn_impl!($($rest)*);
    );
    (fn [$dtype1:tt, $dtype2:tt](self, $field:ident) { $($body:tt)* } $($rest:tt)*) => (
        field_map_fn_impl!(fn $dtype1(self, $field) { $($body)* });
        field_map_fn_impl!(fn $dtype2(self, $field) { $($body)* });
        field_map_fn_impl!($($rest)*);
    );
    (fn [$dtype1:tt, $dtype2:tt, $dtype3:tt](self, $field:ident) { $($body:tt)* } $($rest:tt)*) => (
        field_map_fn_impl!(fn $dtype1(self, $field) { $($body)* });
        field_map_fn_impl!(fn $dtype2(self, $field) { $($body)* });
        field_map_fn_impl!(fn $dtype3(self, $field) { $($body)* });
        field_map_fn_impl!($($rest)*);
    );
    (fn [$dtype1:tt, $dtype2:tt, $dtype3:tt, $dtype4:tt](self, $field:ident)
            { $($body:tt)* } $($rest:tt)*) =>
    (
        field_map_fn_impl!(fn $dtype1(self, $field) { $($body)* });
        field_map_fn_impl!(fn $dtype2(self, $field) { $($body)* });
        field_map_fn_impl!(fn $dtype3(self, $field) { $($body)* });
        field_map_fn_impl!(fn $dtype4(self, $field) { $($body)* });
        field_map_fn_impl!($($rest)*);
    );
    (fn [$dtype1:tt, $dtype2:tt, $dtype3:tt, $dtype4:tt, $dtype5:tt](self, $field:ident)
            { $($body:tt)* }) =>
    (
        field_map_fn_impl!(fn $dtype1(self, $field) { $($body)* });
        field_map_fn_impl!(fn $dtype2(self, $field) { $($body)* });
        field_map_fn_impl!(fn $dtype3(self, $field) { $($body)* });
        field_map_fn_impl!(fn $dtype4(self, $field) { $($body)* });
        field_map_fn_impl!(fn $dtype4(self, $field) { $($body)* });
    );
    (fn all(self, $field:ident) { $($body:tt)* }) => (
        field_map_fn_impl!(
            fn unsigned(self, $field) { $($body)* }
            fn signed(self, $field) { $($body)* }
            fn text(self, $field) { $($body)* }
            fn boolean(self, $field) { $($body)* }
            fn float(self, $field) { $($body)* }
        );
    );
    (fn unsigned(self, $field:ident) { $($body:tt)* } $($rest:tt)*) => (
        #[allow(unused_variables)]
        fn apply_unsigned<T: DataIndex<u64>>(&mut self, $field: &T) -> Self::Output {
            #[allow(dead_code)]
            type DType = u64;
            $($body)*
        }
        field_map_fn_impl!($($rest)*);
    );
    (fn signed(self, $field:ident) { $($body:tt)* } $($rest:tt)*) => (
        #[allow(unused_variables)]
        fn apply_signed<T: DataIndex<i64>>(&mut self, $field: &T) -> Self::Output {
            #[allow(dead_code)]
            type DType = i64;
            $($body)*
        }
        field_map_fn_impl!($($rest)*);
    );
    (fn text(self, $field:ident) { $($body:tt)* } $($rest:tt)*) => (
        #[allow(unused_variables)]
        fn apply_text<T: DataIndex<String>>(&mut self, $field: &T) -> Self::Output {
            #[allow(dead_code)]
            type DType = String;
            $($body)*
        }
        field_map_fn_impl!($($rest)*);
    );
    (fn boolean(self, $field:ident) { $($body:tt)* } $($rest:tt)*) => (
        #[allow(unused_variables)]
        fn apply_boolean<T: DataIndex<bool>>(&mut self, $field: &T) -> Self::Output {
            #[allow(dead_code)]
            type DType = bool;
            $($body)*
        }
        field_map_fn_impl!($($rest)*);
    );
    (fn float(self, $field:ident) { $($body:tt)* } $($rest:tt)*) => (
        #[allow(unused_variables)]
        fn apply_float<T: DataIndex<f64>>(&mut self, $field: &T) -> Self::Output {
            #[allow(dead_code)]
            type DType = f64;
            $($body)*
        }
        field_map_fn_impl!($($rest)*);
    );
    () => ()
}


/// Trait for combiner functions to apply to a field.
pub trait FieldReduceFn<'a> {
    /// The desired output of this function.
    type Output;

    /// Reduce a vector of `ReduceDataIndex` structures (which represent any data structure that
    /// implements `DataIndex`) into a single output.
    fn reduce(&mut self, fields: Vec<FieldData<'a>>) -> Self::Output;
}

/// Trait for data structure which can have a `FieldReduceFn` applied to them.
pub trait ApplyFieldReduce<'a> {
    /// Apply a `FieldReduceFn` to this data structure.
    fn apply_field_reduce<F: FieldReduceFn<'a>>(&self, f: &mut F)
        -> Result<F::Output>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_utils::*;
    use view::DataView;
    use select::Select;

    #[test]
    fn convert() {
        let dv = sample_merged_emp_table();
        println!("{}", dv);

        map_fn![
            ConvertUnsigned { type Output = MaybeNa<u64>; }
            fn unsigned(self, value) { value.map(|&val| val) }
            fn signed(self, value) { value.map(|&val| if val < 0 { 0 } else { val as u64 }) }
            fn text(self, value) { value.map(|&ref val| val.parse().unwrap_or(0)) }
            fn boolean(self, value) { value.map(|&val| if val { 1 } else { 0 }) }
            fn float(self, value) { value.map(|&val| if val < 0.0 { 0 } else { val as u64 }) }
        ];
        let mapped: DataView = dv.select_one("VacationHrs")
            .map(ConvertUnsigned {}).collect().expect("failed to convert");
        println!("{}", mapped);
        unsigned::assert_dv_eq_vec(&mapped, &"Mapped".into(),
            vec![47u64, 54, 98, 12, 0, 5, 22]
        );

        map_fn![
            ConvertFloat { type Output = MaybeNa<f64>; }
            fn [signed, float](self, value) { value.map(|&val| val as f64) }
            fn unsigned(self, value) { value.map(|&val| val as f64 + 0.0001) }
            fn text(self, value) { value.map(|&ref val| val.parse().unwrap_or(0.0)) }
            fn boolean(self, value) { value.map(|&val| if val { 1.0 } else { 0.0 }) }
        ];
        let mapped2: DataView = dv
            .select_one("VacationHrs")
            .map(ConvertUnsigned {})
            .map(ConvertFloat {})
            .name("VacationHrs2")
            .collect().expect("convert failed");
        println!("{}", mapped2);
        float::assert_dv_eq_vec(&mapped2, &"VacationHrs2".into(),
            vec![47.0001, 54.0001, 98.0001, 12.0001, 0.0001, 5.0001, 22.0001]
        );
    }
}
