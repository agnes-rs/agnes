use std::hash::{Hash, Hasher};

use field::FieldIdent;
use apply::{Map, Apply, ApplyTo, MapFn};
use error::*;
use field::DataType;
use masked::MaybeNa;

/// Trait implemented by data structures that represent a single column / vector / field of data.
pub trait DataIndex<T: DataType> {
    /// Returns the data (possibly NA) at the specified index, if it exists.
    fn get_data(&self, idx: usize) -> Result<MaybeNa<&T>>;
    /// Returns the length of this data field.
    fn len(&self) -> usize;
}

impl<'a, T: DataType> DataIndex<T> for Vec<MaybeNa<&'a T>> {
    fn get_data(&self, idx: usize) -> Result<MaybeNa<&T>> {
        Ok(self[idx].clone())
    }
    fn len(&self) -> usize {
        self.len()
    }
}
impl<T: DataType> DataIndex<T> for Vec<MaybeNa<T>> {
    fn get_data(&self, idx: usize) -> Result<MaybeNa<&T>> {
        Ok(self[idx].as_ref())
    }
    fn len(&self) -> usize {
        self.len()
    }
}
impl<T: DataType> DataIndex<T> for Vec<T> {
    fn get_data(&self, idx: usize) -> Result<MaybeNa<&T>> {
        Ok(MaybeNa::Exists(&self[idx]))
    }
    fn len(&self) -> usize {
        self.len()
    }
}

/// Either an owned data structure or reference to a data structure that implements `DataIndex`.
pub enum OwnedOrRef<'a, T: 'a + DataType> {
    /// A boxed data structure that implemented `DataIndex`.
    Owned(Box<DataIndex<T> + 'a>),
    /// A reference to a data structure that implements `DataIndex`.
    Ref(&'a DataIndex<T>)
}
impl<'a, T: 'a + DataType> OwnedOrRef<'a, T> {
    /// Returns a reference to the underlying `DataIndex`, whether this `OwnedOrRef` owns the data
    /// or simply possesses a reference to it.
    pub fn as_ref(&'a self) -> &'a DataIndex<T> {
        match *self {
            OwnedOrRef::Owned(ref data) => data.as_ref(),
            OwnedOrRef::Ref(data) => data,
        }
    }
}
impl<'a, T: 'a + DataType> DataIndex<T> for OwnedOrRef<'a, T> {
    fn get_data(&self, idx: usize) -> Result<MaybeNa<&T>> {
        match *self {
            OwnedOrRef::Owned(ref data) => data.get_data(idx),
            OwnedOrRef::Ref(ref data) => data.get_data(idx),
        }
    }
    fn len(&self) -> usize {
        match *self {
            OwnedOrRef::Owned(ref data) => data.len(),
            OwnedOrRef::Ref(ref data) => data.len(),
        }
    }
}

/// A generic structure to hold either an owned or reference structure which implements `DataIndex`,
/// of any of the accepted agnes types.
pub enum ReduceDataIndex<'a> {
    /// An unsigned data structure implementing `DataIndex`.
    Unsigned(OwnedOrRef<'a, u64>),
    /// An signed data structure implementing `DataIndex`.
    Signed(OwnedOrRef<'a, i64>),
    /// An text data structure implementing `DataIndex`.
    Text(OwnedOrRef<'a, String>),
    /// An boolean data structure implementing `DataIndex`.
    Boolean(OwnedOrRef<'a, bool>),
    /// An floating-point data structure implementing `DataIndex`.
    Float(OwnedOrRef<'a, f64>),
}
impl<'a> ReduceDataIndex<'a> {
    /// Returns the length of this indexable data structure.
    pub fn len(&self) -> usize {
        match *self {
            ReduceDataIndex::Unsigned(ref di) => di.len(),
            ReduceDataIndex::Signed(ref di) => di.len(),
            ReduceDataIndex::Text(ref di) => di.len(),
            ReduceDataIndex::Boolean(ref di) => di.len(),
            ReduceDataIndex::Float(ref di) => di.len(),
        }
    }
    /// Returns a structure that refers to a specific element in this indexable data structure.
    pub fn get_datum(&'a self, idx: usize) -> ReduceDatum<'a> {
        ReduceDatum {
            rdi: self,
            idx,
        }
    }
}

/// Structure that refers to a specific element within a `ReduceDataIndex` data structure.
pub struct ReduceDatum<'a> {
    rdi: &'a ReduceDataIndex<'a>,
    idx: usize
}
impl<'a> PartialEq for ReduceDatum<'a> {
    fn eq(&self, other: &ReduceDatum) -> bool {
        match (self.rdi, other.rdi) {
            (&ReduceDataIndex::Unsigned(ref di1), &ReduceDataIndex::Unsigned(ref di2)) => {
                di1.get_data(self.idx).expect("invalid idx")
                    .eq(&di2.get_data(self.idx).expect("invalid idx"))
            },
            (&ReduceDataIndex::Signed(ref di1), &ReduceDataIndex::Signed(ref di2)) => {
                di1.get_data(self.idx).expect("invalid idx")
                    .eq(&di2.get_data(self.idx).expect("invalid idx"))
            },
            (&ReduceDataIndex::Text(ref di1), &ReduceDataIndex::Text(ref di2)) => {
                di1.get_data(self.idx).expect("invalid idx")
                    .eq(&di2.get_data(self.idx).expect("invalid idx"))
            },
            (&ReduceDataIndex::Boolean(ref di1), &ReduceDataIndex::Boolean(ref di2)) => {
                di1.get_data(self.idx).expect("invalid idx")
                    .eq(&di2.get_data(self.idx).expect("invalid idx"))
            },
            (&ReduceDataIndex::Float(ref di1), &ReduceDataIndex::Float(ref di2)) => {
                di1.get_data(self.idx).expect("invalid idx")
                    .eq(&di2.get_data(self.idx).expect("invalid idx"))
            },
            _ => false // non-matching types
        }
    }
}
impl<'a> Eq for ReduceDatum<'a> {}
impl<'a> Hash for ReduceDatum<'a> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match *self.rdi {
            ReduceDataIndex::Unsigned(ref di) => {
                di.get_data(self.idx).expect("invalid idx").hash(state)
            },
            ReduceDataIndex::Signed(ref di) => {
                di.get_data(self.idx).expect("invalid idx").hash(state)
            },
            ReduceDataIndex::Text(ref di) => {
                di.get_data(self.idx).expect("invalid idx").hash(state)
            },
            ReduceDataIndex::Boolean(ref di) => {
                di.get_data(self.idx).expect("invalid idx").hash(state)
            },
            ReduceDataIndex::Float(ref di) => {
                di.get_data(self.idx).expect("invalid idx").hash(state)
            },
        }
    }
}

/// Type for accessing a specified field (identified by a `FieldIdent`) for an underlying data
/// structure.
#[derive(Debug, Clone)]
pub struct Selection<'a, 'b, D: 'a + ?Sized> {
    /// Underlying data structure for this selection. Contains the field identified by `ident`.
    pub data: &'a D,
    /// Identifier of the field within the `data` structure.
    pub ident: &'b FieldIdent,
}

impl<'a, 'b, D: 'a + ApplyTo> Apply for Selection<'a, 'b, D> {
    fn apply<F: MapFn>(&self, f: &mut F) -> Result<Vec<F::Output>> {
        self.data.apply_to(f, &self.ident)
    }
}

impl<'a, 'b, D> Selection<'a, 'b, D> {
    /// Create a new `Selection` object from specified data and identifier.
    pub fn new(data: &'a D, ident: &'b FieldIdent) -> Selection<'a, 'b, D> {
        Selection {
            data,
            ident: ident
        }
    }
}
impl<'a, 'b, D: ApplyTo> Selection<'a, 'b, D> {
    /// Apply a `MapFn` to this selection (to be lazy evaluated).
    pub fn map<F: MapFn>(&self, f: F) -> Map<Self, F> {
        Map::new(self, f, None)
    }
}

/// Trait for types that can have a specific field selected (for applying `MapFn`s).
pub trait Select {
    /// Select the specified field.
    fn select<'a, 'b>(&'a self, ident: &'b FieldIdent) -> Selection<'a, 'b, Self>;
}

impl<T> Select for T {
    fn select<'a, 'b>(&'a self, ident: &'b FieldIdent)
        -> Selection<'a, 'b, Self>
    {
        Selection::new(self, ident)
    }
}
