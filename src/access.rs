/*!
Traits for accessing data within agnes data structures. Includes `DataIndex` for index-based access
and `DataIterator` for iterator access.
*/
use std::marker::PhantomData;
use std::hash::{Hash, Hasher};

use field::DataType;
use masked::MaybeNa;
use error::*;

/// Trait implemented by data structures that represent a single column / vector / field of data.
pub trait DataIndex<T: DataType> {
    //TODO: change to get_datum (for accuracy)
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

/// Iterator over the data in a data structure that implement DataIndex.
pub struct DataIterator<'a, T: 'a + DataType, DI: 'a + DataIndex<T>> {
    data: &'a DI,
    cur_idx: usize,
    phantom: PhantomData<T>
}
impl<'a, T: 'a + DataType, DI: 'a + DataIndex<T>> DataIterator<'a, T, DI> {
    /// Create a new `DataIterator` from a type that implements `DataIndex`.
    pub fn new(data: &'a DI) -> DataIterator<'a, T, DI> {
        DataIterator {
            data,
            cur_idx: 0,
            phantom: PhantomData
        }
    }
}
impl<'a, T: 'a + DataType, DI: 'a + DataIndex<T>> Iterator for DataIterator<'a, T, DI> {
    type Item = MaybeNa<&'a T>;

    fn next(&mut self) -> Option<MaybeNa<&'a T>> {
        if self.cur_idx < self.data.len() {
            let out = Some(self.data.get_data(self.cur_idx).unwrap());
            self.cur_idx += 1;
            out
        } else {
            None
        }
    }
}

/// Either an owned data structure or reference to a data structure that implements `DataIndex`.
pub enum OwnedOrRef<'a, T: 'a + DataType> {
    /// A boxed data structure that implemented `DataIndex`.
    Owned(Box<dyn DataIndex<T> + 'a>),
    /// A reference to a data structure that implements `DataIndex`.
    Ref(&'a dyn DataIndex<T>)
}
impl<'a, T: 'a + DataType> OwnedOrRef<'a, T> {
    /// Returns a reference to the underlying `DataIndex`, whether this `OwnedOrRef` owns the data
    /// or simply possesses a reference to it.
    pub fn as_ref(&'a self) -> &'a dyn DataIndex<T> {
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
pub enum FieldData<'a> {
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
impl<'a> FieldData<'a> {
    /// Returns the length of this indexable data structure.
    pub fn len(&self) -> usize {
        match *self {
            FieldData::Unsigned(ref di) => di.len(),
            FieldData::Signed(ref di) => di.len(),
            FieldData::Text(ref di) => di.len(),
            FieldData::Boolean(ref di) => di.len(),
            FieldData::Float(ref di) => di.len(),
        }
    }
    /// Returns a structure that refers to a specific element in this indexable data structure.
    pub fn get_datum(&'a self, idx: usize) -> FieldDatum<'a> {
        FieldDatum {
            rdi: self,
            idx,
        }
    }

    /// Returns a `DataIterator` iterator over the elements of this `FieldData` object.
    pub fn data_iter<T: 'a + DataType>(&'a self) -> DataIterator<'a, T, OwnedOrRef<'a, T>>
        where Self: DIter<'a, T>
    {
        (self as &dyn DIter<'a, T>).diter()
    }
}


/// Trait for providing a way to generate a `DataIterator` over the appropriate type from an agnes
/// data structure.
pub trait DIter<'a, T: 'a + DataType> {
    /// Provides a `DataIterator` of the appropriate type `T` for this data structure.
    fn diter(&'a self) -> DataIterator<'a, T, OwnedOrRef<'a, T>>;
}
impl<'a> DIter<'a, u64> for FieldData<'a> {
    fn diter(&'a self) -> DataIterator<'a, u64, OwnedOrRef<'a, u64>> {
        match *self {
            FieldData::Unsigned(ref data) => DataIterator::new(data),
            _ => panic!("Invalid type: u64 iterator requested from non-unsigned integer field")
        }
    }
}
impl<'a> DIter<'a, i64> for FieldData<'a> {
    fn diter(&'a self) -> DataIterator<'a, i64, OwnedOrRef<'a, i64>> {
        match *self {
            FieldData::Signed(ref data) => DataIterator::new(data),
            _ => panic!("Invalid type: i64 iterator requested from non-signed integer field")
        }
    }
}
impl<'a> DIter<'a, String> for FieldData<'a> {
    fn diter(&'a self) -> DataIterator<'a, String, OwnedOrRef<'a, String>> {
        match *self {
            FieldData::Text(ref data) => DataIterator::new(data),
            _ => panic!("Invalid type: String iterator requested from non-text field")
        }
    }
}
impl<'a> DIter<'a, bool> for FieldData<'a> {
    fn diter(&'a self) -> DataIterator<'a, bool, OwnedOrRef<'a, bool>> {
        match *self {
            FieldData::Boolean(ref data) => DataIterator::new(data),
            _ => panic!("Invalid type: bool iterator requested from non-boolean field")
        }
    }
}
impl<'a> DIter<'a, f64> for FieldData<'a> {
    fn diter(&'a self) -> DataIterator<'a, f64, OwnedOrRef<'a, f64>> {
        match *self {
            FieldData::Float(ref data) => DataIterator::new(data),
            _ => panic!("Invalid type: f64 iterator requested from non-floating point field")
        }
    }
}


impl<'a> From<OwnedOrRef<'a, u64>> for FieldData<'a> {
    fn from(orig: OwnedOrRef<'a, u64>) -> FieldData<'a> {
        FieldData::Unsigned(orig)
    }
}
impl<'a> From<OwnedOrRef<'a, i64>> for FieldData<'a> {
    fn from(orig: OwnedOrRef<'a, i64>) -> FieldData<'a> {
        FieldData::Signed(orig)
    }
}
impl<'a> From<OwnedOrRef<'a, String>> for FieldData<'a> {
    fn from(orig: OwnedOrRef<'a, String>) -> FieldData<'a> {
        FieldData::Text(orig)
    }
}
impl<'a> From<OwnedOrRef<'a, bool>> for FieldData<'a> {
    fn from(orig: OwnedOrRef<'a, bool>) -> FieldData<'a> {
        FieldData::Boolean(orig)
    }
}
impl<'a> From<OwnedOrRef<'a, f64>> for FieldData<'a> {
    fn from(orig: OwnedOrRef<'a, f64>) -> FieldData<'a> {
        FieldData::Float(orig)
    }
}

/// Structure that refers to a specific element within a `FieldData` data structure.
pub struct FieldDatum<'a> {
    rdi: &'a FieldData<'a>,
    idx: usize
}
impl<'a> PartialEq for FieldDatum<'a> {
    fn eq(&self, other: &FieldDatum) -> bool {
        match (self.rdi, other.rdi) {
            (&FieldData::Unsigned(ref di1), &FieldData::Unsigned(ref di2)) => {
                di1.get_data(self.idx).expect("invalid idx")
                    .eq(&di2.get_data(self.idx).expect("invalid idx"))
            },
            (&FieldData::Signed(ref di1), &FieldData::Signed(ref di2)) => {
                di1.get_data(self.idx).expect("invalid idx")
                    .eq(&di2.get_data(self.idx).expect("invalid idx"))
            },
            (&FieldData::Text(ref di1), &FieldData::Text(ref di2)) => {
                di1.get_data(self.idx).expect("invalid idx")
                    .eq(&di2.get_data(self.idx).expect("invalid idx"))
            },
            (&FieldData::Boolean(ref di1), &FieldData::Boolean(ref di2)) => {
                di1.get_data(self.idx).expect("invalid idx")
                    .eq(&di2.get_data(self.idx).expect("invalid idx"))
            },
            (&FieldData::Float(ref di1), &FieldData::Float(ref di2)) => {
                di1.get_data(self.idx).expect("invalid idx")
                    .eq(&di2.get_data(self.idx).expect("invalid idx"))
            },
            _ => false // non-matching types
        }
    }
}
impl<'a> Eq for FieldDatum<'a> {}
impl<'a> Hash for FieldDatum<'a> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match *self.rdi {
            FieldData::Unsigned(ref di) => {
                di.get_data(self.idx).expect("invalid idx").hash(state)
            },
            FieldData::Signed(ref di) => {
                di.get_data(self.idx).expect("invalid idx").hash(state)
            },
            FieldData::Text(ref di) => {
                di.get_data(self.idx).expect("invalid idx").hash(state)
            },
            FieldData::Boolean(ref di) => {
                di.get_data(self.idx).expect("invalid idx").hash(state)
            },
            FieldData::Float(ref di) => {
                di.get_data(self.idx).expect("invalid idx").hash(state)
            },
        }
    }
}
