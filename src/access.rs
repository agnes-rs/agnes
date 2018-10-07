/*!
Traits for accessing data within agnes data structures. Includes `DataIndex` for index-based access
and `DataIterator` for iterator access.
*/
use std::fmt::Debug;
use std::marker::PhantomData;

use error::*;
use field::Value;
use data_types::{DTypeList, DataType};

/// Trait implemented by data structures that represent a single column / vector / field of data.
pub trait DataIndex<DTypes>: Debug
    where DTypes: DTypeList,
{
    type DType: DataType<DTypes>;

    /// Returns the data (possibly NA) at the specified index, if it exists.
    fn get_datum(&self, idx: usize) -> Result<Value<&Self::DType>>;
    /// Returns the length of this data field.
    fn len(&self) -> usize;
    fn iter<'a>(&'a self) -> DataIterator<'a, DTypes, Self::DType> where Self: Sized {
        DataIterator::new(self)
    }
}
pub trait DataIndexMut<DTypes>: DataIndex<DTypes>
    where DTypes: DTypeList
{
    // fn set_datum(&mut self, idx: usize, value: Value<Self::DType>) -> Result<()>;
    fn push(&mut self, value: Value<Self::DType>);
}

// impl<'a, DTypes, U> IntoIterator for &'a U
//     where U: DataIndex<DTypes>,
//           DTypes: DTypeList
// {
//     type Item = U::DType;
//     type IntoIter = DataIterator<'a, DTypes, U::DType>;

//     fn into_iter(self) -> DataIterator<'a, DTypes, U::DType> {
//         DataIterator::new(self)
//     }
// }

// impl<'a, T: DataType> DataIndex for Vec<Value<&'a T>> {
//     type Output = T;

//     fn get_datum(&self, idx: usize) -> Result<Value<&T>> {
//         Ok(self[idx].clone())
//     }
//     fn len(&self) -> usize {
//         self.len()
//     }
// }
// impl<DTypes, T: DataType<DTypes>> DataIndex<DTypes> for Vec<Value<T>> {
//     type DType = T;

//     fn get_datum(&self, idx: usize) -> Result<Value<&T>> {
//         if idx >= self.len() {
//             Err(AgnesError::IndexError { index: idx, len: self.len() })
//         } else {
//             Ok(self[idx].as_ref())
//         }
//     }
//     fn len(&self) -> usize {
//         self.len()
//     }
// }
// impl<DTypes, T: DataType<DTypes>> DataIndexMut<DTypes> for Vec<Value<T>> {

//     // fn set_datum(&mut self, idx: usize, value: Value<T>) -> Result<()> {
//     //     if idx >= self.len() {
//     //         Err(AgnesError::IndexError { index: idx, len: self.len() })
//     //     } else {
//     //         self[idx] = value;
//     //         Ok(())
//     //     }
//     // }
//     fn push(&mut self, value: Value<Self::DType>) {
//         self.push(value);
//     }
// }
// impl<DTypes, T: DataType<DTypes>> DataIndex<DTypes> for Vec<T> {
//     type DType = T;

//     fn get_datum(&self, idx: usize) -> Result<Value<&T>> {
//         if idx >= self.len() {
//             Err(AgnesError::IndexError { index: idx, len: self.len() })
//         } else {
//             Ok(Value::Exists(&self[idx]))
//         }
//     }
//     fn len(&self) -> usize {
//         self.len()
//     }
// }
// impl<DTypes, T: DataType<DTypes>> DataIndexMut<DTypes> for Vec<T> {

//     // fn set_datum(&mut self, idx: usize, value: Value<T>) -> Result<()> {
//     //     if idx >= self.len() {
//     //         Err(AgnesError::IndexError { index: idx, len: self.len() })
//     //     } else {
//     //         match value {
//     //             Value::Exists(value) => {
//     //                 self[idx] = value;
//     //                 Ok(())
//     //             },
//     //             Value::Na => Err(AgnesError::IncompatibleTypes {
//     //                 expected: "Value::Exists(<value>)".to_string(),
//     //                 actual: "Value::Na".to_string()
//     //             })
//     //         }
//     //     }
//     // }
//     fn push(&mut self, value: Value<Self::DType>) {
//         match value {
//             Value::Exists(value) => {
//                 self.push(value);
//             },
//             Value::Na => {
//                 panic!["Attempt to push an NA value into a non-NA-holding data structure"]
//             }
//         }
//     }
// }

/// Iterator over the data in a data structure that implement DataIndex.
pub struct DataIterator<'a, DTypes, T>
    where DTypes: 'a + DTypeList,
          T: 'a + DataType<DTypes>
{
    data: &'a dyn DataIndex<DTypes, DType=T>,
    cur_idx: usize,
    phantom: PhantomData<T>
}
impl<'a, DTypes, T> DataIterator<'a, DTypes, T>
    where DTypes: DTypeList,
          T: 'a + DataType<DTypes>,
{
    /// Create a new `DataIterator` from a type that implements `DataIndex`.
    pub fn new(data: &'a dyn DataIndex<DTypes, DType=T>) -> DataIterator<'a, DTypes, T> {
        DataIterator {
            data,
            cur_idx: 0,
            phantom: PhantomData
        }
    }
}
impl<'a, DTypes, T> Iterator for DataIterator<'a, DTypes, T>
    where DTypes: DTypeList,
          T: 'a + DataType<DTypes>,
{
    type Item = Value<&'a T>;

    fn next(&mut self) -> Option<Value<&'a T>> {
        if self.cur_idx < self.data.len() {
            let out = Some(self.data.get_datum(self.cur_idx).unwrap());
            self.cur_idx += 1;
            out
        } else {
            None
        }
    }
}

/// Either an owned data structure or reference to a data structure that implements `DataIndex`.
#[derive(Debug)]
pub enum OwnedOrRef<'a, DTypes, T>
    where DTypes: 'a + DTypeList,
          T: 'a + DataType<DTypes>
{
    /// A boxed data structure that implemented `DataIndex`.
    Owned(Box<dyn DataIndexMut<DTypes, DType=T> + 'a>),
    /// A reference to a data structure that implements `DataIndex`.
    Ref(&'a dyn DataIndex<DTypes, DType=T>),
    Mut(&'a mut dyn DataIndexMut<DTypes, DType=T>)
}
// macro_rules! oor_apply {
//     ($self:ident.data.$($t:tt)*) => {
//         match $self {
//             &OwnedOrRef::Owned(ref data) => data.$($t)*,
//             &OwnedOrRef::Ref(data) => data.$($t)*,
//             &OwnedOrRef::Mut(ref data) => data.$($t)*,
//         }
//     }
// }
// impl<'a, T: 'a + DataType> OwnedOrRef<'a, T> {
//     /// Returns a reference to the underlying `DataIndex`, whether this `OwnedOrRef` owns the data
//     /// or simply possesses a reference to it.
//     pub fn as_ref(&self) -> &dyn DataIndex<DType=T> {
        // match self {
        //     &OwnedOrRef::Owned(ref data) => data.as_ref(),
        //     &OwnedOrRef::Ref(data) => data,
        //     &OwnedOrRef::Mut(ref data) => &**data,
        // }
//     }
//     pub fn as_mut(&mut self) -> &mut dyn DataIndexMut<DType=T> {
//         match *self {
//             OwnedOrRef::Owned(ref mut data) => data.as_mut(),
//             OwnedOrRef::Ref(_) => panic!["as_mut() called on OwnedOrRef 'Ref' variant"],
//             OwnedOrRef::Mut(ref mut data) => &mut **data,
//         }
//     }
// }
impl<'a, DTypes, T> DataIndex<DTypes> for OwnedOrRef<'a, DTypes, T>
    where DTypes: DTypeList,
          T: 'a + DataType<DTypes>
{
    type DType = T;

    fn get_datum(&self, idx: usize) -> Result<Value<&T>> {
        // self.as_ref().get_datum(idx)
        // oor_apply!(self.data.get_datum(idx))
        match *self {
            OwnedOrRef::Owned(ref data) => data.get_datum(idx),
            OwnedOrRef::Ref(ref data) => data.get_datum(idx),
            OwnedOrRef::Mut(ref data) => data.get_datum(idx),
        }
    }
    fn len(&self) -> usize {
        // self.as_ref().len()
        // oor_apply!(self.data.len())
        match *self {
            OwnedOrRef::Owned(ref data) => data.len(),
            OwnedOrRef::Ref(ref data) => data.len(),
            OwnedOrRef::Mut(ref data) => data.len(),
        }
    }
}
impl<'a, DTypes, T> DataIndexMut<DTypes> for OwnedOrRef<'a, DTypes, T>
    where DTypes: DTypeList,
          T: 'a + DataType<DTypes>
{

    // fn set_datum(&mut self, idx: usize, value: Value<T>) -> Result<()> {
    //     self.as_mut().set_datum(idx, value)
    //     // match *self {
    //     //     OwnedOrRef::Owned(ref mut data) => data.set_datum(idx, value),
    //     //     OwnedOrRef::Ref(ref mut data) => data.set_datum(idx, value)
    //     // }
    // }
    fn push(&mut self, value: Value<T>) {
        // self.as_mut().push(value)
        // oor_apply!(self.data.push(value))
        match *self {
            OwnedOrRef::Owned(ref mut data) => data.push(value),
            OwnedOrRef::Ref(_) => panic!["push() called on OwnedOrRef 'Ref' variant"],
            OwnedOrRef::Mut(ref mut data) => data.push(value),
        }
    }
}

// /// A generic structure to hold either an owned or reference structure which implements `DataIndex`,
// /// of any of the accepted agnes types.
// pub enum FieldData<'a> {
//     /// An unsigned data structure implementing `DataIndex`.
//     Unsigned(OwnedOrRef<'a, u64>),
//     /// An signed data structure implementing `DataIndex`.
//     Signed(OwnedOrRef<'a, i64>),
//     /// An text data structure implementing `DataIndex`.
//     Text(OwnedOrRef<'a, String>),
//     /// An boolean data structure implementing `DataIndex`.
//     Boolean(OwnedOrRef<'a, bool>),
//     /// An floating-point data structure implementing `DataIndex`.
//     Float(OwnedOrRef<'a, f64>),
// }
// impl<'a> FieldData<'a> {
//     /// Returns the length of this indexable data structure.
//     pub fn len(&self) -> usize {
//         match *self {
//             FieldData::Unsigned(ref di) => di.len(),
//             FieldData::Signed(ref di) => di.len(),
//             FieldData::Text(ref di) => di.len(),
//             FieldData::Boolean(ref di) => di.len(),
//             FieldData::Float(ref di) => di.len(),
//         }
//     }
//     /// Returns a structure that refers to a specific element in this indexable data structure.
//     pub fn get_datum(&'a self, idx: usize) -> FieldDatum<'a> {
//         FieldDatum {
//             rdi: self,
//             idx,
//         }
//     }

//     // /// Returns a `DataIterator` iterator over the elements of this `FieldData` object.
//     // pub fn data_iter<T: 'a + DataType>(&'a self) -> DataIterator<'a, T, OwnedOrRef<'a, T>>
//     //     where Self: DIter<'a, T, DI=OwnedOrRef<'a, T>>
//     // {
//     //     (self as &dyn DIter<'a, T, DI=OwnedOrRef<'a, T>>).diter().unwrap()
//     // }
// }


// /// Trait for providing a way to generate a `DataIterator` over the appropriate type from an agnes
// /// data structure.
// pub trait DIter<'a, DTypes: DTypeList, T: 'a + DataType<DTypes>> {
//     /// Provides a `DataIterator` of the appropriate type `T` for this data structure.
//     fn diter(&'a self) -> Result<DataIterator<'a, DTypes, T>>;
// }
// impl<'a> DIter<'a, u64> for FieldData<'a> {
//     type DI = OwnedOrRef<'a, u64>;
//     fn diter(&'a self) -> DataIterator<'a, u64, OwnedOrRef<'a, u64>> {
//         match *self {
//             FieldData::Unsigned(ref data) => DataIterator::new(data),
//             _ => panic!("Invalid type: u64 iterator requested from non-unsigned integer field")
//         }
//     }
// }
// impl<'a> DIter<'a, i64> for FieldData<'a> {
//     type DI = OwnedOrRef<'a, i64>;
//     fn diter(&'a self) -> DataIterator<'a, i64, OwnedOrRef<'a, i64>> {
//         match *self {
//             FieldData::Signed(ref data) => DataIterator::new(data),
//             _ => panic!("Invalid type: i64 iterator requested from non-signed integer field")
//         }
//     }
// }
// impl<'a> DIter<'a, String> for FieldData<'a> {
//     type DI = OwnedOrRef<'a, String>;
//     fn diter(&'a self) -> DataIterator<'a, String, OwnedOrRef<'a, String>> {
//         match *self {
//             FieldData::Text(ref data) => DataIterator::new(data),
//             _ => panic!("Invalid type: String iterator requested from non-text field")
//         }
//     }
// }
// impl<'a> DIter<'a, bool> for FieldData<'a> {
//     type DI = OwnedOrRef<'a, bool>;
//     fn diter(&'a self) -> DataIterator<'a, bool, OwnedOrRef<'a, bool>> {
//         match *self {
//             FieldData::Boolean(ref data) => DataIterator::new(data),
//             _ => panic!("Invalid type: bool iterator requested from non-boolean field")
//         }
//     }
// }
// impl<'a> DIter<'a, f64> for FieldData<'a> {
//     type DI = OwnedOrRef<'a, f64>;
//     fn diter(&'a self) -> DataIterator<'a, f64, OwnedOrRef<'a, f64>> {
//         match *self {
//             FieldData::Float(ref data) => DataIterator::new(data),
//             _ => panic!("Invalid type: f64 iterator requested from non-floating point field")
//         }
//     }
// }


// impl<'a> From<OwnedOrRef<'a, u64>> for FieldData<'a> {
//     fn from(orig: OwnedOrRef<'a, u64>) -> FieldData<'a> {
//         FieldData::Unsigned(orig)
//     }
// }
// impl<'a> From<OwnedOrRef<'a, i64>> for FieldData<'a> {
//     fn from(orig: OwnedOrRef<'a, i64>) -> FieldData<'a> {
//         FieldData::Signed(orig)
//     }
// }
// impl<'a> From<OwnedOrRef<'a, String>> for FieldData<'a> {
//     fn from(orig: OwnedOrRef<'a, String>) -> FieldData<'a> {
//         FieldData::Text(orig)
//     }
// }
// impl<'a> From<OwnedOrRef<'a, bool>> for FieldData<'a> {
//     fn from(orig: OwnedOrRef<'a, bool>) -> FieldData<'a> {
//         FieldData::Boolean(orig)
//     }
// }
// impl<'a> From<OwnedOrRef<'a, f64>> for FieldData<'a> {
//     fn from(orig: OwnedOrRef<'a, f64>) -> FieldData<'a> {
//         FieldData::Float(orig)
//     }
// }

// /// Structure that refers to a specific element within a `FieldData` data structure.
// pub struct FieldDatum<'a> {
//     rdi: &'a FieldData<'a>,
//     idx: usize
// }
// impl<'a> PartialEq for FieldDatum<'a> {
//     fn eq(&self, other: &FieldDatum) -> bool {
//         match (self.rdi, other.rdi) {
//             (&FieldData::Unsigned(ref di1), &FieldData::Unsigned(ref di2)) => {
//                 di1.get_datum(self.idx).expect("invalid idx")
//                     .eq(&di2.get_datum(self.idx).expect("invalid idx"))
//             },
//             (&FieldData::Signed(ref di1), &FieldData::Signed(ref di2)) => {
//                 di1.get_datum(self.idx).expect("invalid idx")
//                     .eq(&di2.get_datum(self.idx).expect("invalid idx"))
//             },
//             (&FieldData::Text(ref di1), &FieldData::Text(ref di2)) => {
//                 di1.get_datum(self.idx).expect("invalid idx")
//                     .eq(&di2.get_datum(self.idx).expect("invalid idx"))
//             },
//             (&FieldData::Boolean(ref di1), &FieldData::Boolean(ref di2)) => {
//                 di1.get_datum(self.idx).expect("invalid idx")
//                     .eq(&di2.get_datum(self.idx).expect("invalid idx"))
//             },
//             (&FieldData::Float(ref di1), &FieldData::Float(ref di2)) => {
//                 di1.get_datum(self.idx).expect("invalid idx")
//                     .eq(&di2.get_datum(self.idx).expect("invalid idx"))
//             },
//             _ => false // non-matching types
//         }
//     }
// }
// impl<'a> Eq for FieldDatum<'a> {}
// impl<'a> Hash for FieldDatum<'a> {
//     fn hash<H: Hasher>(&self, state: &mut H) {
//         match *self.rdi {
//             FieldData::Unsigned(ref di) => {
//                 di.get_datum(self.idx).expect("invalid idx").hash(state)
//             },
//             FieldData::Signed(ref di) => {
//                 di.get_datum(self.idx).expect("invalid idx").hash(state)
//             },
//             FieldData::Text(ref di) => {
//                 di.get_datum(self.idx).expect("invalid idx").hash(state)
//             },
//             FieldData::Boolean(ref di) => {
//                 di.get_datum(self.idx).expect("invalid idx").hash(state)
//             },
//             FieldData::Float(ref di) => {
//                 di.get_datum(self.idx).expect("invalid idx").hash(state)
//             },
//         }
//     }
// }
