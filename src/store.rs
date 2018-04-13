//! Data storage struct and implentation.

use std::cmp::max;
use std::collections::HashMap;
use std::hash::Hash;

use field::{FieldIdent, TypedFieldIdent, DsField, FieldType};
use masked::{MaskedData};
use error::*;
use MaybeNa;
use apply::*;

type TypeData<T> = HashMap<FieldIdent, MaskedData<T>>;

/// Data storage underlying a dataframe. Data is retrievable both by index (of the fields vector)
/// and by field name.
#[derive(Debug, Clone)]
pub struct DataStore {
    /// List of fields within the data store
    pub fields: Vec<DsField>,
    /// Map of field names to index of the fields vector
    pub field_map: HashMap<FieldIdent, usize>,

    /// Storage for unsigned integers
    unsigned: TypeData<u64>,
    /// Storage for signed integers
    signed: TypeData<i64>,
    /// Storage for strings
    text: TypeData<String>,
    /// Storage for booleans
    boolean: TypeData<bool>,
    /// Storage for floating-point numbers
    float: TypeData<f64>,
}
impl DataStore {
    /// Generate and return an empty data store
    pub fn empty() -> DataStore {
        DataStore {
            fields: Vec::new(),
            field_map: HashMap::new(),

            unsigned: HashMap::new(),
            signed: HashMap::new(),
            text: HashMap::new(),
            boolean: HashMap::new(),
            float: HashMap::new(),
        }
    }

    fn add_field(&mut self, field: TypedFieldIdent) {
        let ident = field.ident.clone();
        if !self.field_map.contains_key(&ident) {
            let index = self.fields.len();
            self.fields.push(DsField::from_typed_field_ident(field, index));
            self.field_map.insert(ident, index);
        }
    }

    /// Create a new `DataStore` which will contain the provided fields.
    pub fn with_fields(mut fields: Vec<TypedFieldIdent>) -> DataStore {
        let mut ds = DataStore {
            fields: Vec::with_capacity(fields.len()),
            field_map: HashMap::with_capacity(fields.len()),

            // could precompute lengths here to guess capacity, not sure if it'd be necessarily
            // faster
            unsigned: HashMap::new(),
            signed: HashMap::new(),
            text: HashMap::new(),
            boolean: HashMap::new(),
            float: HashMap::new(),
        };
        for field in fields.drain(..) {
            ds.add_field(field);
        }
        ds
    }

    /// Create a new `DataStore` with provided data. Data is provided in type-specific vectors of
    /// field identifiers along with data for the identifier.
    ///
    /// NOTE: This function provides no protection against field name collisions.
    pub fn with_data<FI, U, S, T, B, F>(
        unsigned: U, signed: S, text: T, boolean: B, float: F
        ) -> DataStore
        where FI: Into<FieldIdent>,
              U: Into<Option<Vec<(FI, MaskedData<u64>)>>>,
              S: Into<Option<Vec<(FI, MaskedData<i64>)>>>,
              T: Into<Option<Vec<(FI, MaskedData<String>)>>>,
              B: Into<Option<Vec<(FI, MaskedData<bool>)>>>,
              F: Into<Option<Vec<(FI, MaskedData<f64>)>>>,
    {
        let mut ds = DataStore::empty();
        macro_rules! add_to_ds {
            ($($hm:tt; $fty:path)*) => {$({
                if let Some(src_h) = $hm.into() {
                    for (ident, data) in src_h {
                        let ident: FieldIdent = ident.into();
                        ds.add_field(TypedFieldIdent { ident: ident.clone(), ty: $fty });
                        ds.$hm.insert(ident, data.into());
                    }
                }
            })*}
        }
        add_to_ds!(
            unsigned; FieldType::Unsigned
            signed;   FieldType::Signed
            text;     FieldType::Text
            boolean;  FieldType::Boolean
            float;    FieldType::Float
        );
        ds
    }

    /// Insert a value (provided in unparsed string form) for specified field
    pub fn insert(&mut self, ty_ident: TypedFieldIdent, value_str: String) -> Result<()> {
        let ident = ty_ident.ident.clone();
        let fty = ty_ident.ty;
        self.add_field(ty_ident.clone());
        Ok(match fty {
            FieldType::Unsigned => self.add(ident, parse(value_str, parse_unsigned)?),
            FieldType::Signed   => self.add(ident, parse(value_str, parse_signed)?),
            FieldType::Text     => self.add(ident, parse(value_str, |val| Ok(val))?),
            FieldType::Boolean  => self.add(ident, parse(value_str,
                |val| Ok(val.parse::<bool>()?))?),
            FieldType::Float    => self.add(ident, parse(value_str, |val| Ok(val.parse::<f64>()?))?)
        })
    }

    // Retrieve an unsigned integer field
    pub(crate) fn get_unsigned_field(&self, ident: &FieldIdent) -> Option<&MaskedData<u64>> {
        self.unsigned.get(ident)
    }
    // Retrieve a signed integer field
    pub(crate) fn get_signed_field(&self, ident: &FieldIdent) -> Option<&MaskedData<i64>> {
        self.signed.get(ident)
    }
    // Retrieve a string field
    pub(crate) fn get_text_field(&self, ident: &FieldIdent) -> Option<&MaskedData<String>> {
        self.text.get(ident)
    }
    // Retrieve a boolean field
    pub(crate) fn get_boolean_field(&self, ident: &FieldIdent) -> Option<&MaskedData<bool>> {
        self.boolean.get(ident)
    }
    // Retrieve a floating-point field
    pub(crate) fn get_float_field(&self, ident: &FieldIdent) -> Option<&MaskedData<f64>> {
        self.float.get(ident)
    }
    // /// Get all the data for a field, returned within the `FieldData` common data enum. Returns
    // /// `None` if the specified `FieldIdent` object does not exist.
    // pub fn get_field_data(&self, ident: &FieldIdent) -> Option<FieldData> {
    //     self.field_map.get(ident).and_then(|&idx| {
    //         match self.fields[idx].ty_ident.ty {
                // FieldType::Unsigned => self.get_unsigned_field(ident).map(
                //     |f| FieldData::Unsigned(f)
                // ),
                // FieldType::Signed => self.get_signed_field(ident).map(
                //     |f| FieldData::Signed(f)
                // ),
                // FieldType::Text => self.get_text_field(ident).map(
                //     |f| FieldData::Text(f)
                // ),
                // FieldType::Boolean => self.get_boolean_field(ident).map(
                //     |f| FieldData::Boolean(f)
                // ),
                // FieldType::Float => self.get_float_field(ident).map(
                //     |f| FieldData::Float(f)
                // ),
    //         }
    //     })
    // }
    pub fn has_field(&self, ident: &FieldIdent) -> bool {
        self.field_map.contains_key(ident)
    }

    /// Get the field information struct for a given field name
    pub fn get_field_type(&self, ident: &FieldIdent) -> Option<FieldType> {
        self.field_map.get(ident)
            .and_then(|&index| self.fields.get(index).map(|&ref dsfield| dsfield.ty_ident.ty))
    }

    /// Get the list of field information structs for this data store
    pub fn fields(&self) -> Vec<&TypedFieldIdent> {
        self.fields.iter().map(|&ref s| &s.ty_ident).collect()
    }
    /// Get the field names in this data store
    pub fn fieldnames(&self) -> Vec<String> {
        self.fields.iter().map(|ref fi| fi.ty_ident.ident.to_string()).collect()
    }

    /// Check if datastore is "homogenous": all columns (regardless of field type) are the same
    /// length
    pub fn is_homogeneous(&self) -> bool {
        is_hm_homogeneous(&self.unsigned)
            .and_then(|x| is_hm_homogeneous_with(&self.signed, x))
            .and_then(|x| is_hm_homogeneous_with(&self.text, x))
            .and_then(|x| is_hm_homogeneous_with(&self.boolean, x))
            .and_then(|x| is_hm_homogeneous_with(&self.float, x))
            .is_some()
    }
    /// Retrieve number of rows for this data store
    pub fn nrows(&self) -> usize {
        [max_len(&self.unsigned), max_len(&self.signed), max_len(&self.text),
            max_len(&self.boolean), max_len(&self.float)].iter().fold(0, |acc, l| max(acc, *l))
    }
}
impl Default for DataStore {
    fn default() -> DataStore {
        DataStore::empty()
    }
}

impl<'a> ApplyToElem<FieldIndexSelector<'a>> for DataStore {
    fn apply_to_elem<F: ElemFn>(&self, f: F, select: FieldIndexSelector) -> Option<F::Output> {
        let (ident, idx) = select.index();
        self.field_map.get(ident).and_then(|&field_idx| {
            match self.fields[field_idx].ty_ident.ty {
                FieldType::Unsigned => self.get_unsigned_field(ident).and_then(|data| {
                    data.apply_to_elem(f, IndexSelector(idx))
                }),
                FieldType::Signed => self.get_signed_field(ident).and_then(|data| {
                    data.apply_to_elem(f, IndexSelector(idx))
                }),
                FieldType::Text => self.get_text_field(ident).and_then(|data| {
                    data.apply_to_elem(f, IndexSelector(idx))
                }),
                FieldType::Boolean => self.get_boolean_field(ident).and_then(|data| {
                    data.apply_to_elem(f, IndexSelector(idx))
                }),
                FieldType::Float => self.get_float_field(ident).and_then(|data| {
                    data.apply_to_elem(f, IndexSelector(idx))
                })
            }
        })
        // self.get(select.index()).map(|value| f.apply_unsigned(value))
    }
}

impl<'a> ApplyToField<FieldSelector<'a>> for DataStore {
    fn apply_to_field<F: FieldFn>(&self, f: F, select: FieldSelector) -> Option<F::Output> {
        let ident = select.index();
        self.field_map.get(ident).and_then(|&field_idx| {
            match self.fields[field_idx].ty_ident.ty {
                FieldType::Unsigned => self.get_unsigned_field(ident).and_then(|data| {
                    data.apply_to_field(f, NilSelector)
                }),
                FieldType::Signed => self.get_signed_field(ident).and_then(|data| {
                    data.apply_to_field(f, NilSelector)
                }),
                FieldType::Text => self.get_text_field(ident).and_then(|data| {
                    data.apply_to_field(f, NilSelector)
                }),
                FieldType::Boolean => self.get_boolean_field(ident).and_then(|data| {
                    data.apply_to_field(f, NilSelector)
                }),
                FieldType::Float => self.get_float_field(ident).and_then(|data| {
                    data.apply_to_field(f, NilSelector)
                }),
            }
        })
    }
}

impl<'a, 'b, 'c> ApplyToField2<FieldSelector<'a>> for (&'b DataStore, &'c DataStore) {
    fn apply_to_field2<T: Field2Fn>(&self, f: T, select: (FieldSelector, FieldSelector))
        -> Option<T::Output>
    {
        let (ident0, ident1) = (select.0.index(), select.1.index());
        let (field0, field1) = (
            &self.0.field_map.get(ident0).map(|&field_idx| &self.0.fields[field_idx]),
            &self.1.field_map.get(ident1).map(|&field_idx| &self.1.fields[field_idx]),
        );
        println!("{}:{} {}:{}", ident0, field0.is_some(), ident1, field1.is_some());
        let (field0, field1) = match (field0, field1) {
            (&Some(ref field0), &Some(ref field1)) => (field0, field1),
            _ => { return None; }
        };
        match (field0.ty_ident.ty, field1.ty_ident.ty) {
            (FieldType::Unsigned, FieldType::Unsigned) => (
                self.0.get_unsigned_field(ident0).unwrap(),
                self.1.get_unsigned_field(ident1).unwrap()
            ).apply_to_field2(f, (NilSelector, NilSelector)),
            (FieldType::Signed, FieldType::Signed) => (
                self.0.get_signed_field(ident0).unwrap(),
                self.1.get_signed_field(ident1).unwrap()
            ).apply_to_field2(f, (NilSelector, NilSelector)),
            (FieldType::Text, FieldType::Text) => (
                self.0.get_text_field(ident0).unwrap(),
                self.1.get_text_field(ident1).unwrap()
            ).apply_to_field2(f, (NilSelector, NilSelector)),
            (FieldType::Boolean, FieldType::Boolean) => (
                self.0.get_boolean_field(ident0).unwrap(),
                self.1.get_boolean_field(ident1).unwrap()
            ).apply_to_field2(f, (NilSelector, NilSelector)),
            (FieldType::Float, FieldType::Float) => (
                self.0.get_float_field(ident0).unwrap(),
                self.1.get_float_field(ident1).unwrap()
            ).apply_to_field2(f, (NilSelector, NilSelector)),
            (_,_) => None
        }
    }
}
// impl ApplyToFieldElem for DataStore {
//     fn apply_to_field_elem<T: ElemFn>(&self, f: T, ident: &FieldIdent, idx: usize)
//         -> Option<T::Output>
//     {
//         self.field_map.get(ident).and_then(|&idx| {
//             match self.fields[idx].ty_ident.ty {
//                 FieldType::Unsigned => self.get_unsigned_field(ident).and_then(
//                     |data| data.apply_to_elem(f, idx)
//                 ),
//                 FieldType::Signed => self.get_signed_field(ident).and_then(
//                     |data| data.apply_to_elem(f, idx)
//                 ),
//                 FieldType::Text => self.get_text_field(ident).and_then(
//                     |data| data.apply_to_elem(f, idx)
//                 ),
//                 FieldType::Boolean => self.get_boolean_field(ident).and_then(
//                     |data| data.apply_to_elem(f, idx)
//                 ),
//                 FieldType::Float => self.get_float_field(ident).and_then(
//                     |data| data.apply_to_elem(f, idx)
//                 ),
//             }
//         })
//     }
// }


/// Trait for adding data (of valid types) to a `DataStore`.
pub trait AddData<T: PartialOrd> {
    /// Add a single value to the specified field.
    fn add(&mut self, ident: FieldIdent, value: MaybeNa<T>);
}
impl AddData<u64> for DataStore {
    fn add(&mut self, ident: FieldIdent, value: MaybeNa<u64>) {
        insert_value(&mut self.unsigned, ident, value);
    }
}
impl AddData<i64> for DataStore {
    fn add(&mut self, ident: FieldIdent, value: MaybeNa<i64>) {
        insert_value(&mut self.signed, ident, value);
    }
}
impl AddData<String> for DataStore {
    fn add(&mut self, ident: FieldIdent, value: MaybeNa<String>) {
        insert_value(&mut self.text, ident, value);
    }
}
impl AddData<bool> for DataStore {
    fn add(&mut self, ident: FieldIdent, value: MaybeNa<bool>) {
        insert_value(&mut self.boolean, ident, value);
    }
}
impl AddData<f64> for DataStore {
    fn add(&mut self, ident: FieldIdent, value: MaybeNa<f64>) {
        insert_value(&mut self.float, ident, value);
    }
}

fn max_len<K, T: PartialOrd>(h: &HashMap<K, MaskedData<T>>) -> usize where K: Eq + Hash {
    h.values().fold(0, |acc, v| max(acc, v.len()))
}
fn is_hm_homogeneous<K, T: PartialOrd>(h: &HashMap<K, MaskedData<T>>) -> Option<usize>
    where K: Eq + Hash
{
    let mut all_same_len = true;
    let mut target_len = 0;
    let mut first = true;
    for (_, v) in h {
        if first {
            target_len = v.len();
            first = false;
        }
        all_same_len &= v.len() == target_len;
    }
    if all_same_len { Some(target_len) } else { None }
}
fn is_hm_homogeneous_with<K, T: PartialOrd>(h: &HashMap<K, MaskedData<T>>, value: usize)
    -> Option<usize> where K: Eq + Hash
{
    is_hm_homogeneous(h).and_then(|x| {
        if x == 0 && value != 0 {
            Some(value)
        } else if (value == 0 && x != 0) || x == value {
            Some(x)
        } else { None }
    })
}
fn insert_value<T: Default + PartialOrd>(
    h: &mut HashMap<FieldIdent, MaskedData<T>>,
    k: FieldIdent,
    v: MaybeNa<T>)
{
    h.entry(k).or_insert(MaskedData::new()).push(v);
}
fn parse<T: PartialOrd, F>(value_str: String, f: F) -> Result<MaybeNa<T>> where F: Fn(String)
    -> Result<T>
{
    if value_str.trim().len() == 0 {
        Ok(MaybeNa::Na)
    } else {
        Ok(MaybeNa::Exists(f(value_str)?))
    }
}
/// A forgiving unsigned integer parser. If normal unsigned integer parsing fails, tries to parse
/// as a signed integer; if successful, assumes that the integer is negative and translates that
/// to '0'. If that fails, tries to parse as a float; if successful, converts to unsigned integer
/// (or '0' if negative)
fn parse_unsigned(value_str: String) -> Result<u64> {
    Ok(value_str.parse::<u64>().or_else(|e| {
        // try parsing as a signed int...if successful, it's negative, so just set it to 0
        value_str.parse::<i64>().map(|_| 0u64).or_else(|_| {
            // try parsing as a float
            value_str.parse::<f64>().map(|f| {
                if f < 0.0 { 0u64 } else { f as u64 }
            }).or(Err(e))
        })
    })?)
}
/// A forgiving signed integer parser. If normal signed integer parsing fails, tries to parse as
/// a float; if successful, converts to a signed integer.
fn parse_signed(value_str: String) -> Result<i64> {
    Ok(value_str.parse::<i64>().or_else(|e| {
        // try parsing as float
        value_str.parse::<f64>().map(|f| f as i64).or(Err(e))
    })?)
}
