//! Data storage struct and implentation.

use std::rc::Rc;
use std::cmp::max;
use std::collections::HashMap;
use std::hash::Hash;

use field::{FieldIdent, DataType, TypedFieldIdent, DsField, FieldType};
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
    /// Create a new `DataStore` from an interator of fields.
    pub fn with_field_iter<I: Iterator<Item=TypedFieldIdent>>(field_iter: I) -> DataStore {
        let mut ds = DataStore {
            fields: vec![],
            field_map: HashMap::new(),

            unsigned: HashMap::new(),
            signed: HashMap::new(),
            text: HashMap::new(),
            boolean: HashMap::new(),
            float: HashMap::new(),
        };
        for field in field_iter {
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

    /// Returns `true` if this `DataStore` contains this field.
    pub fn has_field(&self, ident: &FieldIdent) -> bool {
        self.field_map.contains_key(ident)
    }

    /// Get the field information struct for a given field name
    pub fn get_field_type(&self, ident: &FieldIdent) -> Option<FieldType> {
        self.field_map.get(ident)
            .and_then(|&index| self.fields.get(index).map(|&ref dsfield| dsfield.ty_ident.ty))
    }

    fn get_reduce_data_index(&self, ident: &FieldIdent) -> Option<ReduceDataIndex> {
        self.field_map.get(ident).and_then(|&field_idx| {
            match self.fields[field_idx].ty_ident.ty {
                FieldType::Unsigned => self.get_unsigned_field(ident)
                    .map(|data| ReduceDataIndex::Unsigned(OwnedOrRef::Ref(data))),
                FieldType::Signed => self.get_signed_field(ident)
                    .map(|data| ReduceDataIndex::Signed(OwnedOrRef::Ref(data))),
                FieldType::Text => self.get_text_field(ident)
                    .map(|data| ReduceDataIndex::Text(OwnedOrRef::Ref(data))),
                FieldType::Boolean => self.get_boolean_field(ident)
                    .map(|data| ReduceDataIndex::Boolean(OwnedOrRef::Ref(data))),
                FieldType::Float => self.get_float_field(ident)
                    .map(|data| ReduceDataIndex::Float(OwnedOrRef::Ref(data))),
            }
        })
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

impl ApplyToElem for DataStore {
    fn apply_to_elem<F: MapFn>(&self, f: &mut F, ident: &FieldIdent, idx: usize)
        -> Result<F::Output>
    {
        self.field_map.get(ident)
            .ok_or(AgnesError::FieldNotFound(ident.clone()))
            .and_then(|&field_idx| {
                match self.fields[field_idx].ty_ident.ty {
                    FieldType::Unsigned => self.get_unsigned_field(ident)
                        .ok_or(AgnesError::FieldNotFound(ident.clone()))
                        .and_then(|data| {
                            data.apply(f, idx)
                        }
                    ),
                    FieldType::Signed => self.get_signed_field(ident)
                        .ok_or(AgnesError::FieldNotFound(ident.clone()))
                        .and_then(|data| {
                            data.apply(f, idx)
                        }
                    ),
                    FieldType::Text => self.get_text_field(ident)
                        .ok_or(AgnesError::FieldNotFound(ident.clone()))
                        .and_then(|data| {
                            data.apply(f, idx)
                        }
                    ),
                    FieldType::Boolean => self.get_boolean_field(ident)
                        .ok_or(AgnesError::FieldNotFound(ident.clone()))
                        .and_then(|data| {
                            data.apply(f, idx)
                        }
                    ),
                    FieldType::Float => self.get_float_field(ident)
                        .ok_or(AgnesError::FieldNotFound(ident.clone()))
                        .and_then(|data| {
                            data.apply(f, idx)
                        }
                    )
                }
            }
        )
    }
}
impl FieldApplyTo for DataStore {
    fn field_apply_to<F: FieldMapFn>(&self, f: &mut F, ident: &FieldIdent)
        -> Result<F::Output>
    {
        self.field_map.get(ident)
            .ok_or(AgnesError::FieldNotFound(ident.clone()))
            .and_then(|&field_idx| {
                match self.fields[field_idx].ty_ident.ty {
                    FieldType::Unsigned => self.get_unsigned_field(ident)
                        .ok_or(AgnesError::FieldNotFound(ident.clone()))
                        .map(|data| f.apply_unsigned(data)),
                    FieldType::Signed => self.get_signed_field(ident)
                        .ok_or(AgnesError::FieldNotFound(ident.clone()))
                        .map(|data| f.apply_signed(data)),
                    FieldType::Text => self.get_text_field(ident)
                        .ok_or(AgnesError::FieldNotFound(ident.clone()))
                        .map(|data| f.apply_text(data)),
                    FieldType::Boolean => self.get_boolean_field(ident)
                        .ok_or(AgnesError::FieldNotFound(ident.clone()))
                        .map(|data| f.apply_boolean(data)),
                    FieldType::Float => self.get_float_field(ident)
                        .ok_or(AgnesError::FieldNotFound(ident.clone()))
                        .map(|data| f.apply_float(data)),
                }
            })
    }
}
impl<'a, 'b> ApplyFieldReduce<'a> for Selection<'a, 'b, Rc<DataStore>> {
    fn apply_field_reduce<F: FieldReduceFn<'a>>(&self, f: &mut F)
        -> Result<F::Output>
    {
        self.data.get_reduce_data_index(&self.ident)
            .ok_or(AgnesError::FieldNotFound(self.ident.clone()))
            .map(|data| f.reduce(vec![data]))
    }

}
impl<'a, 'b> ApplyFieldReduce<'a> for Vec<Selection<'a, 'b, Rc<DataStore>>> {
    fn apply_field_reduce<F: FieldReduceFn<'a>>(&self, f: &mut F)
        -> Result<F::Output>
    {
        self.iter().map(|selection| {
            selection.data.get_reduce_data_index(&selection.ident)
                .ok_or(AgnesError::FieldNotFound(selection.ident.clone()))
        }).collect::<Result<Vec<_>>>()
            .map(|data_vec| f.reduce(data_vec))
    }
}

/// Trait for adding data (of valid types) to a `DataStore`.
pub trait AddData<T: DataType> {
    /// Add a single value to the specified field.
    fn add(&mut self, ident: FieldIdent, value: MaybeNa<T>);
}
/// Trait for adding a vector of data (of valid types) to a `DataStore`.
pub trait AddDataVec<T: DataType> {
    /// Add a vector of data values to the specified field.
    fn add_data_vec(&mut self, ident: FieldIdent, data: Vec<MaybeNa<T>>);
}

macro_rules! impl_add_data {
    ($($dtype:ty, $fty:path, $hm:tt);*) => {$(

impl AddData<$dtype> for DataStore {
    fn add(&mut self, ident: FieldIdent, value: MaybeNa<$dtype>) {
        insert_value(&mut self.$hm, ident, value);
    }
}
impl AddDataVec<$dtype> for DataStore {
    fn add_data_vec(&mut self, ident: FieldIdent, mut data: Vec<MaybeNa<$dtype>>) {
        self.add_field(TypedFieldIdent { ident: ident.clone(), ty: $fty });
        for datum in data.drain(..) {
            insert_value(&mut self.$hm, ident.clone(), datum);
        }
    }
}

    )*}
}
impl_add_data!(
    u64,    FieldType::Unsigned, unsigned;
    i64,    FieldType::Signed,   signed;
    String, FieldType::Text,     text;
    bool,   FieldType::Boolean,  boolean;
    f64,    FieldType::Float,    float
);

fn max_len<K, T: DataType>(h: &HashMap<K, MaskedData<T>>) -> usize where K: Eq + Hash {
    h.values().fold(0, |acc, v| max(acc, v.len()))
}
fn is_hm_homogeneous<K, T: DataType>(h: &HashMap<K, MaskedData<T>>) -> Option<usize>
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
fn is_hm_homogeneous_with<K, T: DataType>(h: &HashMap<K, MaskedData<T>>, value: usize)
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
fn insert_value<T: Default + DataType>(
    h: &mut HashMap<FieldIdent, MaskedData<T>>,
    k: FieldIdent,
    v: MaybeNa<T>)
{
    h.entry(k).or_insert(MaskedData::new()).push(v);
}
fn parse<T: DataType, F>(value_str: String, f: F) -> Result<MaybeNa<T>> where F: Fn(String)
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
