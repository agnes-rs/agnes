//! CSV-based source and reader objects and implentation.

use std::marker::PhantomData;
use std::collections::HashMap;

use csv;
use csv_sniffer::Sniffer;
use csv_sniffer::metadata::Metadata;

use source::{LocalFileReader, FileLocator};
use source::decode::decode;
use error::*;
use store::{DataStore};
use data_types::DataType;
use field::Value;
use field::{FieldIdent};
// use fieldlist::{AssocFields, FieldSpecs, SrcFieldDesignator};

use data_types::csv as dt_csv;

/// CSV Data source. Contains location of data file, and computes CSV metadata. Can be turned into
/// `CsvReader` object.
#[derive(Debug, Clone)]
pub struct CsvSource {
    // File source object for the CSV file
    src: FileLocator,
    // CSV file metadata (from `csv-sniffer` crate)
    metadata: Metadata
}

impl CsvSource {
    /// Create a new `CsvSource` object with provided file location. This constructor will analyze
    /// (sniff) the file to detect its metadata (delimiter, quote character, field types, etc.)
    ///
    /// # Error
    /// Fails if unable to open the file at the provided location, or if CSV analysis fails.
    pub fn new(loc: FileLocator) -> Result<CsvSource> {
        //TODO: make sample size configurable?
        let mut file_reader = LocalFileReader::new(&loc)?;
        let metadata = Sniffer::new().sniff_reader(&mut file_reader)?;

        Ok(CsvSource {
            src: loc,
            metadata
        })
    }
    /// Return the compute `Metadata` for this CSV source.
    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }
}

/// Reader object responsible for converting a CSV file into a data store.
#[derive(Debug)]
pub struct CsvReader<Fields> {
    src: CsvSource,
    reader: csv::Reader<LocalFileReader>,
    field_coll: FieldCollection,
    _fields: PhantomData<Fields>,
}

impl<Fields> CsvReader<Fields> {
    /// Create a new CSV reader from a CSV source specification. This will process header row (if
    /// exists), and verify the fields specified in the `CsvSource` object exist in this CSV file.
    pub fn new<Spec>(src: &CsvSource, spec: Spec) -> Result<CsvReader<Fields>>
        where Spec: FieldSpecs<dt_csv::Types> + AssocFields<Fields=Fields>,
    {
        let file_reader = LocalFileReader::new(&src.src)?;
        let mut csv_reader = src.metadata.dialect.open_reader(file_reader)?;
        let mut field_coll = FieldCollection::new();
        let field_specs = spec.field_specs();
        let src_field_names: HashMap<String, dt_csv::DType> = field_specs.iter()
            .filter_map(|spec| {
                match spec.src_name {
                    SrcFieldDesignator::Name(ref s) => Some((s.clone(), spec.dtype)),
                    SrcFieldDesignator::Index(_) => None,
                }
            }).collect();
        let src_field_indices: HashMap<usize, dt_csv::DType> = field_specs.iter()
            .filter_map(|spec| {
                match spec.src_name {
                    SrcFieldDesignator::Name(_) => None,
                    SrcFieldDesignator::Index(idx) => Some((idx, spec.dtype))
                }
            }).collect();

        {
            debug_assert_eq!(src.metadata.num_fields, src.metadata.types.len());
            if src.metadata.dialect.header.has_header_row {
                let headers = csv_reader.headers()?;
                if headers.len() != src.metadata.num_fields {
                    return Err(AgnesError::CsvDialect(
                        "header row does not match sniffed number of fields in CSV file".into()));
                }
                for (i, header) in headers.iter().enumerate() {
                    if let Some(&value) = src_field_names.get(header) {
                        field_coll.add(TypedFieldIdent::new(
                            FieldIdent::Name(header.into()),
                            value
                        ), i);
                    }
                    if let Some(&value) = src_field_indices.get(&(i + 1)) {
                        field_coll.add(TypedFieldIdent::new(
                            FieldIdent::Name(header.into()),
                            value
                        ), i);
                    }
                }
            } else {
                for i in 0..src.metadata.num_fields {
                    if let Some(&value) = src_field_indices.get(&(i + 1)) {
                        field_coll.add(TypedFieldIdent::new(
                            FieldIdent::Index(i),
                            value
                        ), i);
                    }
                }
            }
        }

        Ok(CsvReader {
            //TODO: remove source from here
            src: src.clone(),
            reader: csv_reader,
            field_coll,
            _fields: PhantomData::<Fields>
        })
    }

    /// Read a `CsvSource` into a `DataStore` object.
    pub fn read(&mut self) -> Result<DataStore<dt_csv::Types>>
    {
        let mut ds = DataStore::empty();
        for row in self.reader.byte_records() {
            let record = row?;
            for field in &self.field_coll.fields {
                let value = decode(
                    record.get(field.src_index).ok_or_else(||
                        AgnesError::FieldNotFound(field.ty_ident.ident.clone())
                    )?
                )?;
                insert(&mut ds, &field.ty_ident, &value)?;
            }
        }
        Ok(ds)
    }
}

// Insert a value (provided in unparsed string form) for specified field
fn insert(
    ds: &mut DataStore<dt_csv::Types>, ty_ident: &TypedFieldIdent, value_str: &str
)
    -> Result<()>
{
    let ident = ty_ident.ident.clone();
    match ty_ident.ty {
        dt_csv::DType::u64 => {
            ds.add::<u64, _>(ident, parse(value_str, parse_unsigned)?)?;
        },
        dt_csv::DType::i64 => {
            ds.add::<i64, _>(ident, parse(value_str, parse_signed)?)?;
        },
        dt_csv::DType::String => {
            ds.add::<String, _>(ident, parse(value_str, |s| Ok(s.to_string()))?)?;
        },
        dt_csv::DType::bool => {
            ds.add::<bool, _>(ident, parse(value_str, |val| Ok(val.parse::<bool>()?))?)?;
        },
        dt_csv::DType::f64 => {
            ds.add::<f64, _>(ident, parse(value_str, |val| Ok(val.parse::<f64>()?))?)?;
        }
    }
    Ok(())
}

fn parse<T: DataType<dt_csv::Types>, F>(value_str: &str, f: F) -> Result<Value<T>>
    where F: Fn(&str) -> Result<T>
{
    if value_str.trim().is_empty() {
        Ok(Value::Na)
    } else {
        Ok(Value::Exists(f(value_str)?))
    }
}
/// A forgiving unsigned integer parser. If normal unsigned integer parsing fails, tries to parse
/// as a signed integer; if successful, assumes that the integer is negative and translates that
/// to '0'. If that fails, tries to parse as a float; if successful, converts to unsigned integer
/// (or '0' if negative)
fn parse_unsigned(value_str: &str) -> Result<u64> {
    Ok(value_str.parse::<u64>().or_else(|e| {
        // try parsing as a signed int...if successful, it's negative, so just set it to 0
        value_str.parse::<i64>().map(|_| 0u64).or_else(|_| {
            // try parsing as a float
            value_str.parse::<f64>().map(|f| {
                if f < 0.0 { 0u64 } else { f as u64 }
            }).or_else(|_| Err(e))
        })
    })?)
}
/// A forgiving signed integer parser. If normal signed integer parsing fails, tries to parse as
/// a float; if successful, converts to a signed integer.
fn parse_signed(value_str: &str) -> Result<i64> {
    Ok(value_str.parse::<i64>().or_else(|e| {
        // try parsing as float
        value_str.parse::<f64>().map(|f| f as i64).or_else(|_| Err(e))
    })?)
}


#[derive(Debug, Clone)]
struct IndexMap {
    src: usize,
    dest: usize,
}
impl IndexMap {
    fn new(src: usize, dest: usize) -> IndexMap {
        IndexMap { src, dest }
    }
}

#[derive(Debug, Clone)]
struct TypedFieldIdent {
    ident: FieldIdent,
    ty: dt_csv::DType,
}
impl TypedFieldIdent {
    fn new(ident: FieldIdent, ty: dt_csv::DType) -> TypedFieldIdent {
        TypedFieldIdent {
            ident,
            ty
        }
    }
}

#[derive(Debug, Clone)]
struct SrcField {
    /// Field identifier and type
    ty_ident: TypedFieldIdent,
    /// Index of field within the original data file
    src_index: usize,
}
impl SrcField {
    /// Create a new `SrcField` object from specified typed field identifier obejct ans source
    /// index.
    fn from_ty_ident(ty_ident: TypedFieldIdent, src_index: usize) -> SrcField {
        SrcField {
            ty_ident,
            src_index
        }
    }
}

#[derive(Debug, Clone)]
struct FieldCollection {
    pub fields: Vec<SrcField>,
    pub field_map: HashMap<FieldIdent, IndexMap>
}
impl FieldCollection {
    pub fn new() -> FieldCollection {
        FieldCollection {
            fields: vec![],
            field_map: HashMap::new(),
        }
    }
    pub fn add(&mut self, ty_ident: TypedFieldIdent, source_idx: usize) {
        self.field_map.insert(ty_ident.ident.clone(), IndexMap::new(source_idx, self.fields.len()));
        self.fields.push(SrcField::from_ty_ident(ty_ident, source_idx));
    }
}
