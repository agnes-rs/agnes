//! CSV-based source and reader objects and implentation.

use std::collections::HashMap;

use csv;
use csv_sniffer::Sniffer;
use csv_sniffer::metadata::Metadata;

use source::{LocalFileReader, FileLocator};
use source::decode::decode;
use error::*;
use store::DataStore;
use field::{FieldIdent, TypedFieldIdent, SrcField};

/// CSV Data source. Contains location of data file, and computes CSV metadata. Can be turned into
/// `CsvReader` object.
#[derive(Debug)]
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
            metadata: metadata
        })
    }
    /// Return the compute `Metadata` for this CSV source.
    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }
}

/// Reader object responsible for converting a CSV file into a data store.
#[derive(Debug)]
pub struct CsvReader {
    reader: csv::Reader<LocalFileReader>,
    field_coll: FieldCollection
}

impl CsvReader {
    /// Create a new CSV reader from a CSV source specification. This will process header row (if
    /// exists), and verify the fields specified in the `CsvSource` object exist in this CSV file.
    pub fn new(src: &CsvSource) -> Result<CsvReader> {
        let file_reader = LocalFileReader::new(&src.src)?;
        let mut csv_reader = src.metadata.dialect.open_reader(file_reader)?;
        let mut field_coll = FieldCollection::new();

        {
            let headers = csv_reader.headers()?;
            debug_assert_eq!(src.metadata.num_fields, src.metadata.types.len());
            if src.metadata.dialect.header.has_header_row {
                if headers.len() != src.metadata.num_fields {
                    return Err(AgnesError::CsvDialect(
                        "header row must match number of fields in CSV file".into()));
                }
                for (i, header) in headers.iter().enumerate() {
                    field_coll.add(TypedFieldIdent::new(
                        FieldIdent::Name(header.into()),
                        src.metadata.types[i].into()
                    ), i);
                }
            } else {
                for i in 0..src.metadata.num_fields {
                    field_coll.add(TypedFieldIdent::new(
                        FieldIdent::Index(i),
                        src.metadata.types[i].into()
                    ), i);
                }
            }
        }

        Ok(CsvReader {
            reader: csv_reader,
            field_coll: field_coll,
        })
    }

    /// Read a `CsvSource` into a `DataStore` object.
    pub fn read(&mut self) -> Result<DataStore> {
        let mut ds = DataStore::empty();
        for (_, row) in self.reader.byte_records().enumerate() {
            let record = row?;
            for field in &self.field_coll.fields {
                let value = decode(record.get(field.src_index).ok_or(
                    AgnesError::FieldNotFound(field.ty_ident.ident.clone()))?)?;
                ds.insert(field.ty_ident.clone(), value.clone())?;
            }
        }
        Ok(ds)
    }
}

#[derive(Debug, Clone)]
struct IndexMap {
    src: usize,
    dest: usize,
}
impl IndexMap {
    fn new(src: usize, dest: usize) -> IndexMap {
        IndexMap { src: src, dest: dest }
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
