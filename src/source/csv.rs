//! CSV-based source and reader objects and implentation.

use std::io::{BufRead, BufReader};
use std::collections::HashMap;

use csv;

use source::{FileSource, FileReader};
use source::decode::decode;
use error::*;
use store::DataStore;
use field::{FieldIdent, TypedFieldIdent, SrcField};

/// Specification of whether or not a header row exists. Typically, this will be either `Yes` or
/// `No`, but occasionally CSV data files have a few headers lines before the data starts, which
/// can be ignored using `NoSkip` or `YesSkip`, indicating no header exists and skip a certain
/// number of lines, or a header row exists after skipping a certain number of lines, respectively.
#[derive(Debug, Clone)]
pub enum HasHeaders {
    /// No header row, start parsing data at first row.
    No,
    /// Top row is the header. Data is following and subsequent rows.
    Yes,
    /// No header row. Ignore the top X rows, start parsing immediately after.
    NoSkip(usize),
    /// Header row after X rows. Ignore the top X rows, next work is the header, data is after that.
    YesSkip(usize)
}
impl HasHeaders {
    /// Returns `true` if file has a header row (either `HasHeaders::Yes` or
    /// `HasHeaders::YesSkip(_)`). Returns `false` otherwise.
    pub fn is_yes(&self) -> bool {
        match *self {
            HasHeaders::Yes | HasHeaders::YesSkip(_) => true,
            _ => false
        }
    }
    /// Return the number of skipped lines at beginning of file, if any. Returns `None` if no lines
    /// were skipped (i.e. `HasHeaders::Yes` or `HasHeader::No`).
    pub fn skip_count(&self) -> Option<usize> {
        match *self {
            HasHeaders::YesSkip(skip) | HasHeaders::NoSkip(skip) => Some(skip),
            _ => None
        }
    }
}

/// Source information for a CSV file.
#[derive(Debug, Clone)]
pub struct CsvSource {
    /// File source object for the CSV file
    src: FileSource,
    /// Whether or not the first row of this CSV file contains the column headers
    has_headers: HasHeaders,
    /// CSV delimiter (default ',')
    delimiter: u8,
    /// List of fields (columns) to be included from this CSV source
    fields: Vec<TypedFieldIdent>
}

/// Builder for `CsvSource` object.
#[derive(Debug, Clone)]
pub struct CsvSourceBuilder {
    /// File source object for the CSV file
    src: FileSource,
    /// Whether or not the first row of this CSV file contains the column headers
    has_headers: Option<HasHeaders>,
    /// CSV delimiter (default ',')
    delimiter: Option<u8>,
    /// List of fields (columns) to be included from this CSV source
    fields: Option<Vec<TypedFieldIdent>>,
}
impl CsvSourceBuilder {
    /// Start building a `CsvSource` object, starting with a `FileSource`.
    pub fn new<T: Into<FileSource>>(file_source: T) -> CsvSourceBuilder {
        CsvSourceBuilder {
            src: file_source.into(),
            has_headers: None,
            delimiter: None,
            fields: None,
        }
    }
    /// Update this builder with `HasHeaders` information indicating whether or not (and where)
    /// a CSV file has a header row. If not specified, will default the value to `HasHeaders::Yes`.
    pub fn has_headers<T: Into<HasHeaders>>(&mut self, has_headers: T) -> &mut CsvSourceBuilder {
        self.has_headers = Some(has_headers.into());
        self
    }
    /// Update this builder with the specification of the delimiter used in this CSV file. If not
    /// specified, will default the value to `b','` (a comma).
    pub fn delimiter<T: Into<u8>>(&mut self, delimiter: T) -> &mut CsvSourceBuilder {
        self.delimiter = Some(delimiter.into());
        self
    }
    /// Update this builder with the specification of the fields to pull from this CSV file. NOTE:
    /// if not specified, this `CsvSource` will provide no fields (and thus no data).
    pub fn fields<T: Into<Vec<TypedFieldIdent>>>(&mut self, fields: T) -> &mut CsvSourceBuilder {
        self.fields = Some(fields.into());
        self
    }
    /// Finalize building, producing a `CsvSource`.
    pub fn build(&self) -> CsvSource {
        CsvSource {
            src: self.src.clone(),
            has_headers: if let Some(ref has_headers) = self.has_headers {
                has_headers.clone()
            } else {
                HasHeaders::Yes
            },
            delimiter: if let Some(delimiter) = self.delimiter { delimiter } else { b',' },
            fields: if let Some(ref fields) = self.fields { fields.clone() } else { vec![] }
        }
    }
}

/// Reader object responsible for converting a CSV file into a data store.
#[derive(Debug)]
pub struct CsvReader {
    reader: csv::Reader<BufReader<FileReader>>,
    field_coll: FieldCollection
}

impl CsvReader {
    /// Create a new CSV reader from a CSV source specification. This will process header row (if
    /// exists), and verify the fields specified in the `CsvSource` object exist in this CSV file.
    pub fn new(mut src: CsvSource) -> Result<CsvReader> {
        let mut file_reader = BufReader::new(FileReader::new(src.src)?);
        if let Some(skip) = src.has_headers.skip_count() {
            let mut devnull = String::new();
            for _ in 0..skip { file_reader.read_line(&mut devnull)?; }
        }

        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(src.has_headers.is_yes())
            .delimiter(src.delimiter)
            .from_reader(file_reader);

        let mut fields_map: HashMap<FieldIdent, TypedFieldIdent> = HashMap::new();
        for field in src.fields.drain(..) {
            fields_map.insert(field.ident.clone(), field);
        }
        let mut field_coll = FieldCollection::new();

        {
            let headers = rdr.headers()?;
            if src.has_headers.is_yes() {
                for (i, header) in headers.iter().enumerate() {
                    match fields_map.remove(&FieldIdent::Name(header.to_string())) {
                        Some(info) => field_coll.add(info, i),
                        None => {
                            match fields_map.remove(&FieldIdent::Index(i)) {
                                Some(info) => field_coll.add(info, i),
                                None => {} // exists in CSV headers, not in specified field list
                                           // skip
                            }
                        }
                    }
                }
            } else {
                // the first row isn't headers, but will still give us the len
                //TODO: determine if this consumes the first row
                for i in 0..headers.len() {
                    match fields_map.remove(&FieldIdent::Index(i)) {
                        Some(info) => field_coll.add(info, i),
                        None => {} // exists in CSV headers, not in specified field list; skip
                    }
                }
            }
        }

        if !fields_map.is_empty() {
            let column_list = fields_map.keys().map(|ident| {
                match *ident {
                    FieldIdent::Index(i) => format!("Field {}", i),
                    FieldIdent::Name(ref s)  => s.clone()
                }
            }).collect::<Vec<_>>().join(",");
            return Err(ViewsError::Field(
                format!("Field names not found in CSV: {}", column_list)));
        }

        Ok(CsvReader {
            reader: rdr,
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
                    ViewsError::MissingSourceField(field.ty_ident.ident.clone()))?)?;
                ds.insert(field.clone(), value.clone())?;
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
