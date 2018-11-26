//! CSV-based source and reader objects and implentation.

use std::str::FromStr;
use std::fmt::Debug;
use std::collections::{HashMap};

// use typenum::{Unsigned, Add1, B1};

use csv_sniffer::Sniffer;
use csv_sniffer::metadata::Metadata;

use source::file::{LocalFileReader, FileLocator};
use source::decode::decode;
use error::*;
use store::{AssocStorage, DataStore, AddFieldFromValueIter};
use cons::*;
use field::FieldIdent;
use field::{Value};
use fieldlist::{FieldPayloadCons, FieldSpec, FieldDesignator, SpecCons};
use label::{TypedValue, Valued};

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

// pub struct CsvSrcSpec<Field, FIdx, DType> {
//     _field: PhantomData<Field>,
//     _fidx: PhantomData<FIdx>,
//     _dtype: PhantomData<DType>,
//     src_idx: usize,
// }

// impl<Field, FIdx, DType> CsvSrcSpec<Field, FIdx, DType> {
//     fn from_src_field_spec(src_idx: usize)
//         -> Self
//     {
//         CsvSrcSpec {
//             _field: PhantomData,
//             _fidx: PhantomData,
//             _dtype: PhantomData,
//             src_idx,
//         }
//     }
// }

pub type CsvSrcSpecCons<Label, DType, Tail> = FieldPayloadCons<Label, DType, usize, Tail>;

pub trait IntoCsvSrcSpec {
    type CsvSrcSpec;

    fn into_csv_src_spec(self, headers: &HashMap<String, usize>, num_fields: usize)
        -> Result<Self::CsvSrcSpec>;
}
impl IntoCsvSrcSpec for Nil {
    type CsvSrcSpec = Nil;

    fn into_csv_src_spec(self, _headers: &HashMap<String, usize>, _num_fields: usize)
        -> Result<Nil>
    {
        Ok(Nil)
    }
}

impl<Label, DType, Tail> IntoCsvSrcSpec
    for SpecCons<Label, DType, Tail>
    where Tail: IntoCsvSrcSpec,
{
    type CsvSrcSpec = CsvSrcSpecCons<Label, DType, Tail::CsvSrcSpec>;

    fn into_csv_src_spec(
        self,
        headers: &HashMap<String, usize>,
        num_fields: usize
    )
        -> Result<CsvSrcSpecCons<Label, DType, Tail::CsvSrcSpec>>
    {
        let idx = match *self.head.value_ref() {
            FieldDesignator::Expr(ref s) => *headers.get(s)
                .ok_or(AgnesError::FieldNotFound(FieldIdent::Name(s.to_string())))?,
            FieldDesignator::Idx(idx) => {
                if idx >= num_fields {
                    return Err(AgnesError::IndexError { index: idx, len: num_fields });
                };
                idx
            }
        };
        Ok(Cons {
            head: TypedValue::from(idx).into(),
            tail: self.tail.into_csv_src_spec(headers, num_fields)?
        })
    }
}



// pub trait FromSpec<Spec> {
//     fn from_spec(spec: Spec, headers: &HashMap<String, usize>, num_fields: usize)
//         -> Result<Self>
//         where Self: Sized;
// }

// impl<Spec> FromSpec<Spec> for Nil {
//     fn from_spec(_spec: Spec, _headers: &HashMap<String, usize>, _num_fields: usize)
//         -> Result<Nil>
//     {
//         Ok(Nil)
//     }
// }

// impl<Field, SrcFIdx, CsvSrcSpecTail, SpecTail> FromSpec<SpecCons<Field, SrcFIdx, SpecTail>>
//     for CsvSrcSpecCons<Field, CsvSrcSpecTail>
//     where SrcFIdx: Position,
//           CsvSrcSpecTail: FromSpec<SpecTail>
// {
//     fn from_spec(
//         spec: SpecCons<Field, SrcFIdx, SpecTail>,
//         headers: &HashMap<String, usize>,
//         num_fields: usize
//     )
//         -> Result<CsvSrcSpecCons<Field, CsvSrcSpecTail>>
//     {
//         let idx = match spec.head.payload {
//             FieldDesignator::Expr(ref s) => *headers.get(s)
//                 .ok_or(AgnesError::FieldNotFound(FieldIdent::Name(s.to_string())))?,
//             FieldDesignator::Idx(_) => {
//                 let idx = SrcFIdx::POS;
//                 if idx >= num_fields {
//                     return Err(AgnesError::IndexError { index: idx, len: num_fields });
//                 };
//                 idx
//             }
//         };
//         Ok(Cons {
//             head: idx.into(),
//             // head: CsvSrcSpec::from_src_field_spec(idx),
//             tail: FromSpec::from_spec(spec.tail, headers, num_fields)?
//         })
//     }
// }

// pub trait AttachSrcPos {
//     type WithSrcPos;
//     fn attach(self, headers: &HashMap<String, usize>, num_fields: usize)
//         -> Result<Self::WithSrcPos>;
// }
// impl AttachSrcPos for Nil {
//     type WithSrcPos = Nil;
//     fn attach(self, _headers: &HashMap<String, usize>, _num_fields: usize)
//         -> Result<Nil>
//     {
//         Ok(Nil)
//     }
// }
// impl<Field, FIdx, SrcFIdx, DType, Tail> AttachSrcPos
//     for SpecCons<Field, FIdx, SrcFIdx, DType, Tail>
//     where SrcFIdx: Unsigned,
//           Tail: AttachSrcPos
// {
//     type WithSrcPos = Cons<CsvSrcSpec<Field, FIdx, DType>, Tail::WithSrcPos>;
//     fn attach(self, headers: &HashMap<String, usize>, num_fields: usize)
//         -> Result<Self::WithSrcPos>
//     {
//         let idx = match self.head.src_name {
//             FieldDesignator::Expr(ref s) => *headers.get(s)
//                 .ok_or(AgnesError::FieldNotFound(FieldIdent::Name(s.to_string())))?,
//             FieldDesignator::Idx(_) => {
//                 let idx = SrcFIdx::to_usize();
//                 if idx >= num_fields {
//                     return Err(AgnesError::IndexError { index: idx, len: num_fields });
//                 };
//                 idx
//             }
//         };
//         Ok(Cons {
//             head: CsvSrcSpec::from_src_field_spec(idx),
//             tail: self.tail.attach(headers, num_fields)?
//         })
//     }

// }

// impl<Field, FIdx, DType, Payload, Tail> AssocFields
//     for Cons<CsvSrcSpec<Field, FIdx, DType>, Tail>
//     where Tail: AssocFields,
// {
//     type Fields = FieldCons<Field, FIdx, DType, Payload, Tail::Fields>;
// }

pub trait BuildDStore
{
    type OutputFields: AssocStorage;
    fn build(&mut self, src: &CsvSource) -> Result<DataStore<Self::OutputFields>>;
}
impl BuildDStore for Nil {
    type OutputFields = Nil;
    fn build(&mut self, _src: &CsvSource) -> Result<DataStore<Nil>> {
        Ok(DataStore::<Nil>::empty())
    }
}
impl<Label, DType, Tail> BuildDStore
    for FieldPayloadCons<
        Label,
        DType,
        usize,
        Tail
    >
    // for Cons<
    //     CsvSrcSpec<Field, Add1<<Tail::OutputFields as FieldIndex>::FIdx>, DType>,
    //     Tail
    // >
    where
          Tail: BuildDStore,
          DataStore<<Tail as BuildDStore>::OutputFields>: AddFieldFromValueIter<Label, DType>,
          Tail::OutputFields: PushBack<FieldSpec<Label, DType>>,
          <Tail::OutputFields as PushBack<FieldSpec<Label, DType>>>::Output: AssocStorage,
          Label: Debug,
    //       Tail::OutputFields: FieldIndex,
    //       <Tail::OutputFields as FieldIndex>::FIdx: Add<B1>,
          DType: FromStr + Debug + Default + Clone,
          ParseError: From<<DType as FromStr>::Err>,
{
    type OutputFields =
        <DataStore<<Tail as BuildDStore>::OutputFields> as AddFieldFromValueIter<Label, DType>>
            ::OutputFields;

    fn build(&mut self, src: &CsvSource) -> Result<DataStore<Self::OutputFields>> {
        let file_reader = LocalFileReader::new(&src.src)?;
        let mut csv_reader = src.metadata.dialect.open_reader(file_reader)?;
        let ds = self.tail.build(src)?;

        let values: Vec<Value<DType>> = csv_reader.byte_records()
            .map(|row| {
                let record = row?;
                let value = decode(
                    record.get(*self.head.value_ref().value_ref()).ok_or_else(||
                        AgnesError::FieldNotFound(FieldIdent::Name(stringify![Field].to_string()))
                    )?
                )?;
                Ok(value)
            })
            .map(|sresult| sresult.and_then(|s| {
                let trimmed = s.trim();
                if trimmed.is_empty() {
                    Ok(Value::Na)
                } else {
                    trimmed.parse::<DType>()
                        .map(|value| Value::Exists(value))
                        .map_err(|e| AgnesError::Parse(e.into()))
                }
            }))
            .collect::<Result<_>>()?;
        let ds = ds.add_field_from_value_iter::<Label, DType, _, _>(values);


        Ok(ds)
    }
}

/// Reader object responsible for converting a CSV file into a data store.
// #[derive(Debug)]
// pub struct CsvReader<Spec: AttachSrcPos> {
//     src: CsvSource,
//     csv_src_spec: Spec::WithSrcPos,
// }

#[derive(Debug)]
pub struct CsvReader<CsvSpec> {
    src: CsvSource,
    csv_src_spec: CsvSpec,
}

impl<CsvSrcSpec> CsvReader<CsvSrcSpec>
    // where Spec: AttachSrcPos + AssocFields
{
    /// Create a new CSV reader from a CSV source specification. This will process header row (if
    /// exists), and verify the fields specified in the `CsvSource` object exist in this CSV file.
    // pub fn new<SrcFIdx, SpecTail>(src: &CsvSource, spec: SpecCons<Field, SrcFIdx, SpecTail>)
    pub fn new<Spec>(src: &CsvSource, spec: Spec)
        -> Result<CsvReader<Spec::CsvSrcSpec>>
        where
              Spec: IntoCsvSrcSpec<CsvSrcSpec=CsvSrcSpec>
              // SrcFIdx: Position,
              // Spec: AssocField<Field=Field>,
              // SpecCons<Field, SrcFIdx, SpecTail>:
              //   IntoCsvSrcSpec<CsvSrcSpec=CsvSrcSpecCons<Field, Tail>>,
              // CsvSrcSpecCons<Field, Tail>: FromSpec<SpecCons<Field, SrcFIdx, SpecTail>>
              // CsvSrcSpecCons<Field, Tail>: FromSpec<SpecCons<Field, SrcFIdx, SpecTail>>
              // Tail: FromSpec<SpecTail>
        // where Spec: Debug,
    {
        // println!("spec: {:?}", spec);
        let file_reader = LocalFileReader::new(&src.src)?;
        let mut csv_reader = src.metadata.dialect.open_reader(file_reader)?;

        debug_assert_eq!(src.metadata.num_fields, src.metadata.types.len());

        let headers = if src.metadata.dialect.header.has_header_row {
            let headers = csv_reader.headers()?;
            if headers.len() != src.metadata.num_fields {
                return Err(AgnesError::CsvDialect(
                    "header row does not match sniffed number of fields in CSV file".into()));
            }
            headers.iter().enumerate().map(|(i, s)| (s.to_string(), i)).collect::<HashMap<_, _>>()
        } else {
            HashMap::new()
        };
        let csv_src_spec = spec.into_csv_src_spec(&headers, src.metadata.num_fields)?;

        Ok(CsvReader {
            //TODO: remove source from here
            src: src.clone(),
            csv_src_spec
        })
    }

    /// Read a `CsvSource` into a `DataStore` object.
    pub fn read(&mut self)
        -> Result<DataStore<CsvSrcSpec::OutputFields>>
        where
              // Cons<Field, Tail>: DataStorage,
              CsvSrcSpec: BuildDStore,
              // FieldPayloadCons<Field, usize, Tail>: BuildDStore<OutputFields=Cons<Field, Tail>>
        // -> Result<DataStore<StorageCons<Field, Tail>>>
        // where Field: FieldTypes
    // pub fn read(&mut self) -> Result<DataStore<<Spec::WithSrcPos as AssocFields>::Fields>>
        // where Spec::Fields: DataStorage,
        //       Spec::WithSrcPos: AssocFields
        //         + BuildDStore<OutputFields=<Spec::WithSrcPos as AssocFields>::Fields>,
        //       <Spec::WithSrcPos as AssocFields>::Fields: DataStorage,
    {
        self.csv_src_spec.build(&self.src)
    }
}
