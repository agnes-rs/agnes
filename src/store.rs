use std::rc::Rc;
use std::fmt::Debug;
use std::ops::Add;
use std::fmt;

use typenum::{
    bit::B1,
    uint::UTerm
};

use cons::*;
use fieldlist::{FieldCons, FieldPayloadCons};
use field::{Value, FieldData};
use select::{SelectFieldByLabel, FieldSelect};
use access::{DataIndex};
use label::*;
use view::{DataView, ViewFrameCons, FrameLookupCons};
use frame::{DataFrame};

pub type StorageCons<Label, DType, Tail>
    = FieldPayloadCons<Label, DType, Rc<FieldData<DType>>, Tail>;

#[derive(Debug)]
pub struct DataStore<Fields: AssocStorage> {
    data: Fields::Storage,
}

pub trait AssocStorage {
    type Storage: Debug;
}
impl<Label, DType, Tail> AssocStorage for FieldCons<Label, DType, Tail>
    where Tail: AssocStorage,
          Label: Debug,
          DType: Debug,
{
    type Storage = StorageCons<Label, DType, Tail::Storage>;
}
impl AssocStorage for Nil {
    type Storage = Nil;
}

impl<Fields> DataStore<Fields>
    where Fields: AssocStorage
{
    /// Generate and return an empty data store
    pub fn empty() -> DataStore<Nil> {
        DataStore {
            data: Nil,
        }
    }
}

pub trait NRows
{
    fn nrows(&self) -> usize;
}
impl NRows for Nil {
    fn nrows(&self) -> usize { 0 }
}
impl<Label, DType, Tail> NRows for StorageCons<Label, DType, Tail>
{
    fn nrows(&self) -> usize {
        self.head.value_ref().len()
    }
}

impl<Fields> NRows
    for DataStore<Fields>
    where Fields: AssocStorage,
          Fields::Storage: NRows,
{
    fn nrows(&self) -> usize
    {
        self.data.nrows()
    }
}

// ways to add:
// - add_field(FieldData) -> DataStore<...>
// - add_field_from_iter(Iterator<Item=T>) -> DataStore<...>
// - new_field::<Field>() -> DataStore<...>
// - field_mut::<Field>() -> DataIndexMut<Item=T>

pub trait AddLabeledField<NewLabel, NewDType>
{
    type OutputFields: AssocStorage;

    fn add_field(self, data: FieldData<NewDType>)
        -> DataStore<Self::OutputFields>;
}
impl<PrevFields, NewLabel, NewDType> AddLabeledField<NewLabel, NewDType>
    for DataStore<PrevFields>
    where PrevFields: AssocStorage,
          NewLabel: Debug,
          NewDType: Debug,
{
    type OutputFields = FieldCons<NewLabel, NewDType, PrevFields>;

    fn add_field(self, data: FieldData<NewDType>)
        -> DataStore<Self::OutputFields>
    {
        DataStore {
            data: StorageCons{
                head: TypedValue::from(Rc::new(data)).into(),
                tail: self.data
            }
        }
    }
}

// pub trait AddField<NewDType>
// {
//     type OutputFields: AssocStorage;
//     type LabelIdx;
// }

// impl<PrevFields, NewDType> AddField<NewDType>
//     for DataStore<PrevFields>
//     where PrevFields: AssocStorage,
//           NewDType: Debug,
// {
//     type OutputField = FieldCons<
// }

pub trait AddLabeledFieldFromIter<NewLabel, NewDType> {
    type OutputFields: AssocStorage;

    fn add_field_from_iter<IntoIter, Iter>(self, iter: IntoIter)
        -> DataStore<Self::OutputFields>
        where Iter: Iterator<Item=Value<NewDType>>,
              IntoIter: IntoIterator<IntoIter=Iter, Item=Value<NewDType>>;
}
impl<PrevFields, NewLabel, NewDType> AddLabeledFieldFromIter<NewLabel, NewDType>
    for DataStore<PrevFields>
    where PrevFields: AssocStorage,
          NewDType: Default + Clone,
          NewLabel: Debug, NewDType: Debug,
{
    type OutputFields = FieldCons<NewLabel, NewDType, PrevFields>;

    fn add_field_from_iter<IntoIter, Iter>(self, iter: IntoIter)
        -> DataStore<Self::OutputFields>
        where Iter: Iterator<Item=Value<NewDType>>,
              IntoIter: IntoIterator<IntoIter=Iter, Item=Value<NewDType>>
    {
        DataStore {
            data: StorageCons {
                head: TypedValue::from(Rc::new(iter.into_iter().collect::<FieldData<NewDType>>()))
                    .into(),
                tail: self.data
            }
        }
    }
}

pub trait AddLabeledEmptyField<NewLabel, NewDType> {
    type OutputFields: AssocStorage;

    fn add_empty_field(self)
        -> DataStore<Self::OutputFields>;
}
impl<PrevFields, NewLabel, NewDType> AddLabeledEmptyField<NewLabel, NewDType>
    for DataStore<PrevFields>
    where PrevFields: AssocStorage,
          NewLabel: Debug, NewDType: Debug,
{
    type OutputFields = FieldCons<NewLabel, NewDType, PrevFields>;

    fn add_empty_field(self)
        -> DataStore<Self::OutputFields>
    {
        DataStore {
            data: StorageCons {
                head: TypedValue::from(Rc::new(FieldData::default())).into(),
                tail: self.data
            }
        }
    }
}


pub trait NextLabel
{
    type Natural;
    type Output;

    fn next_label(self) -> Self::Output;
}
// impl NextLabel for DataStore<FieldCons<Label, DType, Tail>>
//     where Label: Natural
// {
//     type Natural = Add1<<Label as Natural>::Nat>;
//     type Output = DataStore<FieldCons<;

//     fn next_label(self) -> Self::Output
//     {

//     }
// }

#[macro_export]
macro_rules! add_field {
    ($ds:ident<$fields:ty>.$new_label:ident = $data:expr;) => {
        pub type $new_label = $crate::label::Label<
            typenum::Add1<<$fields as $crate::label::Natural>::Nat>
        >;
        let $ds = $ds.add_field::<$new_label, _>($data);
    }
}
#[macro_export]
macro_rules! add_field_from_iter {
    ($ds:ident<$fields:ty>.$new_label:ident = $iter:expr;) => {
        pub type $new_label = $crate::label::Label<
            typenum::Add1<<$fields as $crate::label::Natural>::Nat>
        >;
        let $ds = $ds.add_field_from_iter::<$new_label, _>($iter);
    }
}

impl<PrevFields> DataStore<PrevFields> where PrevFields: AssocStorage
{
    pub fn add_labeled_field<NewLabel, NewDType>(self, data: FieldData<NewDType>)
        -> DataStore<<Self as AddLabeledField<NewLabel, NewDType>>::OutputFields>
        where NewDType: fmt::Debug,
              Self: AddLabeledField<NewLabel, NewDType>
    {
        AddLabeledField::add_field(self, data)
    }

    pub fn add_labeled_field_from_iter<NewLabel, NewDType, IntoIter, Iter>(self, iter: IntoIter)
        -> DataStore<<Self as AddLabeledFieldFromIter<NewLabel, NewDType>>::OutputFields>
        where Iter: Iterator<Item=Value<NewDType>>,
              IntoIter: IntoIterator<IntoIter=Iter, Item=Value<NewDType>>,
              NewDType: fmt::Debug + Default + Clone,
              Self: AddLabeledFieldFromIter<NewLabel, NewDType>
    {
        AddLabeledFieldFromIter::add_field_from_iter(self, iter)
    }

    pub fn add_labeled_empty_field<NewLabel, NewDType>(self)
        -> DataStore<<Self as AddLabeledEmptyField<NewLabel, NewDType>>::OutputFields>
        where NewDType: fmt::Debug,
              Self: AddLabeledEmptyField<NewLabel, NewDType>
    {
        AddLabeledEmptyField::add_empty_field(self)
    }
}

impl<PrevFields> DataStore<PrevFields>
    where PrevFields: AssocStorage + LabelIndex,
          <PrevFields as LabelIndex>::Idx: Add<B1>
{
    pub fn add_field<NewDType>(self, data: FieldData<NewDType>)
        -> DataStore<<Self as AddLabeledField<NextLabelIndex<PrevFields>, NewDType>>::OutputFields>
        where NewDType: fmt::Debug,
              Self: AddLabeledField<NextLabelIndex<PrevFields>, NewDType>
    {
        AddLabeledField::add_field(self, data)
    }

    pub fn add_field_from_iter<NewDType, IntoIter, Iter>(self, iter: IntoIter)
        -> DataStore<
            <Self as AddLabeledFieldFromIter<NextLabelIndex<PrevFields>, NewDType>>::OutputFields
        >
        where Iter: Iterator<Item=Value<NewDType>>,
              IntoIter: IntoIterator<IntoIter=Iter, Item=Value<NewDType>>,
              NewDType: fmt::Debug + Default + Clone,
              Self: AddLabeledFieldFromIter<NextLabelIndex<PrevFields>, NewDType>
    {
        AddLabeledFieldFromIter::add_field_from_iter(self, iter)
    }

    pub fn add_empty_field<NewDType>(self)
        -> DataStore<
            <Self as AddLabeledEmptyField<NextLabelIndex<PrevFields>, NewDType>>::OutputFields
        >
        where NewDType: fmt::Debug,
              Self: AddLabeledEmptyField<NextLabelIndex<PrevFields>, NewDType>
    {
        AddLabeledEmptyField::add_empty_field(self)
    }
}

// impl<Fields> DataStore<Fields>
//     where Fields: AssocStorage
// {
//     pub fn field<'a, Label>(&'a self)
//         -> OwnedOrRef<'a, <<Fields::Storage as LookupElemByLabel<Label>>::Elem as Typed>::DType>
//         where Fields::Storage: LookupElemByLabel<Label>,
//               ElemOf<Fields::Storage, Label>: 'a + Typed + Valued,
//               ValueOf<ElemOf<Fields::Storage, Label>>:
//                 DataIndex<DType=TypeOf<ElemOf<Fields::Storage, Label>>>,
//               // <Fields::Storage as LookupElemByLabel<Label>>::Elem: 'a + Typed + Valued,
//               // <<Fields::Storage as LookupElemByLabel<Label>>::Elem as Valued>::Value:
//               //   DataIndex<DType=<<Fields::Storage as LookupElemByLabel<Label>>::Elem
//               //       as Typed>::DType>
//     {
//         OwnedOrRef::Ref(LookupElemByLabel::<Label>::elem(&self.data).value_ref())
//     }
// }

impl<Label, Fields> SelectFieldByLabel<Label> for DataStore<Fields>
    where Fields: AssocStorage,
          Fields::Storage: LookupElemByLabel<Label>,
          ElemOf<Fields::Storage, Label>: Typed,
          ElemOf<Fields::Storage, Label>:
            Valued<Value=Rc<FieldData<TypeOfElemOf<Fields::Storage, Label>>>>,
          TypeOfElemOf<Fields::Storage, Label>: Debug,
          // ValueOf<ElemOf<Fields::Storage, Label>>:
          //   DataIndex<DType=TypeOf<ElemOf<Fields::Storage, Label>>>,
          // TypeOf<ElemOf<Fields::Storage, Label>>: Debug
{
    type Output =
        Rc<FieldData<<<Fields::Storage as LookupElemByLabel<Label>>::Elem as Typed>::DType>>;

    fn select_field(&self) -> Self::Output
    {
        Rc::clone(LookupElemByLabel::<Label>::elem(&self.data).value_ref())
    }
}
impl<Fields> FieldSelect for DataStore<Fields> where Fields: AssocStorage {}

pub trait AssocFrameLookup
{
    type Output;
}
impl AssocFrameLookup for Nil
{
    type Output = Nil;
}
impl<Label, Value, Tail> AssocFrameLookup
    for LVCons<Label, Value, Tail>
    where Tail: AssocFrameLookup
{
    type Output = FrameLookupCons<Label, UTerm, <Tail as AssocFrameLookup>::Output>;
}


impl<Fields> DataStore<Fields>
    where Fields: AssocStorage + AssocFrameLookup
{
    pub fn into_view(self)
        -> DataView<<Fields as AssocFrameLookup>::Output, ViewFrameCons<UTerm, Fields, Nil>>
    {
        DataView::new(
            ViewFrameCons
            {
                head: Rc::new(DataFrame::from(self)).into(),
                tail: Nil
            }
        )
    }
}

// impl<Fields, NewLabel> From<DataStore<Fields>>
//     for DataView<<Fields as AssocLabels>::Labels, ViewFrameCons<NewLabel, Fields, Nil>>
//     where Fields: AssocStorage + AssocLabels,
// {
//     fn from(store: DataStore<Fields>)
//         -> DataView<<Fields as AssocLabels>::Labels, ViewFrameCons<NewLabel, Fields, Nil>>
//     {
//         // let frame_rc: Rc<DataFrame<Fields>> = Rc::new(store.into());
//         DataView {
//             _labels: PhantomData,
//             frames: ViewFrameCons {
//             // fields: ViewFields::from_fields(&frame_rc)
//                 head: DataFrame::from(store).into(),
//                 tail: Nil,
//             }
//         }
//     }
// }


// impl<'a, Fields, Ident, FIdx> SelectField<'a, Ident>
//     for DataStore<Fields>
//     where Fields: FSelector<Ident, FIdx>
// {
//     type Output = OwnedOrRef<'a, <Fields as FSelector<Ident, FIdx>>::DType>;

//     fn select_field(&'a self) -> Self::Output
//     {
//         self.data.select_field()
//     }
// }






// pub trait AddField<NewField, NewDType, PrevFields>
//     where PrevFields: AssocStorage + FieldIndex,
//           PrevFields::FIdx: Add<B1>,
// {
//     fn add(self, field: FieldData<NewDType>)
//         -> DataStore<FieldCons<NewField, Add1<PrevFields::FIdx>, NewDType, PrevFields>>
//         where NewDType: fmt::Debug;
// }

// impl<NewField, NewDType, PrevFields>
//     AddField<NewField, NewDType, PrevFields>
//     for DataStore<PrevFields>
//     where PrevFields: AssocStorage + FieldIndex,
//           PrevFields::FIdx: Add<B1>
// {
//     fn add(self, field: FieldData<NewDType>)
//         -> DataStore<FieldCons<NewField, Add1<PrevFields::FIdx>, NewDType, PrevFields>>
//         where NewDType: fmt::Debug
//     {
//         DataStore {
//             data: StorageCons {
//                 head: field,
//                 tail: self.data
//             }
//         }
//     }
// }

// impl<Fields> DataStore<Fields>
//     where Fields: AssocStorage,
// {
//     pub fn map<Field, T, F, FOut>(&self, f: F) -> FOut
//         where F: FnOnce(&Vec<Option<T>>) -> FOut,
//               Fields::Storage: Map<Fields, T, F, FOut>
//     {
//         self.data.map(f)
//     }
// }

// pub trait Map<Fields, T, F, FOut>
//     where F: FnOnce(&Vec<Option<T>>) -> FOut
// {
//     fn map(&self, f: F) -> FOut;
// }
// impl<Fields, T, F, FOut, Tail> Map<Fields, T, F, FOut> for StorageCons<Vec<Option<T>>, Tail>
//     where F: FnOnce(&Vec<Option<T>>) -> FOut,
//           Fields: AssocStorage
// {
//     fn map(&self, f: F) -> FOut {
//         f(&self.head)
//     }
// }


#[cfg(test)]
mod tests {

    use std::fmt::Debug;
    use std::path::Path;

    use csv_sniffer::metadata::Metadata;

    use source::csv::{CsvSource, CsvReader, IntoCsvSrcSpec};
    use field::Value;
    // use data_types::csv::*;
    use select::FieldSelect;
    use super::DataStore;
    use cons::*;
    use view::DataView;
    use label::{LookupElemByLabel};

    fn load_csv_file<Spec>(filename: &str, spec: Spec)
        -> (CsvReader<Spec::CsvSrcSpec>, Metadata)
        where Spec: IntoCsvSrcSpec
        // where CsvSrcSpec: FromSpec<Spec>
        // where Spec: Debug// + FieldSpecs<Types> + AssocFields + AttachSrcPos
    {
        let data_filepath = Path::new(file!()) // start as this file
            .parent().unwrap()                 // navigate up to src directory
            .parent().unwrap()                 // navigate up to root directory
            .join("tests")                     // navigate into integration tests directory            .join("data")                      // navigate into data directory
            .join("data")                      // navigate into data directory
            .join(filename);                   // navigate to target file

        let source = CsvSource::new(data_filepath.into()).unwrap();
        // let csv_rdr = CsvReader::new(&source, spec);
        // csv_rdr.adsjfiaosj();
        (CsvReader::new(&source, spec).unwrap(), source.metadata().clone())
    }

    #[test]
    fn storage_create() {
        let ds = DataStore::<Nil>::empty();
        #[derive(Debug)]
        struct Test;
        let ds = ds.add_labeled_field_from_iter::<Test, _, _, _>(
            vec![Value::Exists(4u64), Value::Exists(1), Value::Na, Value::Exists(3),
                 Value::Exists(7), Value::Exists(8), Value::Na]
        );
        // println!("{:?}", ds);

        spec![
            let gdp_spec = {
                CountryName("Country Name"): String,
                CountryCode("Country Code"): String,
                Year1983("1983"): f64,
            };
        ];
        // println!("{:?}", gdp_spec);

        // gdp_spec.tail.tail.head.ajdfiaoj();

        // ds.adjiaofj();
        let (mut csv_rdr, _metadata) = load_csv_file("gdp.csv", gdp_spec);
        let ds = csv_rdr.read().unwrap();

        // LookupElemByLabel::<CountryName>::elem(&ds.data).adjfiaoj();

        // println!("{:?}", ds);
        println!("{:?}", ds.field::<CountryName::Label>());

        // let dv = ds.into_view();
        // use view::LookupFrameByLabel;
        // use typenum::UTerm;
        // use label::Label;
        // dv.select_frame_by_label::<Label<UTerm>>();

        // println!("{}", dv);
        // println!("{:?}", csv_rdr);
        // println!("{:?}", metadata);


    }
}
