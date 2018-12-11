use std::fmt;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::Add;
use std::rc::Rc;

use typenum::{bit::B1, uint::UTerm};

use access::DataIndex;
use cons::*;
use field::{FieldData, Value};
use fieldlist::{FieldCons, FieldPayloadCons, FieldSpec};
use frame::DataFrame;
use label::*;
use select::{FieldSelect, SelectFieldByLabel};
use view::{DataView, FrameLookupCons, ViewFrameCons};

pub type StorageCons<Label, DType, Tail> =
    FieldPayloadCons<Label, DType, Rc<FieldData<DType>>, Tail>;

#[derive(Debug)]
pub struct DataStore<Fields: AssocStorage> {
    data: Fields::Storage,
}

pub trait AssocStorage {
    type Storage: Debug;
}
impl<Label, DType, Tail> AssocStorage for FieldCons<Label, DType, Tail>
where
    Tail: AssocStorage,
    Label: Debug,
    DType: Debug,
{
    type Storage = StorageCons<Label, DType, Tail::Storage>;
}
impl AssocStorage for Nil {
    type Storage = Nil;
}

impl<Fields> DataStore<Fields>
where
    Fields: AssocStorage,
{
    /// Generate and return an empty data store
    pub fn empty() -> DataStore<Nil> {
        DataStore { data: Nil }
    }
}

pub trait NRows {
    fn nrows(&self) -> usize;
}
impl NRows for Nil {
    fn nrows(&self) -> usize {
        0
    }
}
impl<Label, DType, Tail> NRows for StorageCons<Label, DType, Tail> {
    fn nrows(&self) -> usize {
        self.head.value_ref().len()
    }
}

impl<Fields> NRows for DataStore<Fields>
where
    Fields: AssocStorage,
    Fields::Storage: NRows,
{
    fn nrows(&self) -> usize {
        self.data.nrows()
    }
}

// ways to add:
// - push_field(FieldData) -> DataStore<...>
// - push_field_from_iter(Iterator<Item=T>) -> DataStore<...>
// - new_field::<Field>() -> DataStore<...>
// - field_mut::<Field>() -> DataIndexMut<Item=T>
pub type NewFieldStorage<NewLabel, NewDType> =
    Labeled<NewLabel, TypedValue<NewDType, Rc<FieldData<NewDType>>>>;

pub type AddedField<PrevFields, NewLabel, NewDType> =
    <PrevFields as PushBack<FieldSpec<NewLabel, NewDType>>>::Output;

// pub trait PushField<NewLabel, NewDType>
// {
//     type OutputFields: AssocStorage;

//     fn push_field(self, data: FieldData<NewDType>)
//         -> DataStore<Self::OutputFields>;
// }
// impl<PrevFields, NewLabel, NewDType> PushField<NewLabel, NewDType>
//     for DataStore<PrevFields>
//     where PrevFields: AssocStorage,
//           NewLabel: Debug,
//           NewDType: Debug,
// {
//     type OutputFields = FieldCons<NewLabel, NewDType, PrevFields>;

//     fn push_field(self, data: FieldData<NewDType>)
//         -> DataStore<Self::OutputFields>
//     {
//         DataStore {
//             data: StorageCons{
//                 head: TypedValue::from(Rc::new(data)).into(),
//                 tail: self.data
//             }
//         }
//     }
// }

pub trait AddField<NewLabel, NewDType> {
    type OutputFields: AssocStorage;

    fn add_field(self, data: FieldData<NewDType>) -> DataStore<Self::OutputFields>;
}
impl<PrevFields, NewLabel, NewDType> AddField<NewLabel, NewDType> for DataStore<PrevFields>
where
    PrevFields: AssocStorage + PushBack<FieldSpec<NewLabel, NewDType>>,
    AddedField<PrevFields, NewLabel, NewDType>: AssocStorage,
    PrevFields::Storage: PushBack<
        NewFieldStorage<NewLabel, NewDType>,
        Output = <AddedField<PrevFields, NewLabel, NewDType> as AssocStorage>::Storage,
    >,
    NewLabel: Debug,
    NewDType: Debug,
{
    type OutputFields = AddedField<PrevFields, NewLabel, NewDType>;

    fn add_field(self, data: FieldData<NewDType>) -> DataStore<Self::OutputFields> {
        DataStore {
            data: self.data.push_back(TypedValue::from(Rc::new(data)).into()),
        }
    }
}

// pub trait PushFieldFromValueIter<NewLabel, NewDType> {
//     type OutputFields: AssocStorage;

//     fn push_field_from_value_iter<IntoIter, Iter>(self, iter: IntoIter)
//         -> DataStore<Self::OutputFields>
//         where Iter: Iterator<Item=Value<NewDType>>,
//               IntoIter: IntoIterator<IntoIter=Iter, Item=Value<NewDType>>;
// }
// impl<PrevFields, NewLabel, NewDType> PushFieldFromValueIter<NewLabel, NewDType>
//     for DataStore<PrevFields>
//     where PrevFields: AssocStorage,
//           NewDType: Default + Clone,
//           NewLabel: Debug, NewDType: Debug,
// {
//     type OutputFields = FieldCons<NewLabel, NewDType, PrevFields>;

//     fn push_field_from_value_iter<IntoIter, Iter>(self, iter: IntoIter)
//         -> DataStore<Self::OutputFields>
//         where Iter: Iterator<Item=Value<NewDType>>,
//               IntoIter: IntoIterator<IntoIter=Iter, Item=Value<NewDType>>
//     {
//         DataStore {
//             data: StorageCons {
//                 head: TypedValue::from(Rc::new(iter.into_iter().collect::<FieldData<NewDType>>()))
//                     .into(),
//                 tail: self.data
//             }
//         }
//     }
// }

pub trait AddFieldFromValueIter<NewLabel, NewDType> {
    type OutputFields: AssocStorage;

    fn add_field_from_value_iter<IntoIter, Iter>(
        self,
        iter: IntoIter,
    ) -> DataStore<Self::OutputFields>
    where
        Iter: Iterator<Item = Value<NewDType>>,
        IntoIter: IntoIterator<IntoIter = Iter, Item = Value<NewDType>>;
}
impl<PrevFields, NewLabel, NewDType> AddFieldFromValueIter<NewLabel, NewDType>
    for DataStore<PrevFields>
where
    PrevFields: AssocStorage + PushBack<FieldSpec<NewLabel, NewDType>>,
    AddedField<PrevFields, NewLabel, NewDType>: AssocStorage,
    PrevFields::Storage: PushBack<
        NewFieldStorage<NewLabel, NewDType>,
        Output = <AddedField<PrevFields, NewLabel, NewDType> as AssocStorage>::Storage,
    >,
    NewLabel: Debug,
    NewDType: Default + Debug,
{
    type OutputFields = AddedField<PrevFields, NewLabel, NewDType>;

    fn add_field_from_value_iter<IntoIter, Iter>(
        self,
        iter: IntoIter,
    ) -> DataStore<Self::OutputFields>
    where
        Iter: Iterator<Item = Value<NewDType>>,
        IntoIter: IntoIterator<IntoIter = Iter, Item = Value<NewDType>>,
    {
        DataStore {
            data: self.data.push_back(
                TypedValue::from(Rc::new(iter.into_iter().collect::<FieldData<NewDType>>())).into(),
            ),
        }
    }
}

pub trait AddFieldFromIter<NewLabel, NewDType> {
    type OutputFields: AssocStorage;

    fn add_field_from_iter<IntoIter, Iter>(self, iter: IntoIter) -> DataStore<Self::OutputFields>
    where
        Iter: Iterator<Item = NewDType>,
        IntoIter: IntoIterator<IntoIter = Iter, Item = NewDType>;
}
impl<PrevFields, NewLabel, NewDType> AddFieldFromIter<NewLabel, NewDType> for DataStore<PrevFields>
where
    PrevFields: AssocStorage + PushBack<FieldSpec<NewLabel, NewDType>>,
    AddedField<PrevFields, NewLabel, NewDType>: AssocStorage,
    PrevFields::Storage: PushBack<
        NewFieldStorage<NewLabel, NewDType>,
        Output = <AddedField<PrevFields, NewLabel, NewDType> as AssocStorage>::Storage,
    >,
    NewLabel: Debug,
    NewDType: Debug,
{
    type OutputFields = AddedField<PrevFields, NewLabel, NewDType>;

    fn add_field_from_iter<IntoIter, Iter>(self, iter: IntoIter) -> DataStore<Self::OutputFields>
    where
        Iter: Iterator<Item = NewDType>,
        IntoIter: IntoIterator<IntoIter = Iter, Item = NewDType>,
    {
        DataStore {
            data: self.data.push_back(
                TypedValue::from(Rc::new(iter.into_iter().collect::<FieldData<NewDType>>())).into(),
            ),
        }
    }
}

pub trait AddClonedFieldFromIter<NewLabel, NewDType> {
    type OutputFields: AssocStorage;

    fn add_cloned_field_from_iter<'a, IntoIter, Iter>(
        self,
        iter: IntoIter,
    ) -> DataStore<Self::OutputFields>
    where
        Iter: Iterator<Item = &'a NewDType>,
        IntoIter: IntoIterator<IntoIter = Iter, Item = &'a NewDType>,
        NewDType: 'a;
}
impl<PrevFields, NewLabel, NewDType> AddClonedFieldFromIter<NewLabel, NewDType>
    for DataStore<PrevFields>
where
    PrevFields: AssocStorage + PushBack<FieldSpec<NewLabel, NewDType>>,
    AddedField<PrevFields, NewLabel, NewDType>: AssocStorage,
    PrevFields::Storage: PushBack<
        NewFieldStorage<NewLabel, NewDType>,
        Output = <AddedField<PrevFields, NewLabel, NewDType> as AssocStorage>::Storage,
    >,
    NewLabel: Debug,
    NewDType: Clone + Debug,
{
    type OutputFields = AddedField<PrevFields, NewLabel, NewDType>;

    fn add_cloned_field_from_iter<'a, IntoIter, Iter>(
        self,
        iter: IntoIter,
    ) -> DataStore<Self::OutputFields>
    where
        Iter: Iterator<Item = &'a NewDType>,
        IntoIter: IntoIterator<IntoIter = Iter, Item = &'a NewDType>,
        NewDType: 'a,
    {
        DataStore {
            data: self.data.push_back(
                TypedValue::from(Rc::new(
                    iter.into_iter()
                        .map(|x| x.clone())
                        .collect::<FieldData<NewDType>>(),
                ))
                .into(),
            ),
        }
    }
}

pub trait AddClonedFieldFromValueIter<NewLabel, NewDType> {
    type OutputFields: AssocStorage;

    fn add_cloned_field_from_value_iter<'a, IntoIter, Iter>(
        self,
        iter: IntoIter,
    ) -> DataStore<Self::OutputFields>
    where
        Iter: Iterator<Item = Value<&'a NewDType>>,
        IntoIter: IntoIterator<IntoIter = Iter, Item = Value<&'a NewDType>>,
        NewDType: 'a;
}
impl<PrevFields, NewLabel, NewDType> AddClonedFieldFromValueIter<NewLabel, NewDType>
    for DataStore<PrevFields>
where
    PrevFields: AssocStorage + PushBack<FieldSpec<NewLabel, NewDType>>,
    AddedField<PrevFields, NewLabel, NewDType>: AssocStorage,
    PrevFields::Storage: PushBack<
        NewFieldStorage<NewLabel, NewDType>,
        Output = <AddedField<PrevFields, NewLabel, NewDType> as AssocStorage>::Storage,
    >,
    NewLabel: Debug,
    NewDType: Default + Clone + Debug,
{
    type OutputFields = AddedField<PrevFields, NewLabel, NewDType>;

    fn add_cloned_field_from_value_iter<'a, IntoIter, Iter>(
        self,
        iter: IntoIter,
    ) -> DataStore<Self::OutputFields>
    where
        Iter: Iterator<Item = Value<&'a NewDType>>,
        IntoIter: IntoIterator<IntoIter = Iter, Item = Value<&'a NewDType>>,
        NewDType: 'a,
    {
        DataStore {
            data: self.data.push_back(
                TypedValue::from(Rc::new(
                    iter.into_iter()
                        .map(|x| x.clone())
                        .collect::<FieldData<NewDType>>(),
                ))
                .into(),
            ),
        }
    }
}

// pub trait PushEmptyField<NewLabel, NewDType> {
//     type OutputFields: AssocStorage;

//     fn push_empty_field(self)
//         -> DataStore<Self::OutputFields>;
// }
// impl<PrevFields, NewLabel, NewDType> PushEmptyField<NewLabel, NewDType>
//     for DataStore<PrevFields>
//     where PrevFields: AssocStorage,
//           NewLabel: Debug, NewDType: Debug,
// {
//     type OutputFields = FieldCons<NewLabel, NewDType, PrevFields>;

//     fn push_empty_field(self)
//         -> DataStore<Self::OutputFields>
//     {
//         DataStore {
//             data: StorageCons {
//                 head: TypedValue::from(Rc::new(FieldData::default())).into(),
//                 tail: self.data
//             }
//         }
//     }
// }

pub trait AddEmptyField<NewLabel, NewDType> {
    type OutputFields: AssocStorage;

    fn add_empty_field(self) -> DataStore<Self::OutputFields>;
}
impl<PrevFields, NewLabel, NewDType> AddEmptyField<NewLabel, NewDType> for DataStore<PrevFields>
where
    PrevFields: AssocStorage + PushBack<FieldSpec<NewLabel, NewDType>>,
    AddedField<PrevFields, NewLabel, NewDType>: AssocStorage,
    PrevFields::Storage: PushBack<
        NewFieldStorage<NewLabel, NewDType>,
        Output = <AddedField<PrevFields, NewLabel, NewDType> as AssocStorage>::Storage,
    >,
    NewLabel: Debug,
    NewDType: Debug,
{
    type OutputFields = AddedField<PrevFields, NewLabel, NewDType>;

    fn add_empty_field(self) -> DataStore<Self::OutputFields> {
        DataStore {
            data: self
                .data
                .push_back(TypedValue::from(Rc::new(FieldData::default())).into()),
        }
    }
}

// #[macro_export]
// macro_rules! push_field {
//     ($ds:ident<$fields:ty>.$new_label:ident = $data:expr;) => {
//         pub type $new_label = $crate::label::Label<
//             typenum::Add1<<$fields as $crate::label::Natural>::Nat>
//         >;
//         let $ds = $ds.push_field::<$new_label, _>($data);
//     }
// }
// #[macro_export]
// macro_rules! push_field_from_iter {
//     ($ds:ident<$fields:ty>.$new_label:ident = $iter:expr;) => {
//         pub type $new_label = $crate::label::Label<
//             typenum::Add1<<$fields as $crate::label::Natural>::Nat>
//         >;
//         let $ds = $ds.push_field_from_iter::<$new_label, _>($iter);
//     }
// }

impl<PrevFields> DataStore<PrevFields>
where
    PrevFields: AssocStorage,
{
    // pub fn push_field<NewLabel, NewDType>(self, data: FieldData<NewDType>)
    //     -> DataStore<<Self as PushField<NewLabel, NewDType>>::OutputFields>
    //     where Self: PushField<NewLabel, NewDType>
    // {
    //     PushField::push_field(self, data)
    // }

    pub fn add_field<NewLabel, NewDType>(
        self,
        data: FieldData<NewDType>,
    ) -> DataStore<<Self as AddField<NewLabel, NewDType>>::OutputFields>
    where
        Self: AddField<NewLabel, NewDType>,
    {
        AddField::add_field(self, data)
    }

    // pub fn push_field_from_iter<NewLabel, NewDType, IntoIter, Iter>(self, iter: IntoIter)
    //     -> DataStore<<Self as PushFieldFromIter<NewLabel, NewDType>>::OutputFields>
    //     where Iter: Iterator<Item=NewDType>,
    //           IntoIter: IntoIterator<IntoIter=Iter, Item=NewDType>,
    //           Self: PushFieldFromIter<NewLabel, NewDType>
    // {
    //     PushFieldFromIter::push_field_from_iter(self, iter)
    // }

    pub fn add_field_from_iter<NewLabel, NewDType, IntoIter, Iter>(
        self,
        iter: IntoIter,
    ) -> DataStore<<Self as AddFieldFromIter<NewLabel, NewDType>>::OutputFields>
    where
        Iter: Iterator<Item = NewDType>,
        IntoIter: IntoIterator<IntoIter = Iter, Item = NewDType>,
        Self: AddFieldFromIter<NewLabel, NewDType>,
    {
        AddFieldFromIter::add_field_from_iter(self, iter)
    }

    pub fn add_cloned_field_from_iter<'a, NewLabel, NewDType, IntoIter, Iter>(
        self,
        iter: IntoIter,
    ) -> DataStore<<Self as AddClonedFieldFromIter<NewLabel, NewDType>>::OutputFields>
    where
        Iter: Iterator<Item = &'a NewDType>,
        IntoIter: IntoIterator<IntoIter = Iter, Item = &'a NewDType>,
        Self: AddClonedFieldFromIter<NewLabel, NewDType>,
        NewDType: 'a,
    {
        AddClonedFieldFromIter::add_cloned_field_from_iter(self, iter)
    }

    // pub fn push_field_from_value_iter<NewLabel, NewDType, IntoIter, Iter>(self, iter: IntoIter)
    //     -> DataStore<<Self as PushFieldFromValueIter<NewLabel, NewDType>>::OutputFields>
    //     where Iter: Iterator<Item=Value<NewDType>>,
    //           IntoIter: IntoIterator<IntoIter=Iter, Item=Value<NewDType>>,
    //           Self: PushFieldFromValueIter<NewLabel, NewDType>
    // {
    //     PushFieldFromValueIter::push_field_from_value_iter(self, iter)
    // }

    pub fn add_field_from_value_iter<NewLabel, NewDType, IntoIter, Iter>(
        self,
        iter: IntoIter,
    ) -> DataStore<<Self as AddFieldFromValueIter<NewLabel, NewDType>>::OutputFields>
    where
        Iter: Iterator<Item = Value<NewDType>>,
        IntoIter: IntoIterator<IntoIter = Iter, Item = Value<NewDType>>,
        Self: AddFieldFromValueIter<NewLabel, NewDType>,
    {
        AddFieldFromValueIter::add_field_from_value_iter(self, iter)
    }

    pub fn add_cloned_field_from_value_iter<'a, NewLabel, NewDType, IntoIter, Iter>(
        self,
        iter: IntoIter,
    ) -> DataStore<<Self as AddClonedFieldFromValueIter<NewLabel, NewDType>>::OutputFields>
    where
        Iter: Iterator<Item = Value<&'a NewDType>>,
        IntoIter: IntoIterator<IntoIter = Iter, Item = Value<&'a NewDType>>,
        Self: AddClonedFieldFromValueIter<NewLabel, NewDType>,
        NewDType: 'a,
    {
        AddClonedFieldFromValueIter::add_cloned_field_from_value_iter(self, iter)
    }

    pub fn add_empty_field<NewLabel, NewDType>(
        self,
    ) -> DataStore<<Self as AddEmptyField<NewLabel, NewDType>>::OutputFields>
    where
        Self: AddEmptyField<NewLabel, NewDType>,
    {
        AddEmptyField::add_empty_field(self)
    }
}

// impl<PrevFields> DataStore<PrevFields>
//     where PrevFields: AssocStorage + LabelIndex,
//           <PrevFields as LabelIndex>::Idx: Add<B1>
// {
//     pub fn push_field<NewDType>(self, data: FieldData<NewDType>)
//         -> DataStore<<Self as PushField<NextLabelIndex<PrevFields>, NewDType>>::OutputFields>
//         where NewDType: fmt::Debug,
//               Self: PushField<NextLabelIndex<PrevFields>, NewDType>
//     {
//         PushField::push_field(self, data)
//     }

//     pub fn push_field_from_iter<NewDType, IntoIter, Iter>(self, iter: IntoIter)
//         -> DataStore<
//             <Self as PushFieldFromIter<NextLabelIndex<PrevFields>, NewDType>>::OutputFields
//         >
//         where Iter: Iterator<Item=Value<NewDType>>,
//               IntoIter: IntoIterator<IntoIter=Iter, Item=Value<NewDType>>,
//               NewDType: fmt::Debug + Default + Clone,
//               Self: PushFieldFromIter<NextLabelIndex<PrevFields>, NewDType>
//     {
//         PushFieldFromIter::push_field_from_iter(self, iter)
//     }

//     pub fn push_empty_field<NewDType>(self)
//         -> DataStore<
//             <Self as PushEmptyField<NextLabelIndex<PrevFields>, NewDType>>::OutputFields
//         >
//         where NewDType: fmt::Debug,
//               Self: PushEmptyField<NextLabelIndex<PrevFields>, NewDType>
//     {
//         PushEmptyField::push_empty_field(self)
//     }
// }

impl<Label, Fields> SelectFieldByLabel<Label> for DataStore<Fields>
where
    Fields: AssocStorage,
    Fields::Storage: LookupElemByLabel<Label>,
    ElemOf<Fields::Storage, Label>: Typed,
    ElemOf<Fields::Storage, Label>:
        Valued<Value = Rc<FieldData<TypeOfElemOf<Fields::Storage, Label>>>>,
    TypeOfElemOf<Fields::Storage, Label>: Debug,
{
    type Output =
        Rc<FieldData<<<Fields::Storage as LookupElemByLabel<Label>>::Elem as Typed>::DType>>;

    fn select_field(&self) -> Self::Output {
        Rc::clone(LookupElemByLabel::<Label>::elem(&self.data).value_ref())
    }
}
impl<Fields> FieldSelect for DataStore<Fields> where Fields: AssocStorage {}

// pub struct FrameLookupLabel<FrameLabel>
// {
//     _marker: PhantomData<FrameLabel>
// }
// impl<FrameLabel> Label for FrameLookupLabel<FrameLabel>
//     where FrameLabel: Label
// {
//     const NAME: &'static str = FrameLabel::NAME;
// }
// impl<FrameLabel> Identifier for FrameLookupLabel<FrameLabel>
//     where FrameLabel: Identifier
// {
//     type Ident = Ident<Self::Namespace, Self::Natural>;
//     type Namespace = LocalNamespace;
//     type Natural = <FrameLabel as Identifier>::Natural;
// }

pub trait AssocFrameLookup {
    type Output;
}
impl AssocFrameLookup for Nil {
    type Output = Nil;
}
impl<Label, Value, Tail> AssocFrameLookup for LVCons<Label, Value, Tail>
where
    Tail: AssocFrameLookup,
{
    type Output = FrameLookupCons<Label, UTerm, Label, <Tail as AssocFrameLookup>::Output>;
}

impl<Fields> DataStore<Fields>
where
    Fields: AssocStorage + AssocFrameLookup,
{
    pub fn into_view(self) -> <Self as IntoView>::Output
    where
        Self: IntoView,
    {
        IntoView::into_view(self)
    }
}
pub trait IntoView {
    type Output;
    fn into_view(self) -> Self::Output;
}
impl<Fields> IntoView for DataStore<Fields>
where
    Fields: AssocStorage + AssocFrameLookup,
{
    type Output = DataView<<Fields as AssocFrameLookup>::Output, ViewFrameCons<UTerm, Fields, Nil>>;

    fn into_view(self) -> Self::Output {
        DataView::new(ViewFrameCons {
            head: DataFrame::from(self).into(),
            tail: Nil,
        })
    }
}

#[cfg(test)]
mod tests {

    use std::fmt::Debug;
    use std::path::Path;

    use csv_sniffer::metadata::Metadata;

    use field::Value;
    use source::csv::{CsvReader, CsvSource, IntoCsvSrcSpec};
    // use data_types::csv::*;
    use super::DataStore;
    use cons::*;
    use label::LookupElemByLabel;
    use select::FieldSelect;
    use view::DataView;

    fn load_csv_file<Spec>(filename: &str, spec: Spec) -> (CsvReader<Spec::CsvSrcSpec>, Metadata)
    where
        Spec: IntoCsvSrcSpec, // where CsvSrcSpec: FromSpec<Spec>
                              // where Spec: Debug// + FieldSpecs<Types> + AssocFields + AttachSrcPos
    {
        let data_filepath = Path::new(file!()) // start as this file
            .parent()
            .unwrap() // navigate up to src directory
            .parent()
            .unwrap() // navigate up to root directory
            .join("tests") // navigate into integration tests directory            .join("data")                      // navigate into data directory
            .join("data") // navigate into data directory
            .join(filename); // navigate to target file

        let source = CsvSource::new(data_filepath.into()).unwrap();
        // let csv_rdr = CsvReader::new(&source, spec);
        // csv_rdr.adsjfiaosj();
        (
            CsvReader::new(&source, spec).unwrap(),
            source.metadata().clone(),
        )
    }

    namespace![
        pub namespace gdp {
            field CountryName: String;
            field CountryCode: String;
            field Year1983: f64;
        }
    ];

    #[test]
    fn storage_create() {
        let ds = DataStore::<Nil>::empty();
        #[derive(Debug)]
        struct Test;
        let ds = ds.add_field_from_iter::<Test, _, _, _>(vec![
            Value::Exists(4u64),
            Value::Exists(1),
            Value::Na,
            Value::Exists(3),
            Value::Exists(7),
            Value::Exists(8),
            Value::Na,
        ]);
        // println!("{:?}", ds);

        let gdp_spec = spec![
            fieldname gdp::CountryName = "Country Name";
            fieldname gdp::CountryCode = "Country Code";
            fieldname gdp::Year1983 = "1983";
        ];
        // println!("{:?}", gdp_spec);

        // gdp_spec.tail.tail.head.ajdfiaoj();

        // ds.adjiaofj();
        let (mut csv_rdr, _metadata) = load_csv_file("gdp.csv", gdp_spec.clone());
        let ds = csv_rdr.read().unwrap();

        // LookupElemByLabel::<CountryName>::elem(&ds.data).adjfiaoj();

        // println!("{:?}", ds);
        println!("{:?}", ds.field::<gdp::CountryName>());

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
