use std::fmt::Debug;
use std::ops::Deref;
use std::rc::Rc;

#[cfg(feature = "serialize")]
use serde::ser::{Serialize, Serializer};
use typenum::uint::UTerm;

use access::DataIndex;
use cons::*;
use error;
use field::{FieldData, Value};
use fieldlist::{FieldCons, FieldPayloadCons, FieldSpec};
use frame::DataFrame;
use label::*;
use select::{FieldSelect, SelectFieldByLabel};
use view::{DataView, FrameLookupCons, ViewFrameCons};

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct DataRef<DType>(pub Rc<FieldData<DType>>);

impl<DType> DataRef<DType> {
    fn new(field: FieldData<DType>) -> DataRef<DType> {
        DataRef(Rc::new(field))
    }
}

impl<DType> Clone for DataRef<DType> {
    fn clone(&self) -> DataRef<DType> {
        DataRef(Rc::clone(&self.0))
    }
}

impl<T> Deref for DataRef<T> {
    type Target = FieldData<T>;

    fn deref(&self) -> &FieldData<T> {
        &self.0.deref()
    }
}

impl<T> From<FieldData<T>> for DataRef<T> {
    fn from(orig: FieldData<T>) -> DataRef<T> {
        DataRef(Rc::new(orig))
    }
}

impl<T> DataIndex for DataRef<T>
where
    FieldData<T>: DataIndex<DType = T>,
    T: Debug,
{
    type DType = T;

    fn get_datum(&self, idx: usize) -> error::Result<Value<&T>> {
        <FieldData<T> as DataIndex>::get_datum(&self.0, idx)
    }
    fn len(&self) -> usize {
        <FieldData<T> as DataIndex>::len(&self.0)
    }
}

#[cfg(feature = "serialize")]
impl<T> Serialize for DataRef<T>
where
    T: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.serialize(serializer)
    }
}

pub type StorageCons<Label, DType, Tail> = FieldPayloadCons<Label, DType, DataRef<DType>, Tail>;

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

pub type NewFieldStorage<NewLabel, NewDType> =
    Labeled<NewLabel, TypedValue<NewDType, DataRef<NewDType>>>;

macro_rules! make_add_field {
    (
        $add_trait:tt $add_fn:tt;
        $add_valiter_trait:tt $add_valiter_fn:tt;
        $add_iter_trait:tt $add_iter_fn:tt;
        $add_cloned_valiter_trait:tt $add_cloned_valiter_fn:tt;
        $add_cloned_iter_trait:tt $add_cloned_iter_fn:tt;
        $add_empty_trait:tt $add_empty_fn:tt;
        $push_trait:tt $push_fn:tt $pushed_alias:tt
    ) => {
        pub type $pushed_alias<PrevFields, NewLabel, NewDType> =
            <PrevFields as $push_trait<FieldSpec<NewLabel, NewDType>>>::Output;

        pub trait $add_trait<NewLabel, NewDType> {
            type OutputFields: AssocStorage;

            fn $add_fn(self, data: FieldData<NewDType>) -> DataStore<Self::OutputFields>;
        }

        impl<PrevFields, NewLabel, NewDType> $add_trait<NewLabel, NewDType>
            for DataStore<PrevFields>
        where
            PrevFields: AssocStorage + $push_trait<FieldSpec<NewLabel, NewDType>>,
            $pushed_alias<PrevFields, NewLabel, NewDType>: AssocStorage,
            PrevFields::Storage: $push_trait<
                NewFieldStorage<NewLabel, NewDType>,
                Output = <$pushed_alias<PrevFields, NewLabel, NewDType> as AssocStorage>::Storage,
            >,
            NewLabel: Debug,
            NewDType: Debug,
        {
            type OutputFields = $pushed_alias<PrevFields, NewLabel, NewDType>;

            fn $add_fn(self, data: FieldData<NewDType>) -> DataStore<Self::OutputFields> {
                DataStore {
                    data: self
                        .data
                        .$push_fn(TypedValue::from(DataRef::new(data)).into()),
                }
            }
        }

        pub trait $add_valiter_trait<NewLabel, NewDType> {
            type OutputFields: AssocStorage;

            fn $add_valiter_fn<IntoIter, Iter>(
                self,
                iter: IntoIter,
            ) -> DataStore<Self::OutputFields>
            where
                Iter: Iterator<Item = Value<NewDType>>,
                IntoIter: IntoIterator<IntoIter = Iter, Item = Value<NewDType>>;
        }
        impl<PrevFields, NewLabel, NewDType> $add_valiter_trait<NewLabel, NewDType>
            for DataStore<PrevFields>
        where
            PrevFields: AssocStorage + $push_trait<FieldSpec<NewLabel, NewDType>>,
            $pushed_alias<PrevFields, NewLabel, NewDType>: AssocStorage,
            PrevFields::Storage: $push_trait<
                NewFieldStorage<NewLabel, NewDType>,
                Output = <$pushed_alias<PrevFields, NewLabel, NewDType> as AssocStorage>::Storage,
            >,
            NewLabel: Debug,
            NewDType: Default + Debug,
        {
            type OutputFields = $pushed_alias<PrevFields, NewLabel, NewDType>;

            fn $add_valiter_fn<IntoIter, Iter>(
                self,
                iter: IntoIter,
            ) -> DataStore<Self::OutputFields>
            where
                Iter: Iterator<Item = Value<NewDType>>,
                IntoIter: IntoIterator<IntoIter = Iter, Item = Value<NewDType>>,
            {
                DataStore {
                    data: self.data.$push_fn(
                        TypedValue::from(DataRef::new(
                            iter.into_iter().collect::<FieldData<NewDType>>(),
                        ))
                        .into(),
                    ),
                }
            }
        }

        pub trait $add_iter_trait<NewLabel, NewDType> {
            type OutputFields: AssocStorage;

            fn $add_iter_fn<IntoIter, Iter>(self, iter: IntoIter) -> DataStore<Self::OutputFields>
            where
                Iter: Iterator<Item = NewDType>,
                IntoIter: IntoIterator<IntoIter = Iter, Item = NewDType>;
        }
        impl<PrevFields, NewLabel, NewDType> $add_iter_trait<NewLabel, NewDType>
            for DataStore<PrevFields>
        where
            PrevFields: AssocStorage + $push_trait<FieldSpec<NewLabel, NewDType>>,
            $pushed_alias<PrevFields, NewLabel, NewDType>: AssocStorage,
            PrevFields::Storage: $push_trait<
                NewFieldStorage<NewLabel, NewDType>,
                Output = <$pushed_alias<PrevFields, NewLabel, NewDType> as AssocStorage>::Storage,
            >,
            NewLabel: Debug,
            NewDType: Debug,
        {
            type OutputFields = $pushed_alias<PrevFields, NewLabel, NewDType>;

            fn $add_iter_fn<IntoIter, Iter>(self, iter: IntoIter) -> DataStore<Self::OutputFields>
            where
                Iter: Iterator<Item = NewDType>,
                IntoIter: IntoIterator<IntoIter = Iter, Item = NewDType>,
            {
                DataStore {
                    data: self.data.$push_fn(
                        TypedValue::from(DataRef::new(
                            iter.into_iter().collect::<FieldData<NewDType>>(),
                        ))
                        .into(),
                    ),
                }
            }
        }

        pub trait $add_cloned_valiter_trait<NewLabel, NewDType> {
            type OutputFields: AssocStorage;

            fn $add_cloned_valiter_fn<'a, IntoIter, Iter>(
                self,
                iter: IntoIter,
            ) -> DataStore<Self::OutputFields>
            where
                Iter: Iterator<Item = Value<&'a NewDType>>,
                IntoIter: IntoIterator<IntoIter = Iter, Item = Value<&'a NewDType>>,
                NewDType: 'a;
        }
        impl<PrevFields, NewLabel, NewDType> $add_cloned_valiter_trait<NewLabel, NewDType>
            for DataStore<PrevFields>
        where
            PrevFields: AssocStorage + $push_trait<FieldSpec<NewLabel, NewDType>>,
            $pushed_alias<PrevFields, NewLabel, NewDType>: AssocStorage,
            PrevFields::Storage: $push_trait<
                NewFieldStorage<NewLabel, NewDType>,
                Output = <$pushed_alias<PrevFields, NewLabel, NewDType> as AssocStorage>::Storage,
            >,
            NewLabel: Debug,
            NewDType: Default + Clone + Debug,
        {
            type OutputFields = $pushed_alias<PrevFields, NewLabel, NewDType>;

            fn $add_cloned_valiter_fn<'a, IntoIter, Iter>(
                self,
                iter: IntoIter,
            ) -> DataStore<Self::OutputFields>
            where
                Iter: Iterator<Item = Value<&'a NewDType>>,
                IntoIter: IntoIterator<IntoIter = Iter, Item = Value<&'a NewDType>>,
                NewDType: 'a,
            {
                DataStore {
                    data: self.data.$push_fn(
                        TypedValue::from(DataRef::new(
                            iter.into_iter()
                                .map(|x| x.clone())
                                .collect::<FieldData<NewDType>>(),
                        ))
                        .into(),
                    ),
                }
            }
        }

        pub trait $add_cloned_iter_trait<NewLabel, NewDType> {
            type OutputFields: AssocStorage;

            fn $add_cloned_iter_fn<'a, IntoIter, Iter>(
                self,
                iter: IntoIter,
            ) -> DataStore<Self::OutputFields>
            where
                Iter: Iterator<Item = &'a NewDType>,
                IntoIter: IntoIterator<IntoIter = Iter, Item = &'a NewDType>,
                NewDType: 'a;
        }
        impl<PrevFields, NewLabel, NewDType> $add_cloned_iter_trait<NewLabel, NewDType>
            for DataStore<PrevFields>
        where
            PrevFields: AssocStorage + $push_trait<FieldSpec<NewLabel, NewDType>>,
            $pushed_alias<PrevFields, NewLabel, NewDType>: AssocStorage,
            PrevFields::Storage: $push_trait<
                NewFieldStorage<NewLabel, NewDType>,
                Output = <$pushed_alias<PrevFields, NewLabel, NewDType> as AssocStorage>::Storage,
            >,
            NewLabel: Debug,
            NewDType: Clone + Debug,
        {
            type OutputFields = $pushed_alias<PrevFields, NewLabel, NewDType>;

            fn $add_cloned_iter_fn<'a, IntoIter, Iter>(
                self,
                iter: IntoIter,
            ) -> DataStore<Self::OutputFields>
            where
                Iter: Iterator<Item = &'a NewDType>,
                IntoIter: IntoIterator<IntoIter = Iter, Item = &'a NewDType>,
                NewDType: 'a,
            {
                DataStore {
                    data: self.data.$push_fn(
                        TypedValue::from(DataRef::new(
                            iter.into_iter()
                                .map(|x| x.clone())
                                .collect::<FieldData<NewDType>>(),
                        ))
                        .into(),
                    ),
                }
            }
        }

        pub trait $add_empty_trait<NewLabel, NewDType> {
            type OutputFields: AssocStorage;

            fn $add_empty_fn(self) -> DataStore<Self::OutputFields>;
        }
        impl<PrevFields, NewLabel, NewDType> $add_empty_trait<NewLabel, NewDType>
            for DataStore<PrevFields>
        where
            PrevFields: AssocStorage + $push_trait<FieldSpec<NewLabel, NewDType>>,
            $pushed_alias<PrevFields, NewLabel, NewDType>: AssocStorage,
            PrevFields::Storage: $push_trait<
                NewFieldStorage<NewLabel, NewDType>,
                Output = <$pushed_alias<PrevFields, NewLabel, NewDType> as AssocStorage>::Storage,
            >,
            NewLabel: Debug,
            NewDType: Debug,
        {
            type OutputFields = $pushed_alias<PrevFields, NewLabel, NewDType>;

            fn $add_empty_fn(self) -> DataStore<Self::OutputFields> {
                DataStore {
                    data: self
                        .data
                        .$push_fn(TypedValue::from(DataRef::new(FieldData::default())).into()),
                }
            }
        }

        impl<PrevFields> DataStore<PrevFields>
        where
            PrevFields: AssocStorage,
        {
            pub fn $add_fn<NewLabel, NewDType>(
                self,
                data: FieldData<NewDType>,
            ) -> DataStore<<Self as $add_trait<NewLabel, NewDType>>::OutputFields>
            where
                Self: $add_trait<NewLabel, NewDType>,
            {
                $add_trait::$add_fn(self, data)
            }

            pub fn $add_valiter_fn<NewLabel, NewDType, IntoIter, Iter>(
                self,
                iter: IntoIter,
            ) -> DataStore<<Self as $add_valiter_trait<NewLabel, NewDType>>::OutputFields>
            where
                Iter: Iterator<Item = Value<NewDType>>,
                IntoIter: IntoIterator<IntoIter = Iter, Item = Value<NewDType>>,
                Self: $add_valiter_trait<NewLabel, NewDType>,
            {
                $add_valiter_trait::$add_valiter_fn(self, iter)
            }

            pub fn $add_iter_fn<NewLabel, NewDType, IntoIter, Iter>(
                self,
                iter: IntoIter,
            ) -> DataStore<<Self as $add_iter_trait<NewLabel, NewDType>>::OutputFields>
            where
                Iter: Iterator<Item = NewDType>,
                IntoIter: IntoIterator<IntoIter = Iter, Item = NewDType>,
                Self: $add_iter_trait<NewLabel, NewDType>,
            {
                $add_iter_trait::$add_iter_fn(self, iter)
            }

            pub fn $add_cloned_valiter_fn<'a, NewLabel, NewDType, IntoIter, Iter>(
                self,
                iter: IntoIter,
            ) -> DataStore<<Self as $add_cloned_valiter_trait<NewLabel, NewDType>>::OutputFields>
            where
                Iter: Iterator<Item = Value<&'a NewDType>>,
                IntoIter: IntoIterator<IntoIter = Iter, Item = Value<&'a NewDType>>,
                Self: $add_cloned_valiter_trait<NewLabel, NewDType>,
                NewDType: 'a,
            {
                $add_cloned_valiter_trait::$add_cloned_valiter_fn(self, iter)
            }

            pub fn $add_cloned_iter_fn<'a, NewLabel, NewDType, IntoIter, Iter>(
                self,
                iter: IntoIter,
            ) -> DataStore<<Self as $add_cloned_iter_trait<NewLabel, NewDType>>::OutputFields>
            where
                Iter: Iterator<Item = &'a NewDType>,
                IntoIter: IntoIterator<IntoIter = Iter, Item = &'a NewDType>,
                Self: $add_cloned_iter_trait<NewLabel, NewDType>,
                NewDType: 'a,
            {
                $add_cloned_iter_trait::$add_cloned_iter_fn(self, iter)
            }

            pub fn $add_empty_fn<NewLabel, NewDType>(
                self,
            ) -> DataStore<<Self as $add_empty_trait<NewLabel, NewDType>>::OutputFields>
            where
                Self: $add_empty_trait<NewLabel, NewDType>,
            {
                $add_empty_trait::$add_empty_fn(self)
            }
        }
    };
}

make_add_field![
    PushFrontField push_front_field;
    PushFrontFromValueIter push_front_from_value_iter;
    PushFrontFromIter push_front_from_iter;
    PushFrontClonedFromValueIter push_front_cloned_from_value_iter;
    PushFrontClonedFromIter push_front_cloned_from_iter;
    PushFrontEmpty push_front_empty;
    PushFront push_front PushedFrontField
];

make_add_field![
    PushBackField push_back_field;
    PushBackFromValueIter push_back_from_value_iter;
    PushBackFromIter push_back_from_iter;
    PushBackClonedFromValueIter push_back_cloned_from_value_iter;
    PushBackClonedFromIter push_back_cloned_from_iter;
    PushBackEmpty push_back_empty;
    PushBack push_back PushedBackField
];

impl<Label, Fields> SelectFieldByLabel<Label> for DataStore<Fields>
where
    Fields: AssocStorage,
    Fields::Storage: LookupElemByLabel<Label>,
    ElemOf<Fields::Storage, Label>: Typed,
    ElemOf<Fields::Storage, Label>: Valued<Value = DataRef<TypeOfElemOf<Fields::Storage, Label>>>,
    DataRef<TypeOfElemOf<Fields::Storage, Label>>: DataIndex,
    TypeOfElemOf<Fields::Storage, Label>: Debug,
{
    type Output = DataRef<<<Fields::Storage as LookupElemByLabel<Label>>::Elem as Typed>::DType>;

    fn select_field(&self) -> Self::Output {
        DataRef::clone(LookupElemByLabel::<Label>::elem(&self.data).value_ref())
    }
}
impl<Fields> FieldSelect for DataStore<Fields> where Fields: AssocStorage {}

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
    use typenum::U0;

    use csv_sniffer::metadata::Metadata;

    use super::{DataStore, NRows};
    use cons::*;
    use field::Value;
    use select::FieldSelect;
    use source::csv::{CsvReader, CsvSource, IntoCsvSrcSpec};

    fn load_csv_file<Spec>(filename: &str, spec: Spec) -> (CsvReader<Spec::CsvSrcSpec>, Metadata)
    where
        Spec: IntoCsvSrcSpec,
        <Spec as IntoCsvSrcSpec>::CsvSrcSpec: Debug,
    {
        let data_filepath = Path::new(file!()) // start as this file
            .parent()
            .unwrap() // navigate up to src directory
            .parent()
            .unwrap() // navigate up to root directory
            .join("tests") // navigate into integration tests directory
            .join("data") // navigate into data directory
            .join(filename); // navigate to target file

        let source = CsvSource::new(data_filepath.into()).unwrap();
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

        type TestNamespace = U0;
        first_label![Test, TestNamespace, u64];

        let data = vec![
            Value::Exists(4u64),
            Value::Exists(1),
            Value::Na,
            Value::Exists(3),
            Value::Exists(7),
            Value::Exists(8),
            Value::Na,
        ];
        let expected_nrows = data.len();

        let ds = ds.push_back_from_iter::<Test, _, _, _>(data);
        println!("{:?}", ds);
        assert_eq!(ds.nrows(), expected_nrows);
        assert_eq!(ds.field::<Test>().len(), expected_nrows);

        let gdp_spec = spec![
            fieldname gdp::CountryName = "Country Name";
            fieldname gdp::CountryCode = "Country Code";
            fieldname gdp::Year1983 = "1983";
        ];

        let (mut csv_rdr, _metadata) = load_csv_file("gdp.csv", gdp_spec.clone());
        let ds = csv_rdr.read().unwrap();
        const EXPECTED_GDP_NROWS: usize = 264;
        assert_eq!(ds.nrows(), EXPECTED_GDP_NROWS);
        assert_eq!(ds.field::<gdp::CountryName>().len(), EXPECTED_GDP_NROWS);
    }
}
