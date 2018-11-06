use std::marker::PhantomData;

// use typenum::{Unsigned, U0, B1};

use cons::*;
// use data_types::{DTypeList, GetDType};


#[derive(Debug, Clone)]
pub struct Field<Ident, FIdx, DType> {
    _ident: PhantomData<Ident>,
    _fidx: PhantomData<FIdx>,
    _dtype: PhantomData<DType>,
}

pub trait FieldTypes {
    type Ident;
    type FIdx;
    type DType;
}
impl<Ident, FIdx, DType> FieldTypes for Field<Ident, FIdx, DType> {
    type Ident = Ident;
    type FIdx = FIdx;
    type DType = DType;
}

#[derive(Debug, Clone)]
pub struct FieldPayload<Field, Payload> {
    _field: PhantomData<Field>,
    pub payload: Payload
}
impl<Field, Payload> From<Payload> for FieldPayload<Field, Payload>
{
    fn from(payload: Payload) -> FieldPayload<Field, Payload> {
        FieldPayload {
            _field: PhantomData,
            payload
        }
    }
}

#[derive(Debug, Clone)]
pub struct FieldMarker<Field, Marker> {
    _field: PhantomData<Field>,
    _marker: PhantomData<Marker>
}

pub type FieldCons<Ident, FIdx, DType, Tail> = Cons<Field<Ident, FIdx, DType>, Tail>;
pub type FieldPayloadCons<Field, Payload, Tail> = Cons<FieldPayload<Field, Payload>, Tail>;

impl<Ident, FIdx, DType, Tail> FieldTypes for FieldCons<Ident, FIdx, DType, Tail>
{
    type Ident = Ident;
    type FIdx = FIdx;
    type DType = DType;
}
impl FieldTypes for Nil
{
    type Ident = ();
    type FIdx = FNil;
    type DType = ();
}

pub trait AssocField {
    type Field;
}
impl<Ident, FIdx, DType, Tail> AssocField for FieldCons<Ident, FIdx, DType, Tail> {
    type Field = Field<Ident, FIdx, DType>;
}
impl<Field, Payload, Tail> AssocField for FieldPayloadCons<Field, Payload, Tail> {
    type Field = Field;
}

pub trait AssocFieldCons {
    type Fields;
}
impl AssocFieldCons for Nil {
    type Fields = Nil;
}
impl<Ident, FIdx, DType, Tail> AssocFieldCons
    for FieldCons<Ident, FIdx, DType, Tail>
{
    type Fields = Self;
}
impl<Ident, FIdx, DType, Payload, Tail> AssocFieldCons
    for FieldPayloadCons<Field<Ident, FIdx, DType>, Payload, Tail>
    where Tail: AssocFieldCons,
{
    type Fields = FieldCons<Ident, FIdx, DType, Tail::Fields>;
}


// pub trait FieldIndex {
//     type FIdx;

//     fn index() -> usize where Self::FIdx: Unsigned { Self::FIdx::to_usize() }
// }
// impl FieldIndex for Nil {
//     type FIdx = U0;
// }
// impl<Ident, FIdx, DType, Tail> FieldIndex
//     for FieldCons<Ident, FIdx, DType, Tail>
// {
//     type FIdx = FIdx;
// }

#[derive(Debug, Clone)]
pub enum FieldDesignator<FIdx> {
    Expr(String),
    Idx(PhantomData<FIdx>),
}

pub type SpecCons<Field, SrcFIdx, Tail> = FieldPayloadCons<Field, FieldDesignator<SrcFIdx>, Tail>;

// pub type SpecCons<Name, FIdx, SrcFIdx, DType, Tail>
//     = Cons<SrcFieldSpec<Name, FIdx, SrcFIdx, DType>, Tail>;

// #[derive(Debug, Clone)]
// pub struct SrcFieldSpec<Name, FIdx, SrcFIdx, DType> {
//     _name: PhantomData<Name>,
//     _fidx: PhantomData<FIdx>,
//     pub src_name: FieldDesignator<SrcFIdx>,
//     _dtype: PhantomData<DType>,
// }
// impl<Name, FIdx, SrcFIdx, DType> SrcFieldSpec<Name, FIdx, SrcFIdx, DType> {
//     pub fn new(src_name: FieldDesignator<SrcFIdx>) -> SrcFieldSpec<Name, FIdx, SrcFIdx, DType> {
//         SrcFieldSpec {
//             _name: PhantomData,
//             _fidx: PhantomData,
//             src_name,
//             _dtype: PhantomData,
//         }
//     }
// }

impl<Field, SrcFIdx, Tail> SpecCons<Field, SrcFIdx, Tail> {
    pub fn new(src_name: FieldDesignator<SrcFIdx>, tail: Tail) -> SpecCons<Field, SrcFIdx, Tail>
    {
        SpecCons {
            head: FieldPayload {
                _field: PhantomData,
                payload: src_name,
            },
            tail
        }
    }
}
// impl<Name, FIdx, SrcFIdx, DType, Tail> SpecCons<Name, FIdx, SrcFIdx, DType, Tail> {
//     pub fn new(src_name: FieldDesignator<SrcFIdx>, tail: Tail)
//         -> SpecCons<Name, FIdx, SrcFIdx, DType, Tail>
//     {
//         SpecCons {
//             head: src_name,
//             tail
//         }
//     }
// }

// pub enum SrcFieldDesignator {
//     Name(String),
//     Index(usize),
// }
// pub struct FieldSpec<DTypes: DTypeList> {
//     pub src_name: SrcFieldDesignator,
//     pub dtype: DTypes::DType
// }

// pub trait AssocFields {
//     type Fields;
// }
// impl AssocFields for Nil {
//     type Fields = Nil;
// }
// impl<Name, FIdx, DType, Payload, Tail> AssocFields
//     for FieldPayloadCons<Field<Name, FIdx, DType>, Payload, Tail>
//     where Tail: AssocFields,
// {
//     type Fields = FieldCons<Name, FIdx, DType, Tail::Fields>;
// }

// pub trait FieldSpecs<DTypes: DTypeList> {
//     fn field_specs(&self) -> Vec<FieldSpec<DTypes>>;
// }
// impl<DTypes, Field, SrcFIdx, Tail> FieldSpecs<DTypes>
//     for SpecCons<Field, SrcFIdx, Tail>
//     where Self: ExtendFieldSpecs<DTypes>,
//           Tail: ExtendFieldSpecs<DTypes>,
//           DTypes: DTypeList,
// {
//     // type Fields = FieldCons<Name, FIdx, DType, Tail::Fields>;
//     fn field_specs(&self) -> Vec<FieldSpec<DTypes>> {
//         self.extend_field_specs(vec![])
//     }
// }

// pub trait ExtendFieldSpecs<DTypes: DTypeList>: AssocFields {
//     fn extend_field_specs(&self, specs: Vec<FieldSpec<DTypes>>) -> Vec<FieldSpec<DTypes>>;
// }
// impl<DTypes: DTypeList> ExtendFieldSpecs<DTypes> for Nil {
//     fn extend_field_specs(&self, specs: Vec<FieldSpec<DTypes>>) -> Vec<FieldSpec<DTypes>> {
//         specs
//     }
// }
// impl<DTypes, Ident, FIdx, SrcFIdx, DType, Tail> ExtendFieldSpecs<DTypes>
//     for SpecCons<Field<Ident, FIdx, DType>, SrcFIdx, Tail>
//     where FIdx: Unsigned,
//           SrcFIdx: Unsigned,
//           Tail: ExtendFieldSpecs<DTypes>,
//           DTypes: DTypeList,
//           DType: GetDType<DTypes>,
// {
//     fn extend_field_specs(&self, mut specs: Vec<FieldSpec<DTypes>>) -> Vec<FieldSpec<DTypes>> {
//         let src_field_name =  match self.head.payload {
//             FieldDesignator::Expr(ref s) => SrcFieldDesignator::Name(s.clone()),
//             FieldDesignator::Idx(_) => SrcFieldDesignator::Index(FIdx::to_usize()),
//         };
//         specs.push(FieldSpec {
//             src_name: src_field_name,
//             dtype: <DType as GetDType<DTypes>>::DTYPE,
//         });
//         self.tail.extend_field_specs(specs)
//     }
// }


#[derive(Debug, Clone)]
pub struct FNil;
#[derive(Debug, Clone)]
pub struct F0;
#[derive(Debug, Clone)]
pub struct Next<FIdx> {
    _marker: PhantomData<FIdx>,
}

macro_rules! impl_field_markers {
    // endpoint
    ($name1:ident) => {};
    // recursion unrolling
    ($name1:ident $name2:ident $name3:ident $name4:ident $name5:ident $name6:ident
        $name7:ident $name8:ident $($rest:ident)*) =>
    {
        /// Field $name8
        pub type $name8 = Next<$name7>;
        /// Field $name7
        pub type $name7 = Next<$name6>;
        /// Field $name6
        pub type $name6 = Next<$name5>;
        /// Field $name5
        pub type $name5 = Next<$name4>;
        /// Field $name4
        pub type $name4 = Next<$name3>;
        /// Field $name3
        pub type $name3 = Next<$name2>;
        /// Field $name2
        pub type $name2 = Next<$name1>;
        impl_field_markers![$name8 $($rest)*];
    };
    // main technique: define $name2 in terms of $name1 and recurse, dropping $name1 from the list
    ($name1:ident $name2:ident $($rest:ident)*) => {
        /// Field $name2
        pub type $name2 = Next<$name1>;
        impl_field_markers![$name2 $($rest)*];
    };
}
impl_field_markers![
      F0   F1   F2   F3   F4   F5   F6   F7   F8   F9
     F10  F11  F12  F13  F14  F15  F16  F17  F18  F19
     F20  F21  F22  F23  F24  F25  F26  F27  F28  F29
     F30  F31  F32  F33  F34  F35  F36  F37  F38  F39
     F40  F41  F42  F43  F44  F45  F46  F47  F48  F49
     F50  F51  F52  F53  F54  F55  F56  F57  F58  F59
     F60  F61  F62  F63  F64  F65  F66  F67  F68  F69
     F70  F71  F72  F73  F74  F75  F76  F77  F78  F79
     F80  F81  F82  F83  F84  F85  F86  F87  F88  F89
     F90  F91  F92  F93  F94  F95  F96  F97  F98  F99
    F100 F101 F102 F103 F104 F105 F106 F107 F108 F109
    F110 F111 F112 F113 F114 F115 F116 F117 F118 F119
    F120 F121 F122 F123 F124 F125 F126 F127 F128 F129
    F130 F131 F132 F133 F134 F135 F136 F137 F138 F139
    F140 F141 F142 F143 F144 F145 F146 F147 F148 F149
    F150 F151 F152 F153 F154 F155 F156 F157 F158 F159
    F160 F161 F162 F163 F164 F165 F166 F167 F168 F169
    F170 F171 F172 F173 F174 F175 F176 F177 F178 F179
    F180 F181 F182 F183 F184 F185 F186 F187 F188 F189
    F190 F191 F192 F193 F194 F195 F196 F197 F198 F199
];
pub trait Position {
    const POS: usize;
    fn pos(self) -> usize where Self: Sized { Self::POS }
}
impl Position for FNil {
    const POS: usize = 0usize;
}
impl Position for F0 {
    const POS: usize = 0usize;
}
impl<PrevFIdx> Position for Next<PrevFIdx> where PrevFIdx: Position {
    const POS: usize = PrevFIdx::POS + 1;
}


#[macro_export]
macro_rules! spec {
    // general end point
    (@step ) => {{
        $crate::cons::Nil
    }};

    // end points without trailing comma
    (@step $field_ident:ident($field_name:expr): $field_ty:ty) => {{
        use std::marker::PhantomData;
        use $crate::fieldlist::{FieldDesignator, SpecCons, Field, Next, FNil};
        SpecCons::<
            Field<
                $field_ident,
                Next<spec![@compute_fidx $($rest)*]>,
                $field_ty
            >,
            FNil, // placeholder source index value
            _
        >::new(
            FieldDesignator::Expr($field_name.to_string()),
            spec![@step ]
        )
    }};
    (@step $field_ident:ident[$src_field_idx:ident]: $field_ty:ty) => {{
        use std::marker::PhantomData;
        use $crate::fieldlist::{FieldDesignator, SpecCons, Field, Next};
        SpecCons::<
            Field<
                $field_ident,
                Next<spec![@compute_fidx $($result)*]>,
                $field_ty,
            >,
            $src_field_idx,
            _
        >::new(
            FieldDesignator::Idx(PhantomData::<$src_field_idx>),
            spec![@step ]
        )
    }};

    // entry point / main recursion loop
    (@step $field_ident:ident($field_name:expr): $field_ty:ty, $($rest:tt)*) => {{
        use $crate::fieldlist::{FieldDesignator, SpecCons, Field, Next, FNil};
        SpecCons::<
            Field<
                $field_ident,
                Next<spec![@compute_fidx $($rest)*]>,
                $field_ty
            >,
            FNil, // placeholder source index value
            _
        >::new(
            FieldDesignator::Expr($field_name.to_string()),
            spec![@step $($rest)*]
        )
    }};
    (@step $field_ident:ident[$src_field_idx:ident]: $field_ty:ty, $($rest:tt)*) => {{
        use $crate::fieldlist::{FieldDesignator, SpecCons, Field, Next};
        use std::marker::PhantomData;
        SpecCons::<
            Field<
                $field_ident,
                Next<spec![@compute_fidx $($rest)*]>,
                $field_ty
            >,
            $src_field_idx,
            _
        >::new(
            FieldDesignator::Idx(PhantomData::<$src_field_idx>),
            spec![@step $($rest)*]
        )
    }};
    (@start $($body:tt)*) => {{
        spec![@step $($body)*]
    }};

    (@compute_fidx ) => {
        $crate::fieldlist::FNil
    };
    (@compute_fidx $field_ident:ident($field_name:expr): $field_ty:ty) => {
        $crate::fieldlist::Next<spec![@compute_fidx]>
    };
    (@compute_fidx $field_ident:ident[$field_name:ident]: $field_ty:ty) => {
        $crate::fieldlist::Next<spec![@compute_fidx]>
    };
    (@compute_fidx $field_ident:ident($field_name:expr): $field_ty:ty, $($rest:tt)*) => {
        $crate::fieldlist::Next<spec![@compute_fidx $($rest)*]>
    };
    (@compute_fidx $field_ident:ident[$field_name:ident]: $field_ty:ty, $($rest:tt)*) => {
        $crate::fieldlist::Next<spec![@compute_fidx $($rest)*]>
    };

    (@decl_fields ) => {};
    (@decl_fields $field_ident:ident($field_name:expr): $field_ty:ty) => {
        #[derive(Debug)]
        struct $field_ident;
        // impl $crate::fieldlist::Position for $field_ident {
        //     type Pos = ::typenum::Add1<$pos>;
        // }
        spec![@decl_fields];
    };
    (@decl_fields $field_ident:ident[$field_name:ident]: $field_ty:ty) => {
        #[derive(Debug)]
        struct $field_ident;
        // impl $crate::fieldlist::Position for $field_ident {
        //     type Pos = ::typenum::Add1<$pos>;
        // }
        spec![@decl_fields];
    };
    (@decl_fields $field_ident:ident($field_name:expr): $field_ty:ty, $($rest:tt)*) => {
        #[derive(Debug)]
        struct $field_ident;
        // impl $crate::fieldlist::Position for $field_ident {
        //     type Pos = ::typenum::Add1<$pos>;
        // }
        spec![@decl_fields $($rest)*];
    };
    (@decl_fields $field_ident:ident[$field_name:ident]: $field_ty:ty, $($rest:tt)*) => {
        #[derive(Debug)]
        struct $field_ident;
        // impl $crate::fieldlist::Position for $field_ident {
        //     type Pos = ::typenum::Add1<$pos>;
        // }
        spec![@decl_fields $($rest)*];
    };
    // (@decl_fields $($body:tt)*) => {
    //     spec![@decl_fields $($body)*];
    // };

    (let $spec:ident = { $($body:tt)* };) => {
        spec![@decl_fields $($body)*];
        let $spec = spec![@start $($body)*];
    };

}

pub trait ISelector<FIdx> {
    type Field;
    type DType;

    // fn select(&self) -> &Self::Output;
}
impl ISelector<F0> for Nil {
    type Field = ();
    type DType = Nil;
    // fn select(&self) -> &Self {
    //     self
    // }
}

impl<Field, TargetIdx, DType, Tail> ISelector<TargetIdx>
    for FieldCons<Field, Next<TargetIdx>, DType, Tail>
    where Tail: ISelector<TargetIdx>,
{
    type Field = <Tail as ISelector<TargetIdx>>::Field;
    type DType = <Tail as ISelector<TargetIdx>>::DType;
}
impl<Field, TargetIdx, DType, Tail> ISelector<TargetIdx>
    for FieldCons<Field, TargetIdx, DType, Tail>
{
    type Field = Field;
    type DType = DType;
}

pub trait FSelector<Ident, FIdx> {
    type DType;

    type Output;
    fn select(&self) -> &Self::Output;
}
impl FSelector<(), F0> for Nil {
    type DType = Nil;

    type Output = Self;
    fn select(&self) -> &Self { self }
}
impl<TargetIdent, NonTargetIdent, TargetIdx, DType, Tail> FSelector<TargetIdent, TargetIdx>
    for FieldCons<NonTargetIdent, Next<TargetIdx>, DType, Tail>
    where Tail: FSelector<TargetIdent, TargetIdx>
{
    type DType = <Tail as FSelector<TargetIdent, TargetIdx>>::DType;

    type Output = Tail::Output;
    fn select(&self) -> &Tail::Output {
        self.tail.select()
    }
}
impl<TargetIdent, TargetIdx, DType, Tail> FSelector<TargetIdent, TargetIdx>
    for FieldCons<TargetIdent, TargetIdx, DType, Tail>
{
    type DType = DType;
    type Output = Self;

    fn select(&self) -> &Self { self }
}

impl<TargetIdent, NonTargetIdent, TargetIdx, DType, Payload, Tail>
    FSelector<TargetIdent, TargetIdx>
    for FieldPayloadCons<Field<NonTargetIdent, Next<TargetIdx>, DType>, Payload, Tail>
    where Tail: FSelector<TargetIdent, TargetIdx>
{
    type DType = <Tail as FSelector<TargetIdent, TargetIdx>>::DType;
    type Output = Tail::Output;

    fn select(&self) -> &Tail::Output {
        self.tail.select()
    }
}
impl<TargetIdent, TargetIdx, DType, Payload, Tail> FSelector<TargetIdent, TargetIdx>
    for FieldPayloadCons<Field<TargetIdent, TargetIdx, DType>, Payload, Tail>
{
    type DType = DType;
    type Output = Payload;

    fn select(&self) -> &Payload {
        &self.head.payload
    }
}

impl<Ident, FIdx, DType, Payload, Tail> FieldPayloadCons<Field<Ident, FIdx, DType>, Payload, Tail>
{
    pub fn select<TargetIdent, TargetIdx>(&self)
        -> &<Self as FSelector<TargetIdent, TargetIdx>>::Output
        where Self: FSelector<TargetIdent, TargetIdx>
    {
        FSelector::<TargetIdent, TargetIdx>::select(self)
    }
}

pub trait AttachPayload<Gen, DType>
{
    // type Field: FieldTypes<DType=DType>;
    type Output;

    fn attach_payload() -> Self::Output;
}
impl<Gen, DType> AttachPayload<Gen, DType> for Nil
{
    // type Field = Nil;
    type Output = Nil;

    fn attach_payload() -> Nil { Nil }
}
impl<Ident, FIdx, DType, Tail, Gen> AttachPayload<Gen, DType>
    for FieldCons<Ident, FIdx, DType, Tail>
    where Tail: AttachPayload<Gen, DType>,
          Gen: PayloadGenerator<DType>
{
    type Output = FieldPayloadCons<Field<Ident, FIdx, DType>, Gen::Payload, Tail::Output>;

    fn attach_payload() -> Self::Output
    {
        FieldPayloadCons {
            head: FieldPayload {
                _field: PhantomData,
                payload: Gen::generate(),
            },
            tail: Tail::attach_payload()
        }
    }
}

pub trait PayloadGenerator<DType> {
    type Payload;

    fn generate() -> Self::Payload;
}

// pub trait Position {
//     type Pos: Unsigned;

//     fn pos(self) -> usize where Self: Sized { Self::Pos::to_usize() }
// }

// pub trait Attachment<Fields>: fmt::Debug {}

// pub struct AttachmentCons<Fields: FieldList, P> {
//     payload: Option<Box<dyn PayloadKind>>,
//     fields: PhantomData<Fields>,
//     tail: Option<Box<dyn Attachment<Fields::Tail>>>,
// }
// impl<Fields, P> fmt::Debug for Attachment<Fields, P>
//     where Fields: FieldList,
//           P: fmt::Debug
// {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         write!(f, "Attachment {{ payload: {:?}, tail: {} }}",
//             self.payload,
//             match self.tail {
//                 Some(ref tail) => format!("{:?}", tail),
//                 None => "None".to_string()
//             }
//         )
//     }
// }

// impl AttachmentCons<EmptyList> {
//     pub fn empty() -> AttachmentCons<EmptyList> {
//         AttachmentCons {
//             payload: None,
//             fields: PhantomData,
//             tail: None
//         }
//     }
// }

// impl<'a, Fields, P> IntoIterator for &'a Attachment<Fields, P>
//     where Fields: FieldList
// {
//     type Item = &'a P;
//     type IntoIter = AttachmentIter<'a, P>;

//     fn into_iter(self) -> AttachmentIter<'a, P> {
//         AttachmentIter {
//             attachment: self
//         }
//     }
// }

// pub struct AttachmentIter<'a, P> {
//     attachment: &'a dyn Payload;
// }
// impl Iterator for AttachmentIter<'a, P> {
//     type Item = &'a P;

//     fn next(&mut self) -> Option<&'a P> {

//     }
// }

// pub trait FieldList {
//     type Tail: FieldList;
//     type DType: fmt::Debug;
// }
// impl FieldList for () {
//     type Tail = ();
//     type DType = ();
// }
// impl FieldList for EmptyList {
//     type Tail = ();
//     type DType = ();
// }
// impl<Field, DType, Tail> FieldList for FieldCons<Field, DType, Tail>
//     where Tail: FieldList, DType: fmt::Debug
// {
//     type Tail = Tail;
//     type DType = DType;
// }

// pub trait Payload<Fields: FieldList> {
//     type P;
//     fn as_ref(&self) -> &Self::P;
//     fn as_mut(&mut self) -> &mut Self::P;
// }
// impl<Fields, P> Payload<P> for Attachment<Fields, P> {
//     fn as_ref(&self) -> &P {
//         &self.payload
//     }
//     fn as_mut(&mut self) -> &mut P {
//         &mut self.payload
//     }
// }

