use std::ops::Add;
use std::marker::PhantomData;

use cons::*;
use label::{Labeled, LVCons};
use view::{DataView, FrameLookupCons, ViewFrameCons};
use store::AssocStorage;

pub trait Offset<O>
{
    type Output;
}
impl<O, U> Offset<O>
    for U
    where U: Add<O>
{
    type Output = <U as Add<O>>::Output;
}

pub trait UpdateFrameIndexMarker<FrameIndexOffset>
{
    type Output;
}
impl<FrameIndexOffset>
    UpdateFrameIndexMarker<FrameIndexOffset>
    for Nil
{
    type Output = Nil;
}
impl<RLabel, RFrameIndex, RFrameLabel, RTail, FrameIndexOffset>
    UpdateFrameIndexMarker<FrameIndexOffset>
    for FrameLookupCons<RLabel, RFrameIndex, RFrameLabel, RTail>
    where
        RFrameIndex: Offset<FrameIndexOffset>,
        RTail: UpdateFrameIndexMarker<FrameIndexOffset>
{
    type Output = FrameLookupCons<
        RLabel,
        <RFrameIndex as Offset<FrameIndexOffset>>::Output,
        RFrameLabel,
        <RTail as UpdateFrameIndexMarker<FrameIndexOffset>>::Output
    >;
}

pub trait UpdateFrameIndex<FrameIndexOffset>
{
    type Output;

    fn update_frame_label(self) -> Self::Output;
}
impl<FrameIndexOffset>
    UpdateFrameIndex<FrameIndexOffset>
    for Nil
{
    type Output = Nil;

    fn update_frame_label(self) -> Nil { Nil }
}

impl<RFrameIndex, RFrameFields, RTail, FrameIndexOffset>
    UpdateFrameIndex<FrameIndexOffset>
    for ViewFrameCons<RFrameIndex, RFrameFields, RTail>
    where
        RFrameIndex: Offset<FrameIndexOffset>,
        RFrameFields: AssocStorage,
        RTail: UpdateFrameIndex<FrameIndexOffset>
{
    type Output = ViewFrameCons<
        <RFrameIndex as Offset<FrameIndexOffset>>::Output,
        RFrameFields,
        <RTail as UpdateFrameIndex<FrameIndexOffset>>::Output
    >;

    fn update_frame_label(self) -> Self::Output
    {
        LVCons
        {
            head: Labeled::from(self.head.value),
            tail: self.tail.update_frame_label()
        }
    }
}


pub trait Merge<RLabels, RFrames>
{
    type OutLabels;
    type OutFrames;

    fn merge(&self, right: &DataView<RLabels, RFrames>)
        -> DataView<Self::OutLabels, Self::OutFrames>;
}
impl<LLabels, LFrames, RLabels, RFrames>
    Merge<RLabels, RFrames>
    for DataView<LLabels, LFrames>
    where
        LFrames: Len,
        RLabels: UpdateFrameIndexMarker<<LFrames as Len>::Len>,
        LLabels: Append<<RLabels as UpdateFrameIndexMarker<<LFrames as Len>::Len>>::Output>,
        RFrames: Clone + UpdateFrameIndex<<LFrames as Len>::Len>,
        LFrames: Append<<RFrames as UpdateFrameIndex<<LFrames as Len>::Len>>::Output>
            + Clone,
{
    type OutLabels = <LLabels as Append<
        <RLabels as UpdateFrameIndexMarker<<LFrames as Len>::Len>>::Output
    >>::Appended;
    type OutFrames = <LFrames as Append<
        <RFrames as UpdateFrameIndex<<LFrames as Len>::Len>>::Output
    >>::Appended;

    fn merge(&self, right: &DataView<RLabels, RFrames>)
        -> DataView<Self::OutLabels, Self::OutFrames>
    {
        let out_frames = self.frames.clone().append(right.frames.clone().update_frame_label());

        DataView
        {
            _labels: PhantomData,
            frames: out_frames
        }
    }
}
