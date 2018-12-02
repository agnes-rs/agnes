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

pub trait UpdateFrameLabelMarker<FrameLabelOffset>
{
    type Output;
}
impl<FrameLabelOffset>
    UpdateFrameLabelMarker<FrameLabelOffset>
    for Nil
{
    type Output = Nil;
}
impl<RLabel, RFrameLabel, RTail, FrameLabelOffset>
    UpdateFrameLabelMarker<FrameLabelOffset>
    for FrameLookupCons<RLabel, RFrameLabel, RTail>
    where
        RFrameLabel: Offset<FrameLabelOffset>,
        RTail: UpdateFrameLabelMarker<FrameLabelOffset>
{
    type Output = FrameLookupCons<
        RLabel,
        <RFrameLabel as Offset<FrameLabelOffset>>::Output,
        <RTail as UpdateFrameLabelMarker<FrameLabelOffset>>::Output
    >;
}

pub trait UpdateFrameLabel<FrameLabelOffset>
{
    type Output;

    fn update_frame_label(self) -> Self::Output;
}
impl<FrameLabelOffset>
    UpdateFrameLabel<FrameLabelOffset>
    for Nil
{
    type Output = Nil;

    fn update_frame_label(self) -> Nil { Nil }
}

impl<RFrameLabel, RFrameFields, RTail, FrameLabelOffset>
    UpdateFrameLabel<FrameLabelOffset>
    for ViewFrameCons<RFrameLabel, RFrameFields, RTail>
    where
        RFrameLabel: Offset<FrameLabelOffset>,
        RFrameFields: AssocStorage,
        RTail: UpdateFrameLabel<FrameLabelOffset>
{
    type Output = ViewFrameCons<
        <RFrameLabel as Offset<FrameLabelOffset>>::Output,
        RFrameFields,
        <RTail as UpdateFrameLabel<FrameLabelOffset>>::Output
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
        RLabels: UpdateFrameLabelMarker<<LFrames as Len>::Len>,
        LLabels: Append<<RLabels as UpdateFrameLabelMarker<<LFrames as Len>::Len>>::Output>,
        RFrames: Clone + UpdateFrameLabel<<LFrames as Len>::Len>,
        LFrames: Append<<RFrames as UpdateFrameLabel<<LFrames as Len>::Len>>::Output>
            + Clone,
{
    type OutLabels = <LLabels as Append<
        <RLabels as UpdateFrameLabelMarker<<LFrames as Len>::Len>>::Output
    >>::Appended;
    type OutFrames = <LFrames as Append<
        <RFrames as UpdateFrameLabel<<LFrames as Len>::Len>>::Output
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
