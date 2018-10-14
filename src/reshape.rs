use std::marker::PhantomData;

use apply::{FieldReduceFn, ApplyFieldReduce, Select, ReduceDataIndex, CompositeUnique};
use view::{DataView, IntoFieldList};
use field::FieldIdent;
use store::DataStore;
use error::*;

// struct AggregateFn<'a, F> where F: FieldMapFn {
//     target_ds: &'a mut DataStore,
//     func: F
// }
// impl<'a, 'b, F> FieldReduceFn<'a> for AggregateFn<'b, F> where F: FieldMapFn {
//     type Output = Result<DataView>;

//     fn reduce(&mut self, fields: Vec<ReduceDataIndex<'a>>) -> Result<DataView> {

//         // let mut store = DataSt
//     }
// }
pub trait Aggregate {
    fn aggregate<Ids: IntoFieldList, By: IntoFieldList, F, FOut>(&self, group_by: By,
        fields: Ids, func: F) -> Result<DataView>
        where F: Fn(&DataView, &FieldIdent) -> FOut;
}
impl Aggregate for DataView {
    fn aggregate<Ids: IntoFieldList, By: IntoFieldList, F, FOut>(&self, group_by: By,
        fields: Ids, func: F)-> Result<DataView>
        where F: Fn(&DataView, &FieldIdent) -> FOut
    {
        let group_by = group_by.into_field_list();
        let mut grouped_keys = self.composite_unique(group_by.clone())?;
        println!("{}", grouped_keys);
        struct AggregateFn<'a, FOut> {
            phantom: PhantomData<FOut>,
            source: &'a DataView,
        }
        impl<'a, 'b, FOut> FieldReduceFn<'a> for AggregateFn<'b, FOut> {
            type Output = FOut;
            fn reduce(&mut self, fields: Vec<ReduceDataIndex<'a>>) -> Self::Output {
                debug_assert!(fields.len() > 0);
                for i in 0..grouped_keys.len() {
                    for field in fields {
                        field.sasdjfiaoj();
                    }
                }
            }
        }
        let grouped_key_selections = group_by.iter().map(|ident| grouped_keys.select(ident))
            .collect::<Vec<_>>();
        grouped_key_selections
            .apply_field_reduce(&mut AggregateFn {
                phantom: PhantomData::<FOut>,
                source: self
            });
        // for ident in fields.into_field_list() {

            // ident.asdjfiaosdj();
        // }
        let mut store = DataStore::empty();
        Ok(store.into())
    }
}

struct MeltFn {

}
struct CastFn {

}

#[cfg(test)]
mod tests {
    use super::*;
    use test_utils::*;
    use apply::NumNa;

    #[test]
    fn aggregate() {
        // let orig_dv = sample_merged_emp_table();
        // println!("{}", orig_dv);
        // let aggregated = orig_dv.aggregate(["EmpId", "EmpName"], ["DeptId", "DidTraining"],
        //     NumNaFn {});

        let orig_dv: DataView = generate_sample_random_datastore(50, 0).into();
        println!("{}", orig_dv);
        let aggregated = orig_dv.aggregate(["col1", "col2"], ["col3", "col4"],
            |dv, ident| dv.num_na(ident));
    }
}
