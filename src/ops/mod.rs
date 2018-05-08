/*!
Mathematical operations for `DataView` objects.
*/

mod op_error;
pub use self::op_error::*;

mod infer;
pub(crate) use self::infer::*;

mod scalar_op;
pub(crate) use self::scalar_op::*;

mod field_op;
