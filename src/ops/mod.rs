/*!
Mathematical operations for `DataView` objects.
*/

mod op_error;
pub use self::op_error::*;

#[macro_use] mod scalar_op;
pub use self::scalar_op::*;

#[macro_use] mod field_op;
pub use self::field_op::*;
