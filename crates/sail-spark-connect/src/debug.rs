use std::fmt::{Debug, Formatter};

use sail_common::debug::DebugBinary;

use crate::spark::connect::execute_plan_response::ArrowBatch;
use crate::spark::connect::LocalRelation;

impl Debug for ArrowBatch {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let ArrowBatch { row_count, data } = self;
        f.debug_struct("ArrowBatch")
            .field("row_count", row_count)
            .field("data", &DebugBinary::from(data))
            .finish()
    }
}

impl Debug for LocalRelation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let LocalRelation { data, schema } = self;
        f.debug_struct("LocalRelation")
            .field(
                "data",
                &data.as_ref().map(|x| DebugBinary::from(x.as_slice())),
            )
            .field("schema", schema)
            .finish()
    }
}
