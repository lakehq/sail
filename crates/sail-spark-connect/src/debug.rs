use std::fmt::{Debug, Formatter};

use sail_common::debug::DebugBinary;

use crate::spark::connect::execute_plan_response::ArrowBatch;
use crate::spark::connect::LocalRelation;

impl Debug for ArrowBatch {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let ArrowBatch {
            row_count,
            data,
            start_offset,
            chunk_index,
            num_chunks_in_batch,
        } = self;
        f.debug_struct("ArrowBatch")
            .field("row_count", row_count)
            .field("data", &DebugBinary::from(data))
            .field("start_offset", start_offset)
            .field("chunk_index", chunk_index)
            .field("num_chunks_in_batch", num_chunks_in_batch)
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
