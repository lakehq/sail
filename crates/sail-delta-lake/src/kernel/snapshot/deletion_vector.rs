// https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/LICENSE.txt
//
// Copyright (2020) QP Hou and a number of other contributors.
// Portions Copyright (2025) LakeSail, Inc.
// Modified in 2025 by LakeSail, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::str::FromStr;

use datafusion::arrow::array::cast::AsArray;
use datafusion::arrow::array::types::Int64Type;
use datafusion::arrow::array::{Array, RecordBatch, StructArray};
use datafusion::arrow::datatypes::Int32Type;

use super::iterators::get_string_value;
use crate::spec::fields::{
    DV_FIELD_CARDINALITY, DV_FIELD_OFFSET, DV_FIELD_PATH_OR_INLINE_DV, DV_FIELD_SIZE_IN_BYTES,
    DV_FIELD_STORAGE_TYPE, FIELD_NAME_DELETION_VECTOR,
};
use crate::spec::{DeletionVectorDescriptor, StorageType};

pub(super) fn deletion_vector_descriptor(
    files: &RecordBatch,
    index: usize,
) -> Option<DeletionVectorDescriptor> {
    let dv_col = files
        .column_by_name(FIELD_NAME_DELETION_VECTOR)
        .and_then(|col| col.as_struct_opt())?;
    if dv_col.null_count() == dv_col.len() {
        return None;
    }
    dv_col.is_valid(index).then_some(())?;
    dv_col
        .column_by_name(DV_FIELD_STORAGE_TYPE)
        .filter(|storage_col| storage_col.is_valid(index))?;
    Some(
        DeletionVectorView {
            data: dv_col,
            index,
        }
        .descriptor(),
    )
}

#[derive(Debug)]
struct DeletionVectorView<'a> {
    data: &'a StructArray,
    index: usize,
}

impl DeletionVectorView<'_> {
    fn descriptor(&self) -> DeletionVectorDescriptor {
        let storage_type =
            StorageType::from_str(self.storage_type()).unwrap_or(StorageType::UuidRelativePath);
        DeletionVectorDescriptor {
            storage_type,
            path_or_inline_dv: self.path_or_inline_dv().to_string(),
            size_in_bytes: self.size_in_bytes(),
            cardinality: self.cardinality(),
            offset: self.offset(),
        }
    }

    fn storage_type(&self) -> &str {
        self.data
            .column_by_name(DV_FIELD_STORAGE_TYPE)
            .and_then(|col| get_string_value(col.as_ref(), self.index))
            .unwrap_or("")
    }

    fn path_or_inline_dv(&self) -> &str {
        self.data
            .column_by_name(DV_FIELD_PATH_OR_INLINE_DV)
            .and_then(|col| get_string_value(col.as_ref(), self.index))
            .unwrap_or("")
    }

    fn size_in_bytes(&self) -> i32 {
        self.data
            .column_by_name(DV_FIELD_SIZE_IN_BYTES)
            .map(|column| column.as_primitive::<Int32Type>())
            .filter(|column| column.is_valid(self.index))
            .map(|column| column.value(self.index))
            .unwrap_or(0)
    }

    fn cardinality(&self) -> i64 {
        self.data
            .column_by_name(DV_FIELD_CARDINALITY)
            .map(|column| column.as_primitive::<Int64Type>())
            .filter(|column| column.is_valid(self.index))
            .map(|column| column.value(self.index))
            .unwrap_or(0)
    }

    fn offset(&self) -> Option<i32> {
        let column = self
            .data
            .column_by_name(DV_FIELD_OFFSET)
            .map(|value| value.as_primitive::<Int32Type>())?;
        column
            .is_valid(self.index)
            .then(|| column.value(self.index))
    }
}
