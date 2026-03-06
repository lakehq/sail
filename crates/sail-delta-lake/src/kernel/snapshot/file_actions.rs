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

use std::collections::HashMap;

use chrono::Utc;
use datafusion::common::scalar::ScalarValue;

use super::deletion_vector::deletion_vector_descriptor;
use super::iterators::LogicalFileView;
use crate::conversion::ScalarExt;
use crate::spec::{Add, Remove};

impl LogicalFileView {
    fn partition_values_map(&self) -> HashMap<String, Option<String>> {
        let Some(pv) = self.partition_values() else {
            return HashMap::new();
        };
        let ScalarValue::Struct(struct_array) = &pv else {
            return HashMap::new();
        };
        let fields = struct_array.fields();
        fields
            .iter()
            .enumerate()
            .map(|(i, field)| {
                let col = struct_array.column(i);
                let sv = ScalarValue::try_from_array(col.as_ref(), 0).ok();
                let serialized = sv.as_ref().and_then(|value| {
                    if value.is_null() {
                        None
                    } else {
                        Some(value.serialize().into_owned())
                    }
                });
                (field.name().clone(), serialized)
            })
            .collect()
    }

    pub(crate) fn add_action(&self) -> Add {
        Add {
            path: self.path().to_string(),
            partition_values: self.partition_values_map(),
            size: self.size(),
            modification_time: self.modification_time(),
            data_change: true,
            stats: self.stats().map(|value| value.to_string()),
            tags: None,
            deletion_vector: deletion_vector_descriptor(self.record_batch(), self.row_index()),
            base_row_id: None,
            default_row_commit_version: None,
            clustering_provider: None,
            commit_version: None,
            commit_timestamp: None,
        }
    }

    pub fn remove_action(&self, data_change: bool) -> Remove {
        Remove {
            path: self.path().to_string(),
            data_change,
            deletion_timestamp: Some(Utc::now().timestamp_millis()),
            extended_file_metadata: Some(true),
            size: Some(self.size()),
            partition_values: Some(self.partition_values_map()),
            deletion_vector: deletion_vector_descriptor(self.record_batch(), self.row_index()),
            tags: None,
            base_row_id: None,
            default_row_commit_version: None,
        }
    }
}
