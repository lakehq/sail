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

// [Credit]: <https://github.com/delta-io/delta-rs/blob/1f0b4d0965a85400c1effc6e9b4c7ebbb6795978/crates/core/src/kernel/snapshot/log_data.rs>

use datafusion::arrow::record_batch::RecordBatch;

use crate::kernel::snapshot::DeltaSnapshot;

/// Provides semantic access to the lazily materialized file batch.
#[derive(Clone)]
pub struct LogDataHandler<'a> {
    data: &'a RecordBatch,
    snapshot: &'a DeltaSnapshot,
}

impl<'a> LogDataHandler<'a> {
    pub(crate) fn new(data: &'a RecordBatch, snapshot: &'a DeltaSnapshot) -> Self {
        Self { data, snapshot }
    }

    pub(super) fn data(&self) -> &RecordBatch {
        self.data
    }

    pub(crate) fn snapshot(&self) -> &DeltaSnapshot {
        self.snapshot
    }

    /// The number of files in the log data.
    pub fn num_files(&self) -> usize {
        self.data.num_rows()
    }
}
