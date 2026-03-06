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

use crate::kernel::snapshot::iterators::LogicalFileView;
use crate::kernel::snapshot::SnapshotTableConfiguration;

/// Provides semanitc access to the log data.
///
/// This is a helper struct that provides access to the log data in a more semantic way
/// to avid the necessiity of knowing the exact layout of the underlying log data.
#[derive(Clone)]
pub struct LogDataHandler<'a> {
    data: &'a RecordBatch,
    config: &'a SnapshotTableConfiguration,
}

impl<'a> LogDataHandler<'a> {
    pub(crate) fn new(data: &'a RecordBatch, config: &'a SnapshotTableConfiguration) -> Self {
        Self { data, config }
    }

    pub(super) fn data(&self) -> &RecordBatch {
        self.data
    }

    pub(crate) fn table_configuration(&self) -> &SnapshotTableConfiguration {
        self.config
    }

    /// The number of files in the log data.
    pub fn num_files(&self) -> usize {
        self.data.num_rows()
    }

    pub fn iter(&self) -> impl Iterator<Item = LogicalFileView> {
        let batch = self.data.clone();
        (0..batch.num_rows()).map(move |idx| LogicalFileView::new(batch.clone(), idx))
    }
}

impl IntoIterator for LogDataHandler<'_> {
    type Item = LogicalFileView;
    type IntoIter = Box<dyn Iterator<Item = Self::Item>>;

    fn into_iter(self) -> Self::IntoIter {
        let batch = self.data.clone();
        Box::new((0..self.data.num_rows()).map(move |idx| LogicalFileView::new(batch.clone(), idx)))
    }
}
