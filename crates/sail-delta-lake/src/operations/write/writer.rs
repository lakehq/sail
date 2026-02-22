// Copyright (2025) LakeSail, Inc.
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

use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use object_store::path::Path;
use object_store::ObjectStore;

pub use super::config::{PartitioningMode, WriterConfig};
use super::orchestration::DeltaWriteOrchestrator;
use crate::kernel::models::Add;
use crate::kernel::DeltaTableError;

/// Backwards-compatible Delta writer API used by `DeltaWriterExec`.
pub struct DeltaWriter {
    orchestrator: DeltaWriteOrchestrator,
}

impl DeltaWriter {
    pub fn new(object_store: Arc<dyn ObjectStore>, table_path: Path, config: WriterConfig) -> Self {
        Self {
            orchestrator: DeltaWriteOrchestrator::new(object_store, table_path, config),
        }
    }

    pub async fn write(&mut self, batch: &RecordBatch) -> Result<(), DeltaTableError> {
        self.orchestrator.write(batch).await
    }

    pub async fn close(self) -> Result<Vec<Add>, DeltaTableError> {
        self.orchestrator.close().await
    }
}
