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

use std::sync::Arc;

use object_store::path::Path;
use object_store::prefix::PrefixStore;
use object_store::ObjectStore;
use url::Url;

use crate::kernel::DeltaResult;

// [Credit]: <https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/crates/core/src/logstore/config.rs>
/// Minimal storage configuration used to decorate object stores with table prefixes.
#[derive(Debug, Clone, Default)]
pub struct StorageConfig;

impl StorageConfig {
    /// Apply the table-specific prefix to the provided object store.
    pub fn decorate_store(
        &self,
        store: Arc<dyn ObjectStore>,
        table_root: &Url,
    ) -> DeltaResult<Arc<dyn ObjectStore>> {
        let prefix = Path::parse(table_root.path())?;
        if prefix.as_ref() == "/" || prefix.as_ref().is_empty() {
            return Ok(store);
        }

        Ok(Arc::new(PrefixStore::new(store, prefix)) as Arc<dyn ObjectStore>)
    }
}
