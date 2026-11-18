// SPDX-License-Identifier: Apache-2.0
// SPDX-FileComment: Credit: https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/LICENSE.txt
// SPDX-FileCopyrightText: (2020) QP Hou and a number of other contributors.
// SPDX-FileCopyrightText: (2025) LakeSail, Inc.
// SPDX-FileContributor: LakeSail, Inc.
// SPDX-FileComment: Modified in 2025 by LakeSail, Inc.
// SPDX-FileComment: Credit: https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/crates/core/src/logstore/config.rs

use std::sync::Arc;

use object_store::ObjectStore;
use object_store::path::Path;
use object_store::prefix::PrefixStore;
use url::Url;

use crate::spec::DeltaResult;

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
        let prefix = Path::from_url_path(table_root.path())?;
        if prefix.as_ref() == "/" || prefix.as_ref().is_empty() {
            return Ok(store);
        }

        Ok(Arc::new(PrefixStore::new(store, prefix)) as Arc<dyn ObjectStore>)
    }
}
