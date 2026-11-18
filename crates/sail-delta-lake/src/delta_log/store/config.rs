// SPDX-License-Identifier: Apache-2.0
// SPDX-FileAttributionText: License: https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/LICENSE.txt
// SPDX-FileComment: Credit: https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/crates/core/src/logstore/config.rs
// SPDX-FileCopyrightText: Copyright (2020) QP Hou and a number of other contributors.
// SPDX-FileCopyrightText: Portions Copyright (2025) LakeSail, Inc.
// SPDX-FileContributor: Modified by LakeSail, Inc. in 2025

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
