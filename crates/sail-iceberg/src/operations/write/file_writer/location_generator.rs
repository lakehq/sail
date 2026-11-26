// SPDX-License-Identifier: Apache-2.0

use std::sync::atomic::{AtomicU64, Ordering};

use object_store::path::Path as ObjectPath;
use uuid::Uuid;

pub struct DefaultLocationGenerator {
    base: ObjectPath,
    counter: AtomicU64,
}

impl DefaultLocationGenerator {
    pub fn new(base: ObjectPath) -> Self {
        Self {
            base,
            counter: AtomicU64::new(0),
        }
    }

    pub fn next_data_path(
        &self,
        partition_dir: Option<&str>,
    ) -> Result<(String, ObjectPath), String> {
        let id = self.counter.fetch_add(1, Ordering::Relaxed);
        let file = format!("part-{}-{:020}.parquet", Uuid::new_v4(), id);
        let relative_path = match partition_dir {
            Some(dir) if !dir.is_empty() => format!("{}/{}", dir.trim_matches('/'), file),
            _ => file,
        };
        let full_path = if self.base.as_ref().is_empty() {
            relative_path.clone()
        } else {
            format!("{}/{relative_path}", self.base)
        };
        let full = ObjectPath::parse(full_path)
            .map_err(|error| format!("Invalid Iceberg data path: {error}"))?;
        Ok((relative_path, full))
    }
}
