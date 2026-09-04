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
