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

pub trait LocationGenerator {
    fn next_data_path(&self) -> (String, ObjectPath);
    fn with_partition_dir(&self, partition_dir: Option<&str>) -> (String, ObjectPath);
}

pub struct DefaultLocationGenerator {
    base: ObjectPath,
    data_dir: String,
    counter: AtomicU64,
}

impl DefaultLocationGenerator {
    pub fn new(base: ObjectPath) -> Self {
        Self {
            base,
            data_dir: "data".to_string(),
            counter: AtomicU64::new(0),
        }
    }

    pub fn new_with_data_dir(base: ObjectPath, data_dir: String) -> Self {
        Self {
            base,
            data_dir: data_dir.trim_matches('/').to_string(),
            counter: AtomicU64::new(0),
        }
    }
}

impl LocationGenerator for DefaultLocationGenerator {
    fn next_data_path(&self) -> (String, ObjectPath) {
        self.with_partition_dir(None)
    }

    fn with_partition_dir(&self, partition_dir: Option<&str>) -> (String, ObjectPath) {
        let id = self.counter.fetch_add(1, Ordering::Relaxed);
        let file = format!("part-{}-{:020}.parquet", Uuid::new_v4(), id);
        let rel = match partition_dir {
            Some(dir) if !dir.is_empty() => {
                format!("{}/{}/{}", self.data_dir, dir.trim_matches('/'), file)
            }
            _ => format!("{}/{}", self.data_dir, file),
        };
        // Join each component to avoid encoding '/' into '%2F'
        let mut full = self.base.clone();
        for comp in rel.split('/').filter(|s| !s.is_empty()) {
            full = full.child(comp);
        }
        (rel, full)
    }
}
