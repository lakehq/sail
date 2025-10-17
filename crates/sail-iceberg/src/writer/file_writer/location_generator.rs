use std::sync::atomic::{AtomicU64, Ordering};

use object_store::path::Path as ObjectPath;
use uuid::Uuid;

pub trait LocationGenerator {
    fn next_data_path(&self) -> (String, ObjectPath);
    fn with_partition_dir(&self, partition_dir: Option<&str>) -> (String, ObjectPath);
}

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
                format!("data/{}/{}", dir.trim_matches('/'), file)
            }
            _ => format!("data/{}", file),
        };
        // Join each component to avoid encoding '/' into '%2F'
        let mut full = self.base.clone();
        for comp in rel.split('/').filter(|s| !s.is_empty()) {
            full = full.child(comp);
        }
        (rel, full)
    }
}
