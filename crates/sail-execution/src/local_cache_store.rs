use std::collections::HashMap;
use std::sync::Mutex;

use datafusion::arrow::record_batch::RecordBatch;

/// Worker-local store for cached RecordBatches that persist beyond job lifetime.
pub struct LocalCacheStore {
    data: Mutex<HashMap<(String, usize), Vec<RecordBatch>>>,
}

impl LocalCacheStore {
    /// Creates a new empty LocalCacheStore.
    pub fn new() -> Self {
        Self {
            data: Mutex::new(HashMap::new()),
        }
    }

    /// Stores RecordBatches for a given cache ID and partition.
    pub fn store(&self, cache_id: &str, partition: usize, batches: Vec<RecordBatch>) {
        let mut data = self.data.lock().unwrap_or_else(|e| e.into_inner());
        data.insert((cache_id.to_string(), partition), batches);
    }

    /// Retrieves cloned RecordBatches for a given cache ID and partition.
    pub fn get(&self, cache_id: &str, partition: usize) -> Option<Vec<RecordBatch>> {
        let data = self.data.lock().unwrap_or_else(|e| e.into_inner());
        data.get(&(cache_id.to_string(), partition)).cloned()
    }

    /// Removes all data for a given cache ID.
    pub fn remove(&self, cache_id: &str) {
        let mut data = self.data.lock().unwrap_or_else(|e| e.into_inner());
        data.retain(|(id, _), _| id != cache_id);
    }
}

impl Default for LocalCacheStore {
    fn default() -> Self {
        Self::new()
    }
}
