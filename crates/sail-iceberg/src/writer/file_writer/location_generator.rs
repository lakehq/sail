use std::sync::atomic::{AtomicU64, Ordering};

pub trait LocationGenerator {
    fn next_data_path(&self) -> String;
}

pub struct DefaultLocationGenerator {
    base: String,
    counter: AtomicU64,
}

impl DefaultLocationGenerator {
    pub fn new(base: String) -> Self {
        Self {
            base,
            counter: AtomicU64::new(0),
        }
    }
}

impl LocationGenerator for DefaultLocationGenerator {
    fn next_data_path(&self) -> String {
        let id = self.counter.fetch_add(1, Ordering::Relaxed);
        format!(
            "{}/data/part-{:020}.parquet",
            self.base.trim_end_matches('/'),
            id
        )
    }
}
