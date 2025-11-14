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

use std::sync::atomic::{AtomicI64, Ordering};

static LAST_TIMESTAMP_MS: AtomicI64 = AtomicI64::new(0);

/// Generate a monotonically increasing timestamp in milliseconds.
pub fn monotonic_timestamp_ms() -> i64 {
    loop {
        let now = chrono::Utc::now().timestamp_millis();
        let last = LAST_TIMESTAMP_MS.load(Ordering::Relaxed);

        let next = if now > last { now } else { last + 1 };

        match LAST_TIMESTAMP_MS.compare_exchange(last, next, Ordering::SeqCst, Ordering::Relaxed) {
            Ok(_) => return next,
            Err(_) => continue,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::thread;

    use super::*;

    #[test]
    fn test_monotonic_timestamp_single_thread() {
        let mut timestamps = Vec::new();
        for _ in 0..100 {
            timestamps.push(monotonic_timestamp_ms());
        }

        for i in 1..timestamps.len() {
            assert!(
                timestamps[i] > timestamps[i - 1],
                "Timestamp at index {} ({}) should be greater than previous ({})",
                i,
                timestamps[i],
                timestamps[i - 1]
            );
        }
    }

    #[test]
    fn test_monotonic_timestamp_multi_thread() {
        let num_threads = 10;
        let iterations_per_thread = 100;
        let timestamps = Arc::new(std::sync::Mutex::new(HashSet::new()));
        let mut handles = vec![];

        for _ in 0..num_threads {
            let timestamps_clone = Arc::clone(&timestamps);
            let handle = thread::spawn(move || {
                let mut local_timestamps = Vec::new();
                for _ in 0..iterations_per_thread {
                    local_timestamps.push(monotonic_timestamp_ms());
                }
                #[allow(clippy::unwrap_used)]
                let mut ts = timestamps_clone.lock().unwrap();
                for t in local_timestamps {
                    assert!(ts.insert(t), "Duplicate timestamp detected: {}", t);
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            #[allow(clippy::unwrap_used)]
            handle.join().unwrap();
        }

        #[allow(clippy::unwrap_used)]
        let ts = timestamps.lock().unwrap();
        assert_eq!(
            ts.len(),
            num_threads * iterations_per_thread,
            "All timestamps should be unique"
        );
    }

    #[test]
    fn test_monotonic_timestamp_rapid_succession() {
        let mut prev = monotonic_timestamp_ms();
        for _ in 0..1000 {
            let current = monotonic_timestamp_ms();
            assert!(
                current > prev,
                "Current timestamp {} should be greater than previous {}",
                current,
                prev
            );
            prev = current;
        }
    }
}
