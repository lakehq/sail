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

use uuid::Uuid;

use crate::spec::TableMetadata;

/// Generate a unique snapshot ID from a UUID.
pub fn generate_snapshot_id() -> i64 {
    let (lhs, rhs) = Uuid::new_v4().as_u64_pair();
    let snapshot_id = (lhs ^ rhs) as i64;
    if snapshot_id < 0 {
        -snapshot_id
    } else {
        snapshot_id
    }
}

/// Generate a unique snapshot ID that doesn't conflict with existing snapshots.
pub fn generate_unique_snapshot_id(table_metadata: &TableMetadata) -> i64 {
    let mut snapshot_id = generate_snapshot_id();

    while table_metadata
        .snapshots
        .iter()
        .any(|s| s.snapshot_id() == snapshot_id)
    {
        snapshot_id = generate_snapshot_id();
    }

    snapshot_id
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn test_generate_snapshot_id_is_positive() {
        for _ in 0..1000 {
            let id = generate_snapshot_id();
            assert!(id > 0, "Snapshot ID should be positive: {}", id);
        }
    }

    #[test]
    fn test_generate_snapshot_id_uniqueness() {
        let mut ids = HashSet::new();
        let count = 10000;

        for _ in 0..count {
            let id = generate_snapshot_id();
            assert!(ids.insert(id), "Generated duplicate snapshot ID: {}", id);
        }

        assert_eq!(ids.len(), count, "Should generate {} unique IDs", count);
    }

    #[test]
    fn test_generate_snapshot_id_distribution() {
        let mut ids = Vec::new();
        for _ in 0..100 {
            ids.push(generate_snapshot_id());
        }

        for i in 1..ids.len() {
            assert_ne!(ids[i], ids[i - 1], "Consecutive IDs should be different");
        }
    }
}
