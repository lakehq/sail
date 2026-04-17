// https://github.com/delta-io/delta-kernel-rs/blob/f105333a003232d7284f1a8f06cca3b6d6b232a9/LICENSE
//
// Copyright 2023-2024 The Delta Kernel Rust Authors
// Portions Copyright 2025-2026 LakeSail, Inc.
// Ported and modified in 2026 by LakeSail, Inc.
//
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

// [Credit]: <https://github.com/delta-io/delta-kernel-rs/blob/f105333a003232d7284f1a8f06cca3b6d6b232a9/kernel/src/actions/mod.rs#L1156-L1205>
pub const FIELD_NAME_PATH: &str = "path";
pub const FIELD_NAME_SIZE: &str = "size";
pub const FIELD_NAME_MODIFICATION_TIME: &str = "modificationTime";
pub const FIELD_NAME_STATS: &str = "stats";
pub const FIELD_NAME_STATS_PARSED: &str = "stats_parsed";
#[expect(dead_code)]
const FIELD_NAME_FILE_CONSTANT_VALUES: &str = "fileConstantValues";
#[expect(dead_code)]
const FIELD_NAME_PARTITION_VALUES: &str = "partitionValues";
pub const FIELD_NAME_PARTITION_VALUES_PARSED: &str = "partitionValues_parsed";
pub const FIELD_NAME_DELETION_VECTOR: &str = "deletionVector";

// [Credit]: <https://github.com/delta-io/delta-kernel-rs/blob/f105333a003232d7284f1a8f06cca3b6d6b232a9/kernel/src/scan/data_skipping.rs#L119-L124>
pub const STATS_FIELD_NUM_RECORDS: &str = "numRecords";
pub const STATS_FIELD_TIGHT_BOUNDS: &str = "tightBounds";
pub const STATS_FIELD_MIN_VALUES: &str = "minValues";
pub const STATS_FIELD_MAX_VALUES: &str = "maxValues";
pub const STATS_FIELD_NULL_COUNT: &str = "nullCount";

// [Credit]: <https://github.com/delta-io/delta-kernel-rs/blob/f105333a003232d7284f1a8f06cca3b6d6b232a9/kernel/src/actions/mod.rs#L1195-L1205>
pub const DV_FIELD_STORAGE_TYPE: &str = "storageType";
pub const DV_FIELD_PATH_OR_INLINE_DV: &str = "pathOrInlineDv";
pub const DV_FIELD_SIZE_IN_BYTES: &str = "sizeInBytes";
pub const DV_FIELD_CARDINALITY: &str = "cardinality";
pub const DV_FIELD_OFFSET: &str = "offset";
