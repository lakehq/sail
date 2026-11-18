// SPDX-License-Identifier: Apache-2.0
// SPDX-FileComment: License: https://github.com/delta-io/delta-kernel-rs/blob/f105333a003232d7284f1a8f06cca3b6d6b232a9/LICENSE
// SPDX-FileCopyrightText: Copyright 2023-2024 The Delta Kernel Rust Authors
// SPDX-FileCopyrightText: Portions Copyright 2025-2026 LakeSail, Inc.
// SPDX-FileContributor: Ported and modified by LakeSail, Inc. in 2026

// SPDX-SnippetBegin
// SPDX-License-Identifier: Apache-2.0
// SPDX-SnippetComment: Credit: https://github.com/delta-io/delta-kernel-rs/blob/f105333a003232d7284f1a8f06cca3b6d6b232a9/kernel/src/actions/mod.rs#L1156-L1205
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
// SPDX-SnippetEnd

// SPDX-SnippetBegin
// SPDX-License-Identifier: Apache-2.0
// SPDX-SnippetComment: Credit: https://github.com/delta-io/delta-kernel-rs/blob/f105333a003232d7284f1a8f06cca3b6d6b232a9/kernel/src/scan/data_skipping.rs#L119-L124
pub const STATS_FIELD_NUM_RECORDS: &str = "numRecords";
pub const STATS_FIELD_TIGHT_BOUNDS: &str = "tightBounds";
pub const STATS_FIELD_MIN_VALUES: &str = "minValues";
pub const STATS_FIELD_MAX_VALUES: &str = "maxValues";
pub const STATS_FIELD_NULL_COUNT: &str = "nullCount";
// SPDX-SnippetEnd

// SPDX-SnippetBegin
// SPDX-License-Identifier: Apache-2.0
// SPDX-SnippetComment: Credit: https://github.com/delta-io/delta-kernel-rs/blob/f105333a003232d7284f1a8f06cca3b6d6b232a9/kernel/src/actions/mod.rs#L1195-L1205
pub const DV_FIELD_STORAGE_TYPE: &str = "storageType";
pub const DV_FIELD_PATH_OR_INLINE_DV: &str = "pathOrInlineDv";
pub const DV_FIELD_SIZE_IN_BYTES: &str = "sizeInBytes";
pub const DV_FIELD_CARDINALITY: &str = "cardinality";
pub const DV_FIELD_OFFSET: &str = "offset";
// SPDX-SnippetEnd
