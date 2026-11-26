// SPDX-License-Identifier: Apache-2.0

/// Spark normalizes NaN and -0.0 in grouping keys before removing duplicates.
pub(in crate::aggregate) fn normalize(value: f32) -> f32 {
    if value.is_nan() {
        f32::NAN
    } else if value == 0.0 {
        0.0
    } else {
        value
    }
}
