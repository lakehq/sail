// SPDX-License-Identifier: Apache-2.0

#[expect(
    clippy::module_inception,
    reason = "The array function shares its name with the function group."
)]
pub mod array;
pub mod array_aggregate;
pub mod array_compact;
pub mod array_exists;
pub mod array_filter;
pub mod array_forall;
pub mod array_intersect;
pub mod array_item_with_position;
pub mod array_min_max;
pub mod array_position;
pub mod array_sort;
pub mod array_transform;
pub mod array_zip_with;
pub mod arrays_zip;
pub mod sequence;
pub mod shuffle;
mod utils;
