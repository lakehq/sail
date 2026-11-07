//! `array_sort` and `sort_array`.
//!
//! [`ArraySort`] sorts by the natural order of the elements, for `array_sort(array)`
//! and `sort_array(array[, ascendingOrder])`. [`ArraySortLambda`] sorts with a
//! comparator lambda, for `array_sort(array, (left, right) -> int)`.

mod comparator;
mod sort;

pub use comparator::ArraySortLambda;
pub use sort::ArraySort;
