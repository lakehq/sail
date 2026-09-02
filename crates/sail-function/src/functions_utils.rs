/// [Credit]: <https://github.com/apache/datafusion/blob/e6e1eb229440591263c82bb2b913a4d5a16f9b70/datafusion/functions/src/utils.rs>
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use datafusion::arrow::array::ArrayRef;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::function::Hint;
use datafusion_expr::{ColumnarValue, ScalarFunctionImplementation};

/// Creates a scalar function implementation for the given function.
/// * `inner` - the function to be executed
/// * `hints` - hints to be used when expanding scalars to arrays
pub(super) fn make_scalar_function<F>(inner: F, hints: Vec<Hint>) -> ScalarFunctionImplementation
where
    F: Fn(&[ArrayRef]) -> Result<ArrayRef> + Sync + Send + 'static,
{
    Arc::new(move |args: &[ColumnarValue]| {
        // first, identify if any of the arguments is an Array. If yes, store its `len`,
        // as any scalar will need to be converted to an array of len `len`.
        let len = args
            .iter()
            .fold(Option::<usize>::None, |acc, arg| match arg {
                ColumnarValue::Scalar(_) => acc,
                ColumnarValue::Array(a) => Some(a.len()),
            });

        let is_scalar = len.is_none();

        let inferred_length = len.unwrap_or(1);
        let args = args
            .iter()
            .zip(hints.iter().chain(std::iter::repeat(&Hint::Pad)))
            .map(|(arg, hint)| {
                // Decide on the length to expand this scalar to depending
                // on the given hints.
                let expansion_len = match hint {
                    Hint::AcceptsSingular => 1,
                    Hint::Pad => inferred_length,
                };
                arg.to_array(expansion_len)
            })
            .collect::<Result<Vec<_>>>()?;

        let result = (inner)(&args);
        if is_scalar {
            // If all inputs are scalar, keeps output as scalar
            let result = result.and_then(|arr| ScalarValue::try_from_array(&arr, 0));
            result.map(ColumnarValue::Scalar)
        } else {
            result.map(ColumnarValue::Array)
        }
    })
}

/// Cap on memoized entries. A compiled `Regex` holds roughly 75 KiB once
/// used, so memoizing a whole batch of unique patterns would pin hundreds of
/// MiB until the batch ends; past the cap, values are computed per call. The
/// design target (a handful of distinct patterns per batch) stays far below
/// this.
const MAX_DISTINCT: usize = 128;

/// Per-batch memoization of an expensive `&str -> T` computation (a compiled
/// regex, a parsed interval). Keys borrow from the input, so a cache lives one
/// kernel invocation and holds at most [`MAX_DISTINCT`] entries. Only worth it
/// for low-cardinality inputs (patterns, formats); per-row-unique inputs just
/// pay overhead.
pub(crate) struct StrMemo<'a, T> {
    cache: HashMap<&'a str, T>,
    /// Value computed for a key seen once the cache is full, so
    /// `get_or_try_insert_ref` can still hand out a reference.
    overflow: Option<T>,
}

impl<'a, T> StrMemo<'a, T> {
    pub(crate) fn new() -> Self {
        Self {
            cache: HashMap::new(),
            overflow: None,
        }
    }

    /// Returns a reference to the memoized value for `key`, computing it on
    /// first sight. Errors are not cached and surface unchanged. Returning a
    /// reference matters for values whose clone is not free — cloning a
    /// `Regex` per row discards its internal lazy-DFA cache and forces the
    /// automaton to be rebuilt on every match.
    pub(crate) fn get_or_try_insert_ref(
        &mut self,
        key: &'a str,
        compute: impl FnOnce(&str) -> Result<T>,
    ) -> Result<&T> {
        if self.cache.len() >= MAX_DISTINCT && !self.cache.contains_key(key) {
            return Ok(self.overflow.insert(compute(key)?));
        }
        match self.cache.entry(key) {
            Entry::Occupied(entry) => Ok(entry.into_mut()),
            Entry::Vacant(entry) => Ok(entry.insert(compute(key)?)),
        }
    }

    /// Resolves the value for one row: a pre-compiled scalar if there is one,
    /// otherwise the per-batch memoized value for the row's key. `key` is
    /// evaluated only on the memoized path, so scalar inputs may be length-1
    /// arrays while the row index ranges over the whole batch. The value is
    /// returned by reference: cloning a `Regex` per row would discard its
    /// internal lazy-DFA cache and rebuild the automaton on every match.
    pub(crate) fn resolve<'m>(
        &'m mut self,
        scalar: Option<&'m T>,
        key: impl FnOnce() -> &'a str,
        compute: impl FnOnce(&str) -> Result<T>,
    ) -> Result<&'m T> {
        match scalar {
            Some(value) => Ok(value),
            None => self.get_or_try_insert_ref(key(), compute),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use datafusion_common::exec_err;

    use super::*;

    #[test]
    fn str_memo_computes_each_distinct_key_once_and_does_not_cache_errors() -> Result<()> {
        let calls = Cell::new(0);
        let mut memo: StrMemo<'_, usize> = StrMemo::new();
        let compute = |s: &str| {
            calls.set(calls.get() + 1);
            if s.is_empty() {
                return exec_err!("empty");
            }
            Ok(s.len())
        };
        assert_eq!(*memo.get_or_try_insert_ref("ab", compute)?, 2);
        assert_eq!(*memo.get_or_try_insert_ref("ab", compute)?, 2);
        assert_eq!(*memo.get_or_try_insert_ref("xyz", compute)?, 3);
        assert_eq!(calls.get(), 2);
        assert!(memo.get_or_try_insert_ref("", compute).is_err());
        assert!(memo.get_or_try_insert_ref("", compute).is_err());
        assert_eq!(calls.get(), 4);
        Ok(())
    }

    #[test]
    fn str_memo_caps_distinct_entries() -> Result<()> {
        let calls = Cell::new(0);
        let keys: Vec<String> = (0..MAX_DISTINCT).map(|i| i.to_string()).collect();
        let mut memo: StrMemo<'_, usize> = StrMemo::new();
        let compute = |s: &str| {
            calls.set(calls.get() + 1);
            Ok(s.len())
        };
        for key in &keys {
            memo.get_or_try_insert_ref(key, compute)?;
        }
        assert_eq!(calls.get(), MAX_DISTINCT);
        // A cached key is still served without recomputing.
        memo.get_or_try_insert_ref(&keys[0], compute)?;
        assert_eq!(calls.get(), MAX_DISTINCT);
        // Past the cap, values are computed per call and never cached.
        assert_eq!(*memo.get_or_try_insert_ref("overflow", compute)?, 8);
        assert_eq!(*memo.get_or_try_insert_ref("overflow", compute)?, 8);
        assert_eq!(calls.get(), MAX_DISTINCT + 2);
        Ok(())
    }
}
