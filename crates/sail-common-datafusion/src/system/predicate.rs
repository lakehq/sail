use std::sync::Arc;

use datafusion_common::Result;

/// A predicate function.
pub type Predicate<T> = Arc<dyn Fn(&T) -> Result<bool> + Send + Sync>;

/// A collection of common predicate helpers.
pub struct Predicates;

impl Predicates {
    pub fn always_true<T>() -> Predicate<T> {
        Arc::new(|_| Ok(true))
    }

    pub fn always_false<T>() -> Predicate<T> {
        Arc::new(|_| Ok(false))
    }

    pub fn transform<T, U, F>(predicate: Predicate<T>, mapper: F) -> Predicate<U>
    where
        T: 'static,
        F: Fn(&U) -> T + Send + Sync + 'static,
    {
        Arc::new(move |u: &U| {
            let t = mapper(u);
            predicate(&t)
        })
    }
}
