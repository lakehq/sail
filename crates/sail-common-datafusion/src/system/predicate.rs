use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use datafusion_common::Result;
use futures::FutureExt;

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

/// An extension trait for iterators to support predicate-based filtering and mapping.
pub trait PredicateExt: Sized {
    type Item;

    fn predicate_filter_map<T, U, K, V>(
        self,
        predicate: Predicate<T>,
        key: K,
        value: V,
    ) -> PredicateFilterMap<Self, T, K, V>
    where
        K: Fn(&Self::Item) -> &T,
        V: Fn(Self::Item) -> U,
    {
        PredicateFilterMap {
            input: self,
            predicate,
            key,
            value,
            fetch: usize::MAX,
        }
    }

    fn predicate_filter_flat_map<T, U, K, V, S>(
        self,
        predicate: Predicate<T>,
        key: K,
        value: V,
    ) -> PredicateFilterFlatMap<Self, T, K, V, S::IntoIter>
    where
        K: Fn(&Self::Item) -> &T,
        V: Fn(Self::Item) -> S,
        S: IntoIterator<Item = U>,
    {
        PredicateFilterFlatMap {
            input: self,
            predicate,
            key,
            value,
            fetch: usize::MAX,
            current: None,
        }
    }

    fn predicate_filter_async_flat_map<T, U, K, V>(
        self,
        predicate: Predicate<T>,
        key: K,
        value: V,
    ) -> PredicateFilterAsyncFlatMap<Self, T, K, V>
    where
        K: Fn(&Self::Item) -> &T,
        V: Fn(Self::Item) -> Pin<Box<dyn Future<Output = Result<Vec<U>>> + Send>>,
    {
        PredicateFilterAsyncFlatMap {
            input: self,
            predicate,
            key,
            value,
        }
    }
}

impl<I> PredicateExt for I
where
    I: Iterator,
{
    type Item = I::Item;
}

pub struct PredicateFilterMap<I, T, K, V> {
    input: I,
    predicate: Predicate<T>,
    key: K,
    value: V,
    fetch: usize,
}

impl<I, T, K, V> PredicateFilterMap<I, T, K, V> {
    pub fn fetch(mut self, fetch: usize) -> Self {
        self.fetch = fetch;
        self
    }
}

impl<I, T, K, V, U> Iterator for PredicateFilterMap<I, T, K, V>
where
    I: Iterator,
    K: Fn(&I::Item) -> &T,
    V: Fn(I::Item) -> U,
{
    type Item = Result<U>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.fetch == 0 {
            return None;
        }
        for item in self.input.by_ref() {
            match (self.predicate)((self.key)(&item)) {
                Ok(true) => {
                    self.fetch -= 1;
                    return Some(Ok((self.value)(item)));
                }
                Ok(false) => continue,
                Err(e) => return Some(Err(e)),
            }
        }
        None
    }
}

pub struct PredicateFilterFlatMap<I, T, K, V, C> {
    input: I,
    predicate: Predicate<T>,
    key: K,
    value: V,
    fetch: usize,
    current: Option<C>,
}

impl<I, T, K, V, C> PredicateFilterFlatMap<I, T, K, V, C> {
    pub fn fetch(mut self, fetch: usize) -> Self {
        self.fetch = fetch;
        self
    }
}

impl<I, T, K, V, U, S> Iterator for PredicateFilterFlatMap<I, T, K, V, S::IntoIter>
where
    I: Iterator,
    K: Fn(&I::Item) -> &T,
    V: Fn(I::Item) -> S,
    S: IntoIterator<Item = U>,
{
    type Item = Result<U>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.fetch == 0 {
            return None;
        }
        if let Some(current) = &mut self.current {
            if let Some(item) = current.next() {
                self.fetch -= 1;
                return Some(Ok(item));
            } else {
                self.current = None;
            }
        }
        for item in self.input.by_ref() {
            match (self.predicate)((self.key)(&item)) {
                Ok(true) => {
                    let mut iter = (self.value)(item).into_iter();
                    if let Some(v) = iter.next() {
                        self.fetch -= 1;
                        self.current = Some(iter);
                        return Some(Ok(v));
                    }
                }
                Ok(false) => continue,
                Err(e) => return Some(Err(e)),
            }
        }
        None
    }
}

pub struct PredicateFilterAsyncFlatMap<I, T, K, V> {
    input: I,
    predicate: Predicate<T>,
    key: K,
    value: V,
}

impl<I, T, K, V, U> PredicateFilterAsyncFlatMap<I, T, K, V>
where
    I: Iterator,
    K: Fn(&I::Item) -> &T,
    V: Fn(I::Item) -> Pin<Box<dyn Future<Output = Result<Vec<U>>> + Send>>,
{
    pub fn into_task(self) -> PredicateFilterAsyncFlatMapTask<U> {
        let mut tasks = VecDeque::new();

        for item in self.input {
            match (self.predicate)((self.key)(&item)) {
                Ok(true) => {
                    let fut = (self.value)(item);
                    tasks.push_back(fut);
                }
                Ok(false) => continue,
                Err(e) => {
                    let fut = async move { Err(e) }.boxed();
                    tasks.push_back(fut);
                }
            }
        }

        PredicateFilterAsyncFlatMapTask {
            tasks,
            fetch: usize::MAX,
        }
    }
}

pub struct PredicateFilterAsyncFlatMapTask<U> {
    #[expect(clippy::type_complexity)]
    tasks: VecDeque<Pin<Box<dyn Future<Output = Result<Vec<U>>> + Send>>>,
    fetch: usize,
}

impl<U> PredicateFilterAsyncFlatMapTask<U> {
    pub fn fetch(mut self, fetch: usize) -> Self {
        self.fetch = fetch;
        self
    }

    pub async fn collect(mut self) -> Result<Vec<U>> {
        let mut items = vec![];

        while let Some(task) = self.tasks.pop_front() {
            let mut v = task.await?;
            if v.len() > self.fetch {
                v.truncate(self.fetch);
            }
            self.fetch -= v.len();
            items.extend(v);
            if self.fetch == 0 {
                break;
            }
        }
        Ok(items)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion_common::{internal_err, Result};

    use super::{Predicate, PredicateExt};

    fn is_even() -> Predicate<i32> {
        Arc::new(|x: &i32| Ok(x % 2 == 0))
    }

    fn error_on_three() -> Predicate<i32> {
        Arc::new(|x: &i32| {
            if *x == 3 {
                internal_err!("error: {x}")
            } else {
                Ok(true)
            }
        })
    }

    #[test]
    fn test_predicate_filter_map() {
        let data = [1, 2, 3, 4, 5];

        let filter_map = |predicate| {
            data.iter()
                .predicate_filter_map(predicate, |&x| x, |x| x * 10)
        };

        let result = filter_map(is_even()).fetch(2).collect::<Result<Vec<_>>>();
        assert_eq!(result.ok(), Some(vec![20, 40]));

        let result = filter_map(error_on_three())
            .fetch(3)
            .collect::<Result<Vec<_>>>();
        assert!(result.is_err());

        let result = filter_map(error_on_three())
            .fetch(1)
            .collect::<Result<Vec<_>>>();
        assert_eq!(result.ok(), Some(vec![10]));
    }

    #[test]
    fn test_predicate_filter_flat_map() {
        let data = [1, 2, 3, 4, 5];

        let filter_flat_map = |predicate| {
            data.iter()
                .predicate_filter_flat_map(predicate, |&x| x, |x| vec![*x; *x as usize])
        };

        let result = filter_flat_map(is_even())
            .fetch(5)
            .collect::<Result<Vec<_>>>();
        assert_eq!(result.ok(), Some(vec![2, 2, 4, 4, 4]));

        let result = filter_flat_map(error_on_three())
            .fetch(4)
            .collect::<Result<Vec<_>>>();
        assert!(result.is_err());

        let result = filter_flat_map(error_on_three())
            .fetch(3)
            .collect::<Result<Vec<_>>>();
        assert_eq!(result.ok(), Some(vec![1, 2, 2]));
    }

    #[tokio::test]
    async fn test_predicate_filter_async_flat_map() {
        let data = [1, 2, 3, 4, 5];

        let task = |predicate| {
            data.iter()
                .predicate_filter_async_flat_map(
                    predicate,
                    |&x| x,
                    |x| {
                        Box::pin({
                            let x = *x;
                            async move { Ok(vec![x; x as usize]) }
                        })
                    },
                )
                .into_task()
        };

        let result = task(is_even()).fetch(5).collect().await;
        assert_eq!(result.ok(), Some(vec![2, 2, 4, 4, 4]));

        let result = task(error_on_three()).fetch(4).collect().await;
        assert!(result.is_err());

        let result = task(error_on_three()).fetch(3).collect().await;
        assert_eq!(result.ok(), Some(vec![1, 2, 2]));
    }
}
