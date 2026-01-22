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

    pub async fn collect(&mut self) -> Result<Vec<U>> {
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
