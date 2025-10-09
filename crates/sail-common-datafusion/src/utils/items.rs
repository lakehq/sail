use std::collections::VecDeque;
use std::fmt::Debug;

use datafusion_common::{plan_datafusion_err, plan_err, Result};
use either::Either;

/// A trait for taking items from a container of expected size.
pub trait ItemTaker {
    type Item;

    fn zero(self) -> Result<()>;
    fn one(self) -> Result<Self::Item>;
    fn two(self) -> Result<(Self::Item, Self::Item)>;
    fn three(self) -> Result<(Self::Item, Self::Item, Self::Item)>;
    #[allow(clippy::type_complexity)]
    fn four(self) -> Result<(Self::Item, Self::Item, Self::Item, Self::Item)>;
    fn at_least_one(self) -> Result<(Self::Item, Vec<Self::Item>)>;
    fn one_or_more(self) -> Result<Either<Self::Item, Vec<Self::Item>>>;
}

impl<T: Debug> ItemTaker for Vec<T> {
    type Item = T;

    fn zero(self) -> Result<()> {
        if !self.is_empty() {
            return plan_err!("zero values expected: {:?}", self);
        }
        Ok(())
    }

    fn one(self) -> Result<T> {
        match <[T; 1] as TryFrom<_>>::try_from(self) {
            Ok([item]) => Ok(item),
            Err(v) => plan_err!("one value expected: {v:?}"),
        }
    }

    fn two(self) -> Result<(T, T)> {
        match <[T; 2] as TryFrom<_>>::try_from(self) {
            Ok([first, second]) => Ok((first, second)),
            Err(v) => plan_err!("two values expected: {v:?}"),
        }
    }

    fn three(self) -> Result<(T, T, T)> {
        match <[T; 3] as TryFrom<_>>::try_from(self) {
            Ok([first, second, third]) => Ok((first, second, third)),
            Err(v) => plan_err!("three values expected: {v:?}"),
        }
    }

    fn four(self) -> Result<(T, T, T, T)> {
        match <[T; 4] as TryFrom<_>>::try_from(self) {
            Ok([first, second, third, fourth]) => Ok((first, second, third, fourth)),
            Err(v) => plan_err!("four values expected: {v:?}"),
        }
    }

    fn at_least_one(self) -> Result<(T, Vec<T>)> {
        let mut deque: VecDeque<T> = VecDeque::from(self);
        let first = deque.pop_front().ok_or_else(|| {
            plan_datafusion_err!("expected at least one value, but got an empty vector")
        })?;
        let vec: Vec<T> = Vec::from(deque);
        Ok((first, vec))
    }

    fn one_or_more(self) -> Result<Either<T, Vec<T>>> {
        match <[T; 1] as TryFrom<_>>::try_from(self) {
            Ok([item]) => Ok(Either::Left(item)),
            Err(v) => {
                if v.is_empty() {
                    plan_err!("expected one or more values, but got an empty vector")
                } else {
                    Ok(Either::Right(v))
                }
            }
        }
    }
}
