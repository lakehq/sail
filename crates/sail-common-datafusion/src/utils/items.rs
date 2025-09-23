use std::collections::VecDeque;
use std::fmt::Debug;

use datafusion_common::{plan_datafusion_err, plan_err};
use either::Either;

/// A trait for taking items from a container of expected size.
pub trait ItemTaker {
    type Item;

    fn zero(self) -> datafusion_common::Result<()>;
    fn one(self) -> datafusion_common::Result<Self::Item>;
    fn two(self) -> datafusion_common::Result<(Self::Item, Self::Item)>;
    fn three(self) -> datafusion_common::Result<(Self::Item, Self::Item, Self::Item)>;
    #[allow(clippy::type_complexity, dead_code)]
    fn four(self) -> datafusion_common::Result<(Self::Item, Self::Item, Self::Item, Self::Item)>;
    fn at_least_one(self) -> datafusion_common::Result<(Self::Item, Vec<Self::Item>)>;
    fn one_or_more(self) -> datafusion_common::Result<Either<Self::Item, Vec<Self::Item>>>;
}

impl<T: Debug> ItemTaker for Vec<T> {
    type Item = T;

    fn zero(self) -> datafusion_common::Result<()> {
        if !self.is_empty() {
            return plan_err!("zero values expected: {:?}", self);
        }
        Ok(())
    }

    fn one(mut self) -> datafusion_common::Result<T> {
        if self.len() != 1 {
            return plan_err!("one value expected: {:?}", self);
        }
        self.pop()
            .ok_or_else(|| plan_datafusion_err!("expected one value, but got an empty vector"))
    }

    fn two(mut self) -> datafusion_common::Result<(T, T)> {
        if self.len() != 2 {
            return plan_err!("two values expected: {:?}", self);
        }
        let second = self
            .pop()
            .ok_or_else(|| plan_datafusion_err!("expected two values, but got an empty vector"))?;
        let first = self
            .pop()
            .ok_or_else(|| plan_datafusion_err!("expected two values, but got an empty vector"))?;
        Ok((first, second))
    }

    fn three(mut self) -> datafusion_common::Result<(T, T, T)> {
        if self.len() != 3 {
            return plan_err!("three values expected: {:?}", self);
        }
        let third = self.pop().ok_or_else(|| {
            plan_datafusion_err!("expected three values, but got an empty vector")
        })?;
        let second = self.pop().ok_or_else(|| {
            plan_datafusion_err!("expected three values, but got an empty vector")
        })?;
        let first = self.pop().ok_or_else(|| {
            plan_datafusion_err!("expected three values, but got an empty vector")
        })?;
        Ok((first, second, third))
    }

    fn four(mut self) -> datafusion_common::Result<(T, T, T, T)> {
        if self.len() != 4 {
            return plan_err!("four values expected: {:?}", self);
        }
        let fourth = self
            .pop()
            .ok_or_else(|| plan_datafusion_err!("expected four values, but got an empty vector"))?;
        let third = self
            .pop()
            .ok_or_else(|| plan_datafusion_err!("expected four values, but got an empty vector"))?;
        let second = self
            .pop()
            .ok_or_else(|| plan_datafusion_err!("expected four values, but got an empty vector"))?;
        let first = self
            .pop()
            .ok_or_else(|| plan_datafusion_err!("expected four values, but got an empty vector"))?;
        Ok((first, second, third, fourth))
    }

    fn at_least_one(self) -> datafusion_common::Result<(T, Vec<T>)> {
        if self.is_empty() {
            return plan_err!("at least one value expected: {:?}", self);
        }
        let mut deque: VecDeque<T> = VecDeque::from(self);
        let first = deque.pop_front().ok_or_else(|| {
            plan_datafusion_err!("expected at least one value, but got an empty vector")
        })?;
        let vec: Vec<T> = Vec::from(deque);
        Ok((first, vec))
    }

    fn one_or_more(mut self) -> datafusion_common::Result<Either<T, Vec<T>>> {
        if self.is_empty() {
            return plan_err!("one or more values expected: {:?}", self);
        }
        if self.len() == 1 {
            Ok(Either::Left(self.pop().ok_or_else(|| {
                plan_datafusion_err!("expected one or more values, but got an empty vector")
            })?))
        } else {
            Ok(Either::Right(self))
        }
    }
}
