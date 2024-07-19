use std::collections::VecDeque;
use std::fmt::Debug;

use datafusion_common::{plan_err, Result};
use either::Either;

/// A trait for taking items from a container of expected size.
pub(crate) trait ItemTaker {
    type Item;

    fn zero(self) -> Result<()>;
    fn one(self) -> Result<Self::Item>;
    fn two(self) -> Result<(Self::Item, Self::Item)>;
    fn three(self) -> Result<(Self::Item, Self::Item, Self::Item)>;
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

    fn one(mut self) -> Result<T> {
        if self.len() != 1 {
            return plan_err!("one value expected: {:?}", self);
        }
        Ok(self.pop().unwrap())
    }

    fn two(mut self) -> Result<(T, T)> {
        if self.len() != 2 {
            return plan_err!("two values expected: {:?}", self);
        }
        let second = self.pop().unwrap();
        let first = self.pop().unwrap();
        Ok((first, second))
    }

    fn three(mut self) -> Result<(T, T, T)> {
        if self.len() != 3 {
            return plan_err!("three values expected: {:?}", self);
        }
        let third = self.pop().unwrap();
        let second = self.pop().unwrap();
        let first = self.pop().unwrap();
        Ok((first, second, third))
    }

    fn at_least_one(self) -> Result<(T, Vec<T>)> {
        if self.is_empty() {
            return plan_err!("at least one value expected: {:?}", self);
        }
        let mut deque: VecDeque<T> = VecDeque::from(self);
        let first = deque.pop_front().unwrap();
        let vec: Vec<T> = Vec::from(deque);
        Ok((first, vec))
    }

    fn one_or_more(mut self) -> Result<Either<T, Vec<T>>> {
        if self.is_empty() {
            return plan_err!("one or more values expected: {:?}", self);
        }
        if self.len() == 1 {
            Ok(Either::Left(self.pop().unwrap()))
        } else {
            Ok(Either::Right(self))
        }
    }
}
