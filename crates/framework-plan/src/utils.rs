use datafusion_common::{plan_err, Result};
use std::fmt::Debug;

/// A trait for taking items from a container of expected size.
pub(crate) trait ItemTaker {
    type Item;

    fn zero(self) -> Result<()>;
    fn one(self) -> Result<Self::Item>;
    fn two(self) -> Result<(Self::Item, Self::Item)>;
    fn three(self) -> Result<(Self::Item, Self::Item, Self::Item)>;
    fn at_least_one(self) -> Result<(Self::Item, Vec<Self::Item>)>;
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
        let right = self.pop().unwrap();
        let left = self.pop().unwrap();
        Ok((left, right))
    }

    fn three(mut self) -> Result<(T, T, T)> {
        if self.len() != 3 {
            return plan_err!("three values expected: {:?}", self);
        }
        let first = self.pop().unwrap();
        let second = self.pop().unwrap();
        let third = self.pop().unwrap();
        Ok((first, second, third))
    }

    fn at_least_one(mut self) -> Result<(T, Vec<T>)> {
        if self.is_empty() {
            return plan_err!("at least one value expected: {:?}", self);
        }
        let first = self.pop().unwrap();
        Ok((first, self))
    }
}
