use std::fmt;
use std::marker::PhantomData;

use crate::error::{ExecutionError, ExecutionResult};
pub trait IdValueType: Sized {
    fn first() -> Self;
    fn next(v: Self) -> ExecutionResult<Self>;
}

macro_rules! impl_integer_id_value_type {
    ($type:ty) => {
        impl IdValueType for $type {
            fn first() -> Self {
                1
            }

            fn next(v: Self) -> ExecutionResult<Self> {
                v.checked_add(1)
                    .ok_or(ExecutionError::InternalError("ID overflow".to_string()))
            }
        }
    };
}

impl_integer_id_value_type!(u64);

pub trait IdType: Sized {
    type Value: IdValueType + From<Self> + Into<Self>;
}

macro_rules! define_id_type {
    ($name:ident, $value_type:ty) => {
        #[derive(Copy, Clone, Eq, PartialEq, Hash)]
        pub struct $name($value_type);

        impl IdType for $name {
            type Value = $value_type;
        }

        impl From<$value_type> for $name {
            fn from(id: $value_type) -> Self {
                Self(id)
            }
        }

        impl From<$name> for $value_type {
            fn from(id: $name) -> Self {
                id.0
            }
        }

        impl std::fmt::Debug for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}({})", stringify!($name), self.0)
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };
}

define_id_type!(JobId, u64);
define_id_type!(WorkerId, u64);

#[derive(Debug)]
pub struct IdGenerator<T: IdType> {
    next_value: T::Value,
    phantom: PhantomData<T>,
}

impl<T: IdType> IdGenerator<T>
where
    T::Value: Copy,
{
    pub fn new() -> Self {
        Self {
            next_value: T::Value::first(),
            phantom: PhantomData,
        }
    }

    pub fn next(&mut self) -> ExecutionResult<T> {
        let value = self.next_value;
        self.next_value = T::Value::next(value)?;
        Ok(value.into())
    }
}

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub struct TaskKey {
    pub job_id: JobId,
    pub stage: usize,
    pub partition: usize,
    pub attempt: usize,
}

pub struct TaskKeyDisplay<'a>(pub &'a TaskKey);

impl fmt::Display for TaskKeyDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "job {} stage {} partition {} attempt {}",
            self.0.job_id, self.0.stage, self.0.partition, self.0.attempt
        )
    }
}

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub struct TaskStreamKey {
    pub job_id: JobId,
    pub stage: usize,
    pub partition: usize,
    pub attempt: usize,
    pub channel: usize,
}

pub struct TaskStreamKeyDisplay<'a>(pub &'a TaskStreamKey);

impl fmt::Display for TaskStreamKeyDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "job {} stage {} partition {} attempt {} channel {}",
            self.0.job_id, self.0.stage, self.0.partition, self.0.attempt, self.0.channel
        )
    }
}

pub struct TaskStreamKeyDenseDisplay<'a>(pub &'a TaskStreamKey);

impl fmt::Display for TaskStreamKeyDenseDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}/{}/{}/{}/{}",
            self.0.job_id, self.0.stage, self.0.partition, self.0.attempt, self.0.channel
        )
    }
}

impl From<TaskStreamKey> for TaskKey {
    fn from(key: TaskStreamKey) -> Self {
        Self {
            job_id: key.job_id,
            stage: key.stage,
            partition: key.partition,
            attempt: key.attempt,
        }
    }
}
