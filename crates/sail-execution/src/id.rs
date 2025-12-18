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
        #[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
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

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };
}

define_id_type!(JobId, u64);
define_id_type!(TaskId, u64);
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

#[derive(Clone, Eq, Hash, PartialEq)]
pub struct TaskInstance {
    pub job_id: JobId,
    pub task_id: TaskId,
    pub attempt: usize,
}
