use std::fmt::{Debug, Display};
use std::marker::PhantomData;

use crate::error::{ExecutionError, ExecutionResult};

type IdValueType = u64;

pub trait IdType: From<IdValueType> {
    fn name() -> &'static str;
}

pub struct IdGenerator<T> {
    next_id: IdValueType,
    phantom: PhantomData<T>,
}

impl<T: IdType> IdGenerator<T> {
    pub fn new() -> Self {
        Self {
            next_id: 1,
            phantom: PhantomData,
        }
    }

    pub fn next(&mut self) -> ExecutionResult<T> {
        if self.next_id == 0 {
            Err(ExecutionError::InternalError(format!(
                "{} ID overflow",
                T::name()
            )))
        } else {
            let id = self.next_id;
            self.next_id += 1;
            Ok(T::from(id))
        }
    }
}

macro_rules! define_id_type {
    ($name:ident, $type_name:expr) => {
        #[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
        pub struct $name(IdValueType);

        impl From<IdValueType> for $name {
            fn from(id: IdValueType) -> Self {
                Self(id)
            }
        }

        impl From<$name> for IdValueType {
            fn from(id: $name) -> Self {
                id.0
            }
        }

        impl IdType for $name {
            fn name() -> &'static str {
                $type_name
            }
        }

        impl Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };
}

define_id_type!(JobId, "job");
define_id_type!(TaskId, "task");
define_id_type!(WorkerId, "worker");
