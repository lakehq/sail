// SPDX-License-Identifier: Apache-2.0

mod inferred_type;
pub(crate) mod timestamp;
pub(crate) mod type_coercion;
pub(crate) mod validation;

pub(crate) use inferred_type::{InferredType, TypeMerger, merge_decimals};
