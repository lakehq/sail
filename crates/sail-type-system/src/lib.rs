//! Shared query-analysis policies. This crate must not depend on function kernels
//! or the plan resolver: both use these policies, including when DataFusion asks
//! a kernel adapter for its result field during physical planning.

pub mod abs;
pub mod array;

pub mod scalar;

pub mod aggregate;
pub mod coercion;

pub mod error;
pub mod policies;

pub mod higher_order;

pub mod window;

pub mod relational;

pub mod number_format;

pub mod data_type;
pub mod schema;

pub mod collection;

pub mod temporal;

pub mod cast;

pub mod store_assignment;

pub mod predicate;
