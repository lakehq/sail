//! Generic query-analysis type rules and call resolution through DataFusion UDF hooks.

pub mod aggregate;
pub mod cast;
pub mod coercion;
pub mod collection;
pub mod data_type;
pub mod higher_order;
pub mod predicate;
pub mod relational;
pub mod scalar;
pub mod store_assignment;
pub mod temporal;
pub mod window;
