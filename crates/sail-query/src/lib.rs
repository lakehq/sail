//! Dependency-neutral logical query representation and structural transformations.
//! This crate has no resolver, catalog, or execution-kernel dependency.
//! Backend plans and expressions are never stored in the tree.
pub mod deferred;
mod instantiate;
pub mod ir;
pub mod tree;

pub mod udf;

pub mod rewrite;
