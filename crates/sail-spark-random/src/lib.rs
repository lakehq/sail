//! Spark-compatible random number generation.
//!
//! This crate provides a Rust implementation of Spark's XORShiftRandom,
//! which is used for deterministic sampling operations like `randomSplit`.

mod xorshift;

pub use xorshift::SparkXorShiftRandom;
