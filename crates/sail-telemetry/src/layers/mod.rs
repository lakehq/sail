//! Layers for tracing gRPC clients and servers.
//!
//! This module is inspired by `fastrace-tonic` but we maintain our own implementation
//! to have more control over span names and attributes.

pub mod client;
pub mod server;

pub use client::{TracingClientLayer, TracingClientService};
pub use server::{TracingServerLayer, TracingServerService};
