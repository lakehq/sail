// Manipulate local filesystem, all methods represent cross-platform filesystem operations.
use std::fs;
// Cross-platform path manipulation.
use std::path::{Path, PathBuf};
use std::num::ParseFloatError;
use std::str::ParseBoolError;

// Parse: args into enums, CLI args into Self, and sub-command into user-defined enum
use clap::{ValueEnum, Parser, Subcommand};

// The set of datatypes that are supported by this implementation of Apache Arrow.
use datafusion::arrow::datatypes::DataType;
// DataFusion “prelude” to simplify importing common types.
use datafusion::prelude::*;
use datafusion::error::DataFusionError;

// Builder acts as builder for initializing a Logger. Env is a set of env vars to configure from.
use env_logger::{Builder, Env};

// LevelFilter is an enum representing the available verbosity level filters of the logger.
use log::{debug, info, trace, LevelFilter};

// This library provides a convenient derive macro for the standard library’s std::error::Error trait.
use thiserror::Error;


fn main() {
    println!("Hello, world!");
}
