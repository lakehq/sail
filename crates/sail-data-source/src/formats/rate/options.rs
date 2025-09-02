use std::collections::HashMap;

use datafusion_common::Result;

use crate::options::internal::RateReadOptions;
use crate::options::{load_default_options, load_options};

#[derive(Debug, Clone, Default)]
pub struct TableRateOptions {
    pub rows_per_second: usize,
    pub num_partitions: usize,
}

fn apply_rate_read_options(from: RateReadOptions, to: &mut TableRateOptions) -> Result<()> {
    let RateReadOptions {
        rows_per_second,
        num_partitions,
    } = from;
    if let Some(v) = rows_per_second {
        to.rows_per_second = v;
    }
    if let Some(v) = num_partitions {
        to.num_partitions = v;
    }
    Ok(())
}

pub fn resolve_rate_read_options(
    options: Vec<HashMap<String, String>>,
) -> Result<TableRateOptions> {
    let mut rate_options = TableRateOptions::default();
    apply_rate_read_options(load_default_options()?, &mut rate_options)?;
    for opt in options {
        apply_rate_read_options(load_options(opt)?, &mut rate_options)?;
    }
    Ok(rate_options)
}
