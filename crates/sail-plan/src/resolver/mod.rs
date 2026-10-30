use std::sync::{Arc, Mutex};

use datafusion::prelude::SessionContext;

use crate::config::PlanConfig;

mod command;
mod constraint;
mod data_type;
mod expression;
mod function;
mod lakehouse;
mod lineage;
mod literal;
pub mod plan;
mod query;
mod schema;
mod state;
mod tree;

pub struct PlanResolver<'a> {
    ctx: &'a SessionContext,
    config: Arc<PlanConfig>,
    lineage_outputs: Mutex<lineage::LineageOutputs>,
}

impl<'a> PlanResolver<'a> {
    pub fn new(ctx: &'a SessionContext, config: Arc<PlanConfig>) -> Self {
        Self {
            ctx,
            config,
            lineage_outputs: Mutex::new(Default::default()),
        }
    }
}
