//! Query analysis: deferred requests, ID-based bindings, ordered layers,
//! and checked lowering to the execution backend.

#![doc = include_str!("architecture.md")]

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::prelude::SessionContext;
pub use sail_common_plan::deferred;

use crate::config::PlanConfig;
use crate::error::PlanResult;

pub mod lowering;
pub mod model;
pub(crate) mod ordering;

pub(crate) mod row_identity;
pub(crate) mod scalar_iterator;
pub mod scope;
pub use sail_common_plan::tree;
pub mod typing;

use model::IdGenerator;
pub use model::{Plan, PlanKind};
pub use state::PlanState;
pub mod merge;
pub mod row_level;
pub mod state;
pub mod writer;

use crate::resolver::PlanResolver;

pub struct AnalyzerContext<'a> {
    pub session: &'a SessionContext,
    pub config: Arc<PlanConfig>,
}

impl AnalyzerContext<'_> {
    pub(crate) fn function_context<'a>(
        &'a self,
        ids: &'a mut IdGenerator,
    ) -> crate::function::common::FunctionContextInput<'a> {
        crate::function::common::FunctionContextInput {
            session: self.session,
            config: &self.config,
            ids,
        }
    }
}

/// A pass over a complete query or command, including its nested inputs.
#[async_trait]
pub trait AnalyzerLayer: Send + Sync {
    async fn analyze(&mut self, plan: Plan) -> PlanResult<Plan>;
}

/// Analysis layers execute once in their configured order.
pub struct AnalyzerPipeline<'a> {
    layers: Vec<Box<dyn AnalyzerLayer + 'a>>,
}

impl Default for AnalyzerPipeline<'_> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> AnalyzerPipeline<'a> {
    pub fn new() -> Self {
        Self { layers: vec![] }
    }

    pub fn with_layer(mut self, layer: impl AnalyzerLayer + 'a) -> Self {
        self.layers.push(Box::new(layer));
        self
    }

    pub async fn analyze(&mut self, plan: Plan) -> PlanResult<Plan> {
        let mut plan = plan;
        for layer in &mut self.layers {
            plan = layer.analyze(plan).await?;
        }
        Ok(plan)
    }
}

/// Backend execution input and the query's public output names.
#[derive(Debug)]
pub struct NamedPlan {
    pub plan: datafusion_expr::LogicalPlan,
    pub fields: Option<Vec<String>>,
}

/// Entry point for query and command analysis.
pub struct PlanAnalyzer<'a> {
    session: &'a SessionContext,
    config: Arc<PlanConfig>,
    pipeline: AnalyzerPipeline<'a>,
}

impl<'a> PlanAnalyzer<'a> {
    pub fn new(session: &'a SessionContext, config: Arc<PlanConfig>) -> Self {
        Self {
            session,
            config: Arc::clone(&config),
            pipeline: AnalyzerPipeline::new().with_layer(PlanResolver::new(session, config)),
        }
    }

    pub fn with_layer(mut self, layer: impl AnalyzerLayer + 'a) -> Self {
        self.pipeline = self.pipeline.with_layer(layer);
        self
    }

    pub async fn analyze(&mut self, plan: impl Into<Plan>) -> PlanResult<Plan> {
        self.pipeline.analyze(plan.into()).await
    }

    pub async fn plan(&mut self, plan: sail_common::spec::Plan) -> PlanResult<NamedPlan> {
        self.analyze(plan).await?.lower(self.session).await
    }

    pub fn data_type(
        &self,
        data_type: &sail_common::spec::DataType,
    ) -> PlanResult<arrow_schema::DataType> {
        crate::resolver::data_type::DataTypeResolver {
            config: &self.config,
            allow_large_var_types: false,
        }
        .resolve_data_type(data_type)
    }
}
