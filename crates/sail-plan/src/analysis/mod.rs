//! Native query analysis: deferred requests, ID-based bindings, ordered layers,
//! and checked lowering to the execution backend.

#![doc = include_str!("architecture.md")]

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::prelude::SessionContext;

use crate::config::PlanConfig;
use crate::error::PlanResult;

pub mod command;
pub mod data_type;
pub use sail_query::deferred;
mod literal;
pub mod lowering;
pub mod model;
mod ordering;
pub mod resolver;
mod row_identity;
mod scalar_iterator;
pub mod scope;
pub use sail_query::tree;
pub mod merge;
pub mod row_level;
pub mod typing;
pub mod writer;

use model::{IdGenerator, Plan};

pub struct AnalysisContext<'a> {
    pub session: &'a SessionContext,
    pub config: Arc<PlanConfig>,
    pub ids: IdGenerator,
}

/// A pass over a complete query or command, including its nested inputs.
#[async_trait]
pub trait AnalysisLayer: Send + Sync {
    fn name(&self) -> &str;

    async fn transform(&self, plan: Plan, context: &mut AnalysisContext<'_>) -> PlanResult<Plan>;
}

/// Resolution is mandatory and first. Subsequent layers execute once, in order;
/// there is no implicit fixed-point loop or fallback to backend analysis.
pub struct AnalysisPipeline {
    resolver: Box<dyn AnalysisLayer>,
    layers: Vec<Box<dyn AnalysisLayer>>,
}

impl AnalysisPipeline {
    pub fn new(resolver: impl AnalysisLayer + 'static) -> Self {
        Self {
            resolver: Box::new(resolver),
            layers: vec![],
        }
    }

    pub fn with_layer(mut self, layer: impl AnalysisLayer + 'static) -> Self {
        self.layers.push(Box::new(layer));
        self
    }

    pub async fn analyze(&self, plan: Plan, context: &mut AnalysisContext<'_>) -> PlanResult<Plan> {
        let plan = self.resolver.transform(plan, context).await?;
        self.apply_layers(plan, context).await
    }

    pub(super) async fn apply_layers(
        &self,
        mut plan: Plan,
        context: &mut AnalysisContext<'_>,
    ) -> PlanResult<Plan> {
        for layer in &self.layers {
            plan = layer.transform(plan, context).await?;
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

/// Entry point for native query and command analysis.
pub struct QueryAnalyzer<'a> {
    session: &'a SessionContext,
    config: Arc<PlanConfig>,
    pipeline: AnalysisPipeline,
}

impl<'a> QueryAnalyzer<'a> {
    pub fn new(session: &'a SessionContext, config: Arc<PlanConfig>) -> Self {
        Self {
            session,
            config,
            pipeline: AnalysisPipeline::new(resolver::ResolvePlan),
        }
    }

    pub fn with_layer(mut self, layer: impl AnalysisLayer + 'static) -> Self {
        self.pipeline = self.pipeline.with_layer(layer);
        self
    }

    pub async fn analyze(&self, plan: sail_common::spec::Plan) -> PlanResult<Plan> {
        let mut context = AnalysisContext {
            session: self.session,
            config: Arc::clone(&self.config),
            ids: IdGenerator::default(),
        };
        self.pipeline.analyze(plan.into(), &mut context).await
    }

    pub async fn plan(&self, plan: sail_common::spec::Plan) -> PlanResult<NamedPlan> {
        self.analyze(plan).await?.lower(self.session).await
    }

    pub fn data_type(
        &self,
        data_type: &sail_common::spec::DataType,
    ) -> PlanResult<arrow_schema::DataType> {
        data_type::DataTypeResolver {
            config: &self.config,
            allow_large_var_types: false,
        }
        .resolve_data_type(data_type)
    }
}
