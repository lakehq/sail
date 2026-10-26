//! Native query analysis: deferred requests, ID-based bindings, ordered layers,
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
pub mod merge;
pub mod row_level;
pub mod typing;
pub mod writer;

pub use model::AnalyzerPlan;
use model::IdGenerator;

use crate::resolver::PlanResolver;

pub struct AnalyzerContext<'a> {
    pub session: &'a SessionContext,
    pub config: Arc<PlanConfig>,
    pub ids: IdGenerator,
}

impl AnalyzerContext<'_> {
    pub(crate) fn function_context(&mut self) -> crate::function::common::FunctionContextInput<'_> {
        crate::function::common::FunctionContextInput {
            session: self.session,
            config: &self.config,
            ids: &mut self.ids,
        }
    }
}

/// A pass over a complete query or command, including its nested inputs.
#[async_trait]
pub trait AnalyzerLayer: Send + Sync {
    async fn analyze(&mut self, plan: AnalyzerPlan) -> PlanResult<AnalyzerPlan>;
}

/// Resolution is mandatory and first. Subsequent layers execute once, in order.
pub struct AnalyzerPipeline<'a> {
    resolver: Box<dyn AnalyzerLayer + 'a>,
    layers: Vec<Box<dyn AnalyzerLayer + 'a>>,
}

impl<'a> AnalyzerPipeline<'a> {
    pub fn new(resolver: impl AnalyzerLayer + 'a) -> Self {
        Self {
            resolver: Box::new(resolver),
            layers: vec![],
        }
    }

    pub fn with_layer(mut self, layer: impl AnalyzerLayer + 'a) -> Self {
        self.layers.push(Box::new(layer));
        self
    }

    pub async fn analyze(&mut self, plan: AnalyzerPlan) -> PlanResult<AnalyzerPlan> {
        let mut plan = self.resolver.analyze(plan).await?;
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

/// Entry point for native query and command analysis.
pub struct QueryAnalyzer<'a> {
    session: &'a SessionContext,
    config: Arc<PlanConfig>,
    pipeline: AnalyzerPipeline<'a>,
}

impl<'a> QueryAnalyzer<'a> {
    pub fn new(session: &'a SessionContext, config: Arc<PlanConfig>) -> Self {
        Self {
            session,
            config: Arc::clone(&config),
            pipeline: AnalyzerPipeline::new(PlanResolver::new(session, config)),
        }
    }

    pub fn with_layer(mut self, layer: impl AnalyzerLayer + 'static) -> Self {
        self.pipeline = self.pipeline.with_layer(layer);
        self
    }

    pub async fn analyze(&mut self, plan: sail_common::spec::Plan) -> PlanResult<AnalyzerPlan> {
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
