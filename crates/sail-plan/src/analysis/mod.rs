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
pub mod ir;
mod literal;
pub mod lowering;
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

use ir::{IdGenerator, LogicalQuery};

pub struct AnalysisContext<'a> {
    pub session: &'a SessionContext,
    pub config: Arc<PlanConfig>,
    pub ids: IdGenerator,
}

#[async_trait]
pub trait AnalysisLayer: Send + Sync {
    fn name(&self) -> &str;

    async fn transform(
        &self,
        query: LogicalQuery,
        context: &mut AnalysisContext<'_>,
    ) -> PlanResult<LogicalQuery>;
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

    pub async fn analyze(
        &self,
        query: LogicalQuery,
        context: &mut AnalysisContext<'_>,
    ) -> PlanResult<LogicalQuery> {
        let query = self.resolver.transform(query, context).await?;
        self.apply_layers(query, context).await
    }

    pub(super) async fn apply_layers(
        &self,
        mut query: LogicalQuery,
        context: &mut AnalysisContext<'_>,
    ) -> PlanResult<LogicalQuery> {
        for layer in &self.layers {
            query = layer.transform(query, context).await?;
        }
        Ok(query)
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
            pipeline: AnalysisPipeline::new(resolver::ResolveQuery),
        }
    }

    pub fn with_layer(mut self, layer: impl AnalysisLayer + 'static) -> Self {
        self.pipeline = self.pipeline.with_layer(layer);
        self
    }

    pub async fn analyze(
        &self,
        plan: sail_common::spec::Plan,
    ) -> PlanResult<command::AnalyzedPlan> {
        let mut context = AnalysisContext {
            session: self.session,
            config: Arc::clone(&self.config),
            ids: IdGenerator::default(),
        };
        match plan {
            sail_common::spec::Plan::Query(request) => Ok(command::AnalyzedPlan::Query(
                self.pipeline.analyze(request.into(), &mut context).await?,
            )),
            plan => {
                let mut plan = resolver::analyze_plan(plan, &mut context).await?;
                plan.apply_layers(&self.pipeline, &mut context).await?;
                Ok(plan)
            }
        }
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
