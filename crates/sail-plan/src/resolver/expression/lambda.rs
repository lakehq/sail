use datafusion_common::DFSchemaRef;
use sail_common::spec;

use crate::error::{PlanError, PlanResult};
use crate::resolver::expression::NamedExpr;
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) async fn resolve_expression_lambda_function(
        &self,
        _function: spec::Expr,
        _arguments: Vec<spec::UnresolvedNamedLambdaVariable>,
        _schema: &DFSchemaRef,
        _state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        Err(PlanError::todo("lambda function"))
    }

    pub(super) async fn resolve_expression_named_lambda_variable(
        &self,
        _variable: spec::UnresolvedNamedLambdaVariable,
        _schema: &DFSchemaRef,
        _state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        Err(PlanError::todo("named lambda variable"))
    }
}
