use datafusion_common::DFSchemaRef;
use log::info;
use sail_common::spec;

use crate::error::{PlanError, PlanResult};
use crate::resolver::expression::NamedExpr;
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) async fn resolve_expression_lambda_function(
        &self,
        function: spec::Expr,
        arguments: Vec<spec::UnresolvedNamedLambdaVariable>,
        _schema: &DFSchemaRef,
        _state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        // DEBUG: Let's see what the lambda looks like
        info!("=== LAMBDA FUNCTION RECEIVED ===");
        info!("Arguments: {:?}", arguments);
        info!("Function body: {:#?}", function);
        info!("================================");
        Err(PlanError::todo("lambda function"))
    }

    pub(super) async fn resolve_expression_named_lambda_variable(
        &self,
        variable: spec::UnresolvedNamedLambdaVariable,
        _schema: &DFSchemaRef,
        _state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        // DEBUG: Let's see what the variable looks like
        info!("=== NAMED LAMBDA VARIABLE ===");
        info!("Variable: {:?}", variable);
        info!("=============================");
        Err(PlanError::todo("named lambda variable"))
    }
}
