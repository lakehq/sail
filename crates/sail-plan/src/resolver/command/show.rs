use datafusion_common::ScalarValue;
use sail_common::spec;
use sail_logical_plan::show_string::{ShowStringFormat, ShowStringStyle};

use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::*;
use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(in crate::resolver) async fn show(
        &mut self,
        input: spec::QueryPlan,
        rows: usize,
        truncate: usize,
        style: ShowStringStyle,
        name: &str,
    ) -> PlanResult<CommandKind> {
        let input = self.command_query(input).await?;
        // Spark clients request the complete result with i32::MAX. Do not turn
        // that sentinel into a TopK allocation of billions of entries. Show's
        // execution operator still enforces the requested display row count.
        let input = if rows == i32::MAX as usize {
            input
        } else {
            let schema = input.schema.clone();
            let fetch = i64::try_from(rows)
                .ok()
                .and_then(|n| n.checked_add(1))
                .ok_or_else(|| PlanError::invalid("show row count is too large"))?;
            Query {
                kind: Deferred::Resolved(QueryKind::Limit {
                    input: Box::new(input),
                    skip: Some(Box::new(
                        self.context
                            .function_context(&mut self.state.ids)
                            .literal(ScalarValue::Int64(Some(0))),
                    )),
                    fetch: Some(Box::new(
                        self.context
                            .function_context(&mut self.state.ids)
                            .literal(ScalarValue::Int64(Some(fetch))),
                    )),
                }),
                schema,
            }
        };
        Ok(CommandKind::Show {
            input,
            rows,
            format: ShowStringFormat::new(style, truncate),
            name: name.into(),
        })
    }
}
