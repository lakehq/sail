use std::sync::Arc;

use datafusion_expr::{lit, Extension, Limit, LogicalPlan};
use sail_common::spec;

use crate::error::PlanResult;
use crate::extension::logical::{ShowStringFormat, ShowStringNode, ShowStringStyle};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) async fn resolve_command_show_string(
        &self,
        show: spec::ShowString,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let spec::ShowString {
            input,
            num_rows,
            truncate,
            vertical,
        } = show;
        let input = self.resolve_query_plan(*input, state).await?;
        // add a `Limit` plan so that the optimizer can push down the limit
        let input = LogicalPlan::Limit(Limit {
            skip: Some(Box::new(lit(0))),
            // fetch one more row so that the proper message can be displayed if there is more data
            fetch: Some(Box::new(lit(num_rows as i64 + 1))),
            input: Arc::new(input),
        });
        let style = match vertical {
            true => ShowStringStyle::Vertical,
            false => ShowStringStyle::Default,
        };
        let format = ShowStringFormat::new(style, truncate);
        let names = Self::get_field_names(input.schema(), state)?;
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(ShowStringNode::try_new(
                Arc::new(input),
                names,
                num_rows,
                format,
                "show_string".to_string(),
            )?),
        }))
    }

    pub(super) async fn resolve_command_html_string(
        &self,
        html: spec::HtmlString,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let spec::HtmlString {
            input,
            num_rows,
            truncate,
        } = html;
        let input = self.resolve_query_plan(*input, state).await?;
        let format = ShowStringFormat::new(ShowStringStyle::Html, truncate);
        let names = Self::get_field_names(input.schema(), state)?;
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(ShowStringNode::try_new(
                Arc::new(input),
                names,
                num_rows,
                format,
                "html_string".to_string(),
            )?),
        }))
    }
}
