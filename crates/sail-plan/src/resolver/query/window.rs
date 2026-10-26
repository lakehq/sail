use sail_common::spec;

use crate::analyzer::model::*;
use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    #[async_recursion::async_recursion]
    pub(in crate::resolver) async fn resolve_query_named_windows(
        &mut self,
        input: spec::QueryPlan,
        windows: Vec<(spec::Identifier, spec::Window)>,
    ) -> PlanResult<Query> {
        Ok({
            let previous = self.windows.clone();
            let mut names = std::collections::HashSet::new();
            for (name, window) in windows {
                let name = name.as_ref().to_ascii_lowercase();
                if !names.insert(name.clone()) {
                    return Err(PlanError::analysis(format!(
                        "Name {name} is used more than once in WINDOW clause"
                    )));
                }
                self.windows.insert(name, window);
            }
            let result = self.resolve_query_plan(input).await;
            self.windows = previous;
            result?
        })
    }
}
