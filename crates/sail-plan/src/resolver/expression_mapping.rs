use std::collections::{HashSet, VecDeque};

use datafusion_expr::Expr;

#[derive(Debug, Default)]
pub(super) struct ExpressionMappingState {
    /// Expression rewrites recorded while rewriting a logical plan.
    /// The first expression is the original expression and the second is the rewritten expression.
    expression_rewrites: Vec<(Expr, Expr)>,
    /// A map from expressions to visible output field IDs.
    /// This allows later stages (e.g. sort binding) to reuse projected output columns.
    expression_output_fields: Vec<(Expr, String)>,
}

impl ExpressionMappingState {
    pub fn register_expression_rewrite(&mut self, original: Expr, rewritten: Expr) {
        let already_exists = self
            .expression_rewrites
            .iter()
            .any(|(source, target)| source == &original && target == &rewritten);
        if !already_exists {
            self.expression_rewrites.push((original, rewritten));
        }
    }

    pub fn register_expression_output_field(&mut self, expr: Expr, field_id: impl Into<String>) {
        let field_id = field_id.into();
        let mut queue = VecDeque::new();
        queue.push_back(expr);
        let mut visited = Vec::new();
        while let Some(candidate) = queue.pop_front() {
            if visited.iter().any(|seen| seen == &candidate) {
                continue;
            }
            visited.push(candidate.clone());

            let already_exists = self
                .expression_output_fields
                .iter()
                .any(|(source, target)| source == &candidate && target == &field_id);
            if !already_exists {
                self.expression_output_fields
                    .push((candidate.clone(), field_id.clone()));
            }

            let related_exprs = self
                .expression_rewrites
                .iter()
                .filter_map(|(source, target)| {
                    if source == &candidate {
                        Some(target.clone())
                    } else if target == &candidate {
                        Some(source.clone())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            queue.extend(related_exprs);
        }
    }

    pub fn find_output_field_for_expression(
        &self,
        expr: &Expr,
        output_field_ids: &[String],
    ) -> Option<String> {
        let output_field_ids = output_field_ids
            .iter()
            .map(|field_id| field_id.as_str())
            .collect::<HashSet<_>>();
        if output_field_ids.is_empty() {
            return None;
        }

        let mut queue = VecDeque::new();
        queue.push_back(expr.clone());
        let mut visited = Vec::new();
        while let Some(candidate) = queue.pop_front() {
            if visited.iter().any(|seen| seen == &candidate) {
                continue;
            }
            visited.push(candidate.clone());

            if let Some((_, field_id)) = self
                .expression_output_fields
                .iter()
                .find(|(source, id)| source == &candidate && output_field_ids.contains(id.as_str()))
            {
                return Some(field_id.clone());
            }

            for (_, rewritten) in self
                .expression_rewrites
                .iter()
                .filter(|(source, _)| source == &candidate)
            {
                queue.push_back(rewritten.clone());
            }
            for (source, _) in self
                .expression_rewrites
                .iter()
                .filter(|(_, rewritten)| rewritten == &candidate)
            {
                queue.push_back(source.clone());
            }
        }
        None
    }
}
