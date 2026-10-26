//! Name lookup ends here. Bound expressions carry attribute IDs, so later
//! transformations do not propagate names through expression trees.

use super::model::{Attribute, AttributeId, ExprType, QuerySchema};
use crate::error::{PlanError, PlanResult};

#[derive(Debug, Clone)]
pub struct BoundAttribute {
    pub id: AttributeId,
    /// Zero is local; one is the immediately enclosing query scope.
    pub depth: usize,
    pub result_type: ExprType,
}

pub struct ResolutionScope<'a> {
    /// Innermost first. Each entry is a query scope, not an individual relation.
    schemas: Vec<&'a QuerySchema>,
    case_sensitive: bool,
}

impl<'a> ResolutionScope<'a> {
    pub fn new(local: &'a QuerySchema, outer: impl IntoIterator<Item = &'a QuerySchema>) -> Self {
        Self {
            schemas: std::iter::once(local).chain(outer).collect(),
            case_sensitive: false,
        }
    }

    pub fn with_case_sensitive(mut self, case_sensitive: bool) -> Self {
        self.case_sensitive = case_sensitive;
        self
    }

    pub fn resolve(&self, name: &[String], plan_id: Option<i64>) -> PlanResult<BoundAttribute> {
        self.resolve_optional(name, plan_id)?
            .ok_or_else(|| PlanError::analysis(format!("unresolved attribute: {}", name.join("."))))
    }

    pub fn resolve_optional(
        &self,
        name: &[String],
        plan_id: Option<i64>,
    ) -> PlanResult<Option<BoundAttribute>> {
        let Some((column, qualifier)) = name.split_last() else {
            return Err(PlanError::analysis("empty attribute name"));
        };
        for (depth, schema) in self.schemas.iter().enumerate() {
            let mut candidates = schema.attributes.iter().filter(|attribute| {
                (!attribute.hidden || !qualifier.is_empty() || plan_id.is_some())
                    && matches_name(&attribute.name, column, self.case_sensitive)
                    && matches_qualifier(attribute, qualifier, self.case_sensitive)
                    && plan_id.is_none_or(|id| attribute.plan_ids.contains(&id))
            });
            let Some(attribute) = candidates.next() else {
                continue;
            };
            // Duplicate occurrences of the same attribute have one semantic
            // binding. Distinct origins remain ambiguous even if names match.
            if candidates.any(|other| other.origin != attribute.origin) {
                return Err(PlanError::analysis(format!(
                    "[AMBIGUOUS_REFERENCE] ambiguous attribute: {}",
                    name.join(".")
                )));
            }
            return Ok(Some(BoundAttribute {
                id: attribute.id,
                depth,
                result_type: attribute.result_type.resolved("attribute type")?.clone(),
            }));
        }
        Ok(None)
    }
}

fn matches_name(left: &str, right: &str, case_sensitive: bool) -> bool {
    if case_sensitive {
        left == right
    } else {
        left.eq_ignore_ascii_case(right)
    }
}

fn matches_qualifier(attribute: &Attribute, qualifier: &[String], case_sensitive: bool) -> bool {
    attribute.qualifier.len() >= qualifier.len()
        && attribute.qualifier[attribute.qualifier.len() - qualifier.len()..]
            .iter()
            .zip(qualifier)
            .all(|(left, right)| matches_name(left, right, case_sensitive))
}

#[cfg(test)]
mod tests {
    use arrow_schema::DataType;

    use super::*;
    use crate::analyzer::deferred::Deferred;

    fn attribute(id: u64, origin: u64, qualifier: &str) -> Attribute {
        Attribute {
            id: AttributeId(id),
            origin: AttributeId(origin),
            name: "value".into(),
            qualifier: vec![qualifier.into()],
            qualified_position: 0,
            plan_ids: vec![id as i64],
            hidden: false,
            result_type: Deferred::Resolved(ExprType::new(DataType::Int32, false)),
        }
    }

    #[test]
    fn qualified_binding_does_not_use_display_name_as_identity() -> PlanResult<()> {
        let schema = QuerySchema {
            attributes: vec![attribute(1, 1, "a"), attribute(2, 2, "b")],
        };
        let scope = ResolutionScope::new(&schema, []);
        assert!(scope.resolve(&["value".into()], None).is_err());
        assert_eq!(
            scope.resolve(&["a".into(), "VALUE".into()], None)?.id,
            AttributeId(1)
        );
        assert_eq!(
            scope.resolve(&["value".into()], Some(2))?.id,
            AttributeId(2)
        );
        Ok(())
    }

    #[test]
    fn repeated_origin_resolves_and_outer_scope_tracks_depth() -> PlanResult<()> {
        let schema = QuerySchema {
            attributes: vec![attribute(1, 1, "a"), attribute(2, 1, "a")],
        };
        let empty = QuerySchema::default();
        let scope = ResolutionScope::new(&empty, [&schema]);
        let binding = scope.resolve(&["value".into()], None)?;
        assert_eq!(binding.id, AttributeId(1));
        assert_eq!(binding.depth, 1);
        Ok(())
    }
}
