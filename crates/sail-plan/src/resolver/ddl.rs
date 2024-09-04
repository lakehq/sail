use datafusion_common::{Constraint, Constraints, DFSchema};
use sail_common::spec;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) fn resolve_table_constraint(
        &self,
        constraint: spec::TableConstraint,
        schema: &DFSchema,
    ) -> PlanResult<Constraint> {
        use spec::TableConstraint;

        match constraint {
            TableConstraint::Unique { name: _, columns } => {
                let field_names = schema.field_names();
                let indices = columns
                    .into_iter()
                    .map(|col| {
                        let col: String = col.into();
                        let idx = field_names
                            .iter()
                            .position(|item| *item == col)
                            .ok_or_else(|| {
                                PlanError::invalid(format!(
                                    "column for unique constraint not found in schema: {col}.",
                                ))
                            })?;
                        Ok(idx)
                    })
                    .collect::<PlanResult<Vec<_>>>()?;
                Ok(Constraint::Unique(indices))
            }
            TableConstraint::PrimaryKey { name: _, columns } => {
                let field_names = schema.field_names();
                let indices = columns
                    .into_iter()
                    .map(|col| {
                        let col: String = col.into();
                        let idx = field_names
                            .iter()
                            .position(|item| *item == col)
                            .ok_or_else(|| {
                                PlanError::invalid(format!(
                                    "column for primary key not found in schema: {col}",
                                ))
                            })?;
                        Ok(idx)
                    })
                    .collect::<PlanResult<Vec<_>>>()?;
                Ok(Constraint::PrimaryKey(indices))
            }
        }
    }

    pub(super) fn resolve_table_constraints(
        &self,
        constraints: Vec<spec::TableConstraint>,
        schema: &DFSchema,
    ) -> PlanResult<Constraints> {
        let constraints = constraints
            .into_iter()
            .map(|c| self.resolve_table_constraint(c, schema))
            .collect::<PlanResult<Vec<_>>>()?;
        Ok(Constraints::new_unverified(constraints))
    }
}
