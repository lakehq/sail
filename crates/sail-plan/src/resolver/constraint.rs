use datafusion::arrow::datatypes::Schema;
use datafusion_common::{Constraint, Constraints};
use sail_common_datafusion::catalog::CatalogTableConstraint;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    fn resolve_catalog_table_constraint(
        &self,
        constraint: CatalogTableConstraint,
        schema: &Schema,
    ) -> PlanResult<Constraint> {
        let fields = schema.fields();
        let lookup = |kind: &'static str, col: &str| -> PlanResult<usize> {
            let mut positions = fields
                .iter()
                .enumerate()
                .filter(|(_, f)| f.name().eq_ignore_ascii_case(col));
            let Some((first, _)) = positions.next() else {
                return Err(PlanError::invalid(format!(
                    "column for {kind} constraint not found: {col}",
                )));
            };
            if positions.next().is_some() {
                return Err(PlanError::invalid(format!(
                    "column for {kind} constraint is ambiguous: {col}",
                )));
            }
            Ok(first)
        };

        match constraint {
            CatalogTableConstraint::Unique { name: _, columns } => {
                let indices = columns
                    .into_iter()
                    .map(|col| lookup("unique", &col))
                    .collect::<PlanResult<Vec<_>>>()?;
                Ok(Constraint::Unique(indices))
            }
            CatalogTableConstraint::PrimaryKey { name: _, columns } => {
                let indices = columns
                    .into_iter()
                    .map(|col| lookup("primary key", &col))
                    .collect::<PlanResult<Vec<_>>>()?;
                Ok(Constraint::PrimaryKey(indices))
            }
        }
    }

    pub(super) fn resolve_catalog_table_constraints(
        &self,
        constraints: Vec<CatalogTableConstraint>,
        schema: &Schema,
    ) -> PlanResult<Constraints> {
        let constraints = constraints
            .into_iter()
            .map(|c| self.resolve_catalog_table_constraint(c, schema))
            .collect::<PlanResult<Vec<_>>>()?;
        Ok(Constraints::new_unverified(constraints))
    }
}
