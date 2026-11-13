use super::*;

pub(super) struct MergeClauseSelection {
    pub ordinal: String,
    pub inserts: Vec<i32>,
    pub matched: Vec<i32>,
    pub by_source: Vec<i32>,
}

impl MergeClauseSelection {
    pub fn apply(
        input: LogicalPlan,
        options: &mut MergeIntoOptions,
        fast_append: bool,
    ) -> Result<(LogicalPlan, Self)> {
        let mut occupied = input
            .schema()
            .fields()
            .iter()
            .map(|f| merge_name_key(f.name(), options.case_sensitive))
            .collect();
        let ordinal = collision_free_source_alias("clause", &mut occupied, options.case_sensitive);
        let mut next = 1_i32;
        let mut select = |conditions: Vec<Option<ExprWithSource>>| -> Result<(Expr, Vec<i32>)> {
            let mut ids = Vec::new();
            let mut branches = Vec::new();
            for condition in conditions {
                let id = next;
                next += 1;
                ids.push(id);
                branches.push((
                    Box::new(condition.map(|c| c.expr).unwrap_or_else(|| lit(true))),
                    Box::new(lit(id)),
                ));
            }
            let expr = if branches.is_empty() {
                lit(0_i32)
            } else {
                Expr::Case(Case {
                    expr: None,
                    when_then_expr: branches,
                    else_expr: Some(Box::new(lit(0_i32))),
                })
            };
            Ok((expr, ids))
        };
        let (matched, matched_ids) = select(
            options
                .matched_clauses
                .iter()
                .map(|c| c.condition.clone())
                .collect(),
        )?;
        let (by_source, by_source_ids) = select(
            options
                .not_matched_by_source_clauses
                .iter()
                .map(|c| c.condition.clone())
                .collect(),
        )?;
        let (insert, inserts) = select(
            options
                .not_matched_by_target_clauses
                .iter()
                .map(|c| c.condition.clone())
                .collect(),
        )?;
        let expr = if fast_append {
            insert
        } else {
            when(col(TARGET_PRESENT_COLUMN).is_null(), insert)
                .when(col(SOURCE_PRESENT_COLUMN).is_null(), by_source)
                .otherwise(matched)?
        };
        let selected = |id| Some(ExprWithSource::new(col(&ordinal).eq(lit(id)), None));
        for (clause, id) in options.matched_clauses.iter_mut().zip(&matched_ids) {
            clause.condition = selected(*id);
        }
        for (clause, id) in options
            .not_matched_by_source_clauses
            .iter_mut()
            .zip(&by_source_ids)
        {
            clause.condition = selected(*id);
        }
        for (clause, id) in options
            .not_matched_by_target_clauses
            .iter_mut()
            .zip(&inserts)
        {
            clause.condition = selected(*id);
        }
        let mut projections = input
            .schema()
            .columns()
            .into_iter()
            .map(Expr::Column)
            .collect::<Vec<_>>();
        projections.push(expr.alias(&ordinal));
        Ok((
            evaluate_once(input, projections)?,
            Self {
                ordinal,
                inserts,
                matched: matched_ids,
                by_source: by_source_ids,
            },
        ))
    }

    pub fn predicates(&self, ids: &[i32]) -> Vec<Expr> {
        ids.iter()
            .map(|id| col(&self.ordinal).eq(lit(*id)))
            .collect()
    }

    pub fn insert_predicate(&self) -> Expr {
        combine_disjunction(
            &self
                .inserts
                .iter()
                .map(|id| col(&self.ordinal).eq(lit(*id)))
                .collect::<Vec<_>>(),
        )
        .unwrap_or_else(|| lit(false))
    }
}

/// Capture each RHS occurrence against the original joined row before structural
/// alignment and simultaneous assignment. Never share slots across occurrences.
pub(super) fn capture_merge_values(
    input: LogicalPlan,
    options: &mut MergeIntoOptions,
    target_schema: &DFSchemaRef,
    path_column: &str,
    row_index_column: Option<&str>,
    metadata: &[&str],
) -> Result<LogicalPlan> {
    let original = options.clone();
    let fields = target_schema
        .fields()
        .iter()
        .filter(|f| !is_merge_metadata_column(f.name(), path_column, row_index_column, metadata))
        .collect::<Vec<_>>();
    let mut occupied = input
        .schema()
        .fields()
        .iter()
        .chain(target_schema.fields().iter())
        .map(|f| merge_name_key(f.name(), options.case_sensitive))
        .collect();
    let mut projections = input
        .schema()
        .columns()
        .into_iter()
        .map(Expr::Column)
        .collect::<Vec<_>>();
    let mut capture = |value: Expr,
                       predicate: Expr,
                       field: &datafusion_common::arrow::datatypes::Field,
                       alignment|
     -> Result<Expr> {
        let source_type = value.get_type(input.schema())?;
        let slot = collision_free_source_alias("value", &mut occupied, original.case_sensitive);
        projections.push(
            when(predicate, value)
                .otherwise(lit(ScalarValue::try_new_null(&source_type)?))?
                .alias(&slot),
        );
        align_row_level_value(
            col(slot),
            &source_type,
            field.data_type(),
            original.case_sensitive,
            alignment,
            &arrow_timezone_label(&original.session_timezone),
        )
    };
    for clause in &mut options.matched_clauses {
        let predicate = clause
            .condition
            .as_ref()
            .ok_or_else(|| DataFusionError::Internal("missing selected clause".into()))?
            .expr
            .clone();
        match &mut clause.action {
            MergeMatchedAction::UpdateSet(assignments) => {
                for assignment in assignments {
                    let field = merge_target_field(target_schema, &assignment.column)?;
                    assignment.value = capture(
                        assignment.value.clone(),
                        predicate.clone(),
                        field,
                        StructFieldAlignment::Exact,
                    )?;
                }
            }
            MergeMatchedAction::UpdateAll => {
                let mut assignments = Vec::new();
                for field in &fields {
                    if let Some(value) = merge_source_expr(&original, field.name()) {
                        assignments.push(MergeAssignment {
                            column: field.name().clone(),
                            value: capture(
                                value,
                                predicate.clone(),
                                field,
                                StructFieldAlignment::FillMissingWithNull,
                            )?,
                        });
                    }
                }
                clause.action = MergeMatchedAction::UpdateSet(assignments);
            }
            MergeMatchedAction::Delete => {}
        }
    }
    for clause in &mut options.not_matched_by_source_clauses {
        let predicate = clause
            .condition
            .as_ref()
            .ok_or_else(|| DataFusionError::Internal("missing selected clause".into()))?
            .expr
            .clone();
        if let MergeNotMatchedBySourceAction::UpdateSet(assignments) = &mut clause.action {
            for assignment in assignments {
                let field = merge_target_field(target_schema, &assignment.column)?;
                assignment.value = capture(
                    assignment.value.clone(),
                    predicate.clone(),
                    field,
                    StructFieldAlignment::Exact,
                )?;
            }
        }
    }
    for clause in &mut options.not_matched_by_target_clauses {
        let predicate = clause
            .condition
            .as_ref()
            .ok_or_else(|| DataFusionError::Internal("missing selected clause".into()))?
            .expr
            .clone();
        let mut columns = Vec::new();
        let mut values = Vec::new();
        for field in &fields {
            // Omitted identity columns must remain absent, not explicit NULL.
            if original
                .identity_columns
                .iter()
                .any(|(name, _)| merge_names_equal(name, field.name(), original.case_sensitive))
                && merge_insert_omits_identity(&original, &clause.action, field.name())
            {
                continue;
            }
            let (value, alignment) = match &clause.action {
                MergeNotMatchedByTargetAction::InsertAll => (
                    merge_source_expr(&original, field.name())
                        .unwrap_or_else(|| merge_insert_value(&original, field.name())),
                    StructFieldAlignment::FillMissingWithNull,
                ),
                MergeNotMatchedByTargetAction::InsertColumns { columns, values } => (
                    columns
                        .iter()
                        .zip(values)
                        .find(|(name, _)| {
                            merge_names_equal(name, field.name(), original.case_sensitive)
                        })
                        .map(|(_, value)| value.clone())
                        .unwrap_or_else(|| merge_insert_value(&original, field.name())),
                    StructFieldAlignment::Exact,
                ),
            };
            columns.push(field.name().clone());
            values.push(capture(value, predicate.clone(), field, alignment)?);
        }
        clause.action = MergeNotMatchedByTargetAction::InsertColumns { columns, values };
    }
    options.default_column_exprs.clear();
    evaluate_once(input, projections)
}
