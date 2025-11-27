// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, BooleanArray, UInt64Array};
use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::catalog::Session;
use datafusion::common::pruning::PruningStatistics;
use datafusion::common::{Column, Result, ToDFSchema};
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use datafusion::physical_optimizer::pruning::PruningPredicate;

use crate::spec::partition::PartitionSpec;
use crate::spec::types::values::{Datum, Literal, PrimitiveLiteral};
use crate::spec::types::{PrimitiveType, Type};
use crate::spec::{DataFile, Manifest, ManifestContentType, ManifestList, Schema};
use crate::utils::conversions::{scalar_to_primitive_literal, to_scalar};
// TODO: Implement robust logical expression parsing for summary pruning

/// Pruning statistics over Iceberg DataFiles
pub struct IcebergPruningStats {
    files: Vec<DataFile>,
    #[allow(unused)]
    arrow_schema: Arc<ArrowSchema>,
    /// Arrow field name -> Iceberg field id
    name_to_field_id: HashMap<String, i32>,
    /// Iceberg field id -> Iceberg primitive type (for proper ScalarValue typing)
    field_id_to_type: HashMap<i32, PrimitiveType>,
    min_cache: RefCell<HashMap<i32, ArrayRef>>,
    max_cache: RefCell<HashMap<i32, ArrayRef>>,
    nulls_cache: RefCell<HashMap<i32, ArrayRef>>,
    rows_cache: RefCell<Option<ArrayRef>>,
}

impl IcebergPruningStats {
    pub fn new(
        files: Vec<DataFile>,
        arrow_schema: Arc<ArrowSchema>,
        iceberg_schema: &Schema,
    ) -> Self {
        let mut name_to_field_id = HashMap::new();
        let mut field_id_to_type = HashMap::new();
        for f in iceberg_schema.fields().iter() {
            name_to_field_id.insert(f.name.clone(), f.id);
            if let crate::spec::types::Type::Primitive(p) = f.field_type.as_ref() {
                field_id_to_type.insert(f.id, p.clone());
            }
        }
        Self {
            files,
            arrow_schema,
            name_to_field_id,
            field_id_to_type,
            min_cache: RefCell::new(HashMap::new()),
            max_cache: RefCell::new(HashMap::new()),
            nulls_cache: RefCell::new(HashMap::new()),
            rows_cache: RefCell::new(None),
        }
    }

    fn field_id_for(&self, column: &Column) -> Option<i32> {
        self.name_to_field_id.get(&column.name).copied()
    }

    fn datum_to_scalar_for_field(
        &self,
        field_id: i32,
        datum: &Datum,
    ) -> datafusion::common::scalar::ScalarValue {
        // Use the unified conversion with type context
        let iceberg_type = self
            .field_id_to_type
            .get(&field_id)
            .map(|p| Type::Primitive(p.clone()))
            .unwrap_or_else(|| Type::Primitive(datum.r#type.clone()));

        match to_scalar(&Literal::Primitive(datum.literal.clone()), &iceberg_type) {
            Ok(value) => value,
            Err(err) => {
                log::warn!(
                    "Failed to convert literal for field {} during pruning: {}",
                    field_id,
                    err
                );
                datafusion::common::scalar::ScalarValue::Null
            }
        }
    }
}

impl PruningStatistics for IcebergPruningStats {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        // TODO: Materialize arrays only for columns referenced by the predicate
        let field_id = self.field_id_for(column)?;
        if let Some(arr) = self.min_cache.borrow().get(&field_id) {
            return Some(arr.clone());
        }
        let scalars = self.files.iter().map(|f| {
            f.lower_bounds()
                .get(&field_id)
                .map(|d| self.datum_to_scalar_for_field(field_id, d))
        });
        let values =
            scalars.map(|opt| opt.unwrap_or(datafusion::common::scalar::ScalarValue::Null));
        let arr = datafusion::common::scalar::ScalarValue::iter_to_array(values).ok()?;
        self.min_cache.borrow_mut().insert(field_id, arr.clone());
        Some(arr)
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        let field_id = self.field_id_for(column)?;
        if let Some(arr) = self.max_cache.borrow().get(&field_id) {
            return Some(arr.clone());
        }
        let scalars = self.files.iter().map(|f| {
            f.upper_bounds()
                .get(&field_id)
                .map(|d| self.datum_to_scalar_for_field(field_id, d))
        });
        let values =
            scalars.map(|opt| opt.unwrap_or(datafusion::common::scalar::ScalarValue::Null));
        let arr = datafusion::common::scalar::ScalarValue::iter_to_array(values).ok()?;
        self.max_cache.borrow_mut().insert(field_id, arr.clone());
        Some(arr)
    }

    fn num_containers(&self) -> usize {
        self.files.len()
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        let field_id = self.field_id_for(column)?;
        if let Some(arr) = self.nulls_cache.borrow().get(&field_id) {
            return Some(arr.clone());
        }
        let counts: Vec<u64> = self
            .files
            .iter()
            .map(|f| f.null_value_counts().get(&field_id).copied().unwrap_or(0))
            .collect();
        let arr: ArrayRef = Arc::new(UInt64Array::from(counts));
        self.nulls_cache.borrow_mut().insert(field_id, arr.clone());
        Some(arr)
    }

    fn row_counts(&self, _column: &Column) -> Option<ArrayRef> {
        if let Some(arr) = self.rows_cache.borrow().as_ref() {
            return Some(arr.clone());
        }
        let rows: Vec<u64> = self.files.iter().map(|f| f.record_count()).collect();
        let arr: ArrayRef = Arc::new(UInt64Array::from(rows));
        *self.rows_cache.borrow_mut() = Some(arr.clone());
        Some(arr)
    }

    fn contained(
        &self,
        _column: &Column,
        _value: &std::collections::HashSet<datafusion::common::scalar::ScalarValue>,
    ) -> Option<BooleanArray> {
        let field_id = self.field_id_for(_column)?;
        let mut result = Vec::with_capacity(self.files.len());
        for f in &self.files {
            let lower = f.lower_bounds().get(&field_id);
            let upper = f.upper_bounds().get(&field_id);
            if let (Some(lb), Some(ub)) = (lower, upper) {
                let lb_sv = self.datum_to_scalar_for_field(field_id, lb);
                let ub_sv = self.datum_to_scalar_for_field(field_id, ub);
                let mut any_match = false;
                for v in _value.iter() {
                    if &lb_sv == v && &ub_sv == v {
                        any_match = true;
                        break;
                    }
                }
                result.push(any_match);
            } else {
                result.push(false);
            }
        }
        Some(BooleanArray::from(result))
    }
}

/// Prune Iceberg data files using DataFusion PruningPredicate over IcebergPruningStats
pub fn prune_files(
    session: &dyn Session,
    filters: &[Expr],
    limit: Option<usize>,
    logical_schema: Arc<ArrowSchema>,
    files: Vec<DataFile>,
    iceberg_schema: &Schema,
) -> Result<(Vec<DataFile>, Option<Vec<bool>>)> {
    let filter_expr = conjunction(filters.iter().cloned());

    if filter_expr.is_none() && limit.is_none() {
        return Ok((files, None));
    }

    let stats = IcebergPruningStats::new(files, logical_schema.clone(), iceberg_schema);

    let files_to_keep = if let Some(predicate) = &filter_expr {
        let df_schema = logical_schema.clone().to_dfschema()?;
        let physical_predicate = session.create_physical_expr(predicate.clone(), &df_schema)?;
        let pruning_predicate = PruningPredicate::try_new(physical_predicate, logical_schema)?;
        pruning_predicate.prune(&stats)?
    } else {
        vec![true; stats.num_containers()]
    };

    let mut kept = Vec::new();
    let mut rows_collected: u64 = 0;
    for (file, keep) in stats.files.into_iter().zip(files_to_keep.iter()) {
        if *keep {
            if let Some(lim) = limit {
                if rows_collected <= lim as u64 {
                    rows_collected += file.record_count();
                    kept.push(file);
                    if rows_collected > lim as u64 {
                        break;
                    }
                } else {
                    break;
                }
            } else {
                kept.push(file);
            }
        }
    }

    Ok((kept, Some(files_to_keep)))
}

/// Manifest-level pruning using partition summaries from ManifestList
pub fn prune_manifests_by_partition_summaries<'a>(
    manifest_list: &'a ManifestList,
    table_schema: &Schema,
    partition_specs: &std::collections::HashMap<i32, PartitionSpec>,
    filters: &[Expr],
) -> Vec<&'a crate::spec::manifest_list::ManifestFile> {
    // TODO: Add support for non-identity transforms (day/month/hour/bucket/truncate)
    let eq_filters = collect_identity_eq_filters(table_schema, filters);
    let in_filters = collect_identity_in_filters(table_schema, filters);
    let range_filters = collect_identity_range_filters(table_schema, filters);
    manifest_list
        .entries()
        .iter()
        .filter(|mf| mf.content == ManifestContentType::Data)
        .filter(|mf| {
            if eq_filters.is_empty() {
                return true;
            }
            let Some(spec) = partition_specs.get(&mf.partition_spec_id) else {
                return true;
            };
            let Some(part_summaries) = mf.partitions.as_ref() else {
                return true;
            };
            let part_type = match spec.partition_type(table_schema) {
                Ok(t) => t,
                Err(_) => return true,
            };
            for (source_id, lit) in &eq_filters {
                if let Some((idx, _pf)) = spec.fields().iter().enumerate().find(|(_, pf)| {
                    pf.source_id == *source_id
                        && matches!(pf.transform, crate::spec::transform::Transform::Identity)
                }) {
                    if let Some(summary) = part_summaries.get(idx) {
                        let field_ty = part_type.fields().get(idx).map(|nf| nf.field_type.as_ref());
                        if let Some(Type::Primitive(prim_ty)) = field_ty {
                            // TODO: Handle contains_null/contains_nan from FieldSummary
                            let lower = summary
                                .lower_bound_bytes
                                .as_ref()
                                .and_then(|b| prim_ty.literal_from_bytes(b).ok());
                            let upper = summary
                                .upper_bound_bytes
                                .as_ref()
                                .and_then(|b| prim_ty.literal_from_bytes(b).ok());

                            if let (Some(lb), Some(ub)) = (lower.as_ref(), upper.as_ref()) {
                                if lit < lb || lit > ub {
                                    return false;
                                }
                            } else if let Some(lb) = lower.as_ref() {
                                if lit < lb {
                                    return false;
                                }
                            } else if let Some(ub) = upper.as_ref() {
                                if lit > ub {
                                    return false;
                                }
                            }
                        }
                    }
                }
            }

            for (source_id, lits) in &in_filters {
                if let Some((idx, _pf)) = spec.fields().iter().enumerate().find(|(_, pf)| {
                    pf.source_id == *source_id
                        && matches!(pf.transform, crate::spec::transform::Transform::Identity)
                }) {
                    if let Some(summary) = part_summaries.get(idx) {
                        let field_ty = part_type.fields().get(idx).map(|nf| nf.field_type.as_ref());
                        if let Some(Type::Primitive(prim_ty)) = field_ty {
                            let lower = summary
                                .lower_bound_bytes
                                .as_ref()
                                .and_then(|b| prim_ty.literal_from_bytes(b).ok());
                            let upper = summary
                                .upper_bound_bytes
                                .as_ref()
                                .and_then(|b| prim_ty.literal_from_bytes(b).ok());
                            if let (Some(lb), Some(ub)) = (lower.as_ref(), upper.as_ref()) {
                                let mut any_in = false;
                                for lit in lits {
                                    if !(lit < lb || lit > ub) {
                                        any_in = true;
                                        break;
                                    }
                                }
                                if !any_in {
                                    return false;
                                }
                            } else if let Some(lb) = lower.as_ref() {
                                let any_in = lits.iter().any(|v| v >= lb);
                                if !any_in {
                                    return false;
                                }
                            } else if let Some(ub) = upper.as_ref() {
                                let any_in = lits.iter().any(|v| v <= ub);
                                if !any_in {
                                    return false;
                                }
                            }
                        }
                    }
                }
            }

            for (source_id, range) in &range_filters {
                if let Some((idx, _pf)) = spec.fields().iter().enumerate().find(|(_, pf)| {
                    pf.source_id == *source_id
                        && matches!(pf.transform, crate::spec::transform::Transform::Identity)
                }) {
                    if let Some(summary) = part_summaries.get(idx) {
                        let field_ty = part_type.fields().get(idx).map(|nf| nf.field_type.as_ref());
                        if let Some(Type::Primitive(prim_ty)) = field_ty {
                            let lower = summary
                                .lower_bound_bytes
                                .as_ref()
                                .and_then(|b| prim_ty.literal_from_bytes(b).ok());
                            let upper = summary
                                .upper_bound_bytes
                                .as_ref()
                                .and_then(|b| prim_ty.literal_from_bytes(b).ok());
                            if let (Some(lb), Some(ub)) = (lower.as_ref(), upper.as_ref()) {
                                if let Some((ref qmin, _incl_min)) = range.min {
                                    if qmin > ub {
                                        return false;
                                    }
                                }
                                if let Some((ref qmax, _incl_max)) = range.max {
                                    if qmax < lb {
                                        return false;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            true
        })
        .collect()
}

/// Load a manifest and prune entries by partition+metrics
pub fn prune_manifest_entries(
    manifest: &Manifest,
    _schema: &Schema,
    _filters: &[Expr],
) -> Vec<DataFile> {
    // TODO: Add partition-transform awareness and metrics-only pruning at manifest entry granularity
    manifest
        .entries()
        .iter()
        .filter(|e| {
            matches!(
                e.status,
                crate::spec::manifest::ManifestStatus::Added
                    | crate::spec::manifest::ManifestStatus::Existing
            )
        })
        .map(|e| e.data_file.clone())
        .collect()
}

fn collect_identity_eq_filters(schema: &Schema, filters: &[Expr]) -> Vec<(i32, PrimitiveLiteral)> {
    fn strip(expr: &Expr) -> &Expr {
        match expr {
            Expr::Cast(c) => strip(&c.expr),
            Expr::Alias(a) => strip(&a.expr),
            _ => expr,
        }
    }

    let mut result = Vec::new();

    fn visit_expr(acc: &mut Vec<(i32, PrimitiveLiteral)>, schema: &Schema, e: &Expr) {
        match e {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) if *op == Operator::Eq => {
                let l = strip(left);
                let r = strip(right);

                // col = lit
                if let Expr::Column(c) = l {
                    if let Expr::Literal(sv, _) = r {
                        let col_name = c.name.clone();
                        if let Some(field) = schema.field_by_name(&col_name) {
                            if let Ok(pl) = scalar_to_primitive_literal(sv) {
                                acc.push((field.id, pl));
                                return;
                            }
                        }
                    }
                }

                // lit = col
                if let Expr::Literal(sv, _) = l {
                    if let Expr::Column(c) = r {
                        let col_name = c.name.clone();
                        if let Some(field) = schema.field_by_name(&col_name) {
                            if let Ok(pl) = scalar_to_primitive_literal(sv) {
                                acc.push((field.id, pl));
                            }
                        }
                    }
                }
            }
            Expr::BinaryExpr(BinaryExpr { left, op, right }) if *op == Operator::And => {
                visit_expr(acc, schema, left);
                visit_expr(acc, schema, right);
            }
            _ => {}
        }
    }

    for expr in filters {
        visit_expr(&mut result, schema, expr);
    }
    result
}

fn collect_identity_in_filters(
    schema: &Schema,
    filters: &[Expr],
) -> std::collections::HashMap<i32, Vec<PrimitiveLiteral>> {
    fn strip(expr: &Expr) -> &Expr {
        match expr {
            Expr::Cast(c) => strip(&c.expr),
            Expr::Alias(a) => strip(&a.expr),
            _ => expr,
        }
    }

    let mut result: std::collections::HashMap<_, Vec<_>> = std::collections::HashMap::new();

    fn visit_expr(
        acc: &mut std::collections::HashMap<i32, Vec<PrimitiveLiteral>>,
        schema: &Schema,
        e: &Expr,
    ) {
        match e {
            Expr::InList(in_list) if !in_list.negated => {
                let e = strip(&in_list.expr);
                if let Expr::Column(c) = e {
                    if let Some(field) = schema.field_by_name(&c.name) {
                        let mut vals = Vec::new();
                        for item in &in_list.list {
                            if let Expr::Literal(ref sv, _) = item {
                                if let Ok(pl) = scalar_to_primitive_literal(sv) {
                                    vals.push(pl);
                                }
                            }
                        }
                        if !vals.is_empty() {
                            acc.entry(field.id).or_default().extend(vals);
                        }
                    }
                }
            }
            Expr::BinaryExpr(BinaryExpr { left, op, right }) if *op == Operator::And => {
                visit_expr(acc, schema, left);
                visit_expr(acc, schema, right);
            }
            _ => {}
        }
    }

    for expr in filters {
        visit_expr(&mut result, schema, expr);
    }
    result
}

#[derive(Clone, Default)]
struct RangeConstraint {
    min: Option<(PrimitiveLiteral, bool)>,
    max: Option<(PrimitiveLiteral, bool)>,
}

fn collect_identity_range_filters(
    schema: &Schema,
    filters: &[Expr],
) -> std::collections::HashMap<i32, RangeConstraint> {
    fn strip(expr: &Expr) -> &Expr {
        match expr {
            Expr::Cast(c) => strip(&c.expr),
            Expr::Alias(a) => strip(&a.expr),
            _ => expr,
        }
    }

    fn tighten_min(cur: &mut Option<(PrimitiveLiteral, bool)>, cand: (PrimitiveLiteral, bool)) {
        match cur {
            None => *cur = Some(cand),
            Some((ref mut v, ref mut incl)) => {
                if cand.0 > *v || (cand.0 == *v && cand.1 && !*incl) {
                    *v = cand.0;
                    *incl = cand.1;
                }
            }
        }
    }
    fn tighten_max(cur: &mut Option<(PrimitiveLiteral, bool)>, cand: (PrimitiveLiteral, bool)) {
        match cur {
            None => *cur = Some(cand),
            Some((ref mut v, ref mut incl)) => {
                if cand.0 < *v || (cand.0 == *v && cand.1 && !*incl) {
                    *v = cand.0;
                    *incl = cand.1;
                }
            }
        }
    }

    let mut result: std::collections::HashMap<i32, RangeConstraint> =
        std::collections::HashMap::new();

    fn visit_expr(
        acc: &mut std::collections::HashMap<i32, RangeConstraint>,
        schema: &Schema,
        e: &Expr,
    ) {
        if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = e {
            let l = strip(left);
            let r = strip(right);
            match op {
                Operator::Gt | Operator::GtEq => {
                    if let (Expr::Column(c), Expr::Literal(sv, _)) = (l, r) {
                        if let Some(field) = schema.field_by_name(&c.name) {
                            if let Ok(pl) = scalar_to_primitive_literal(sv) {
                                let entry = acc.entry(field.id).or_default();
                                tighten_min(&mut entry.min, (pl, *op == Operator::GtEq));
                            }
                        }
                    }
                }
                Operator::Lt | Operator::LtEq => {
                    if let (Expr::Column(c), Expr::Literal(sv, _)) = (l, r) {
                        if let Some(field) = schema.field_by_name(&c.name) {
                            if let Ok(pl) = scalar_to_primitive_literal(sv) {
                                let entry = acc.entry(field.id).or_default();
                                tighten_max(&mut entry.max, (pl, *op == Operator::LtEq));
                            }
                        }
                    }
                }
                Operator::And => {
                    visit_expr(acc, schema, l);
                    visit_expr(acc, schema, r);
                }
                _ => {}
            }
        }
    }

    for expr in filters {
        visit_expr(&mut result, schema, expr);
    }
    result
}
