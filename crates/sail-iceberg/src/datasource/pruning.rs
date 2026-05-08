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
use std::collections::{HashMap, HashSet};
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
use crate::spec::transform::Transform;
use crate::spec::types::values::{Datum, Literal, PrimitiveLiteral};
use crate::spec::types::{PrimitiveType, Type};
use crate::spec::{DataFile, Manifest, ManifestContentType, ManifestList, Schema};
use crate::utils::conversions::{scalar_to_primitive_literal, to_scalar};
use crate::utils::transform::apply_transform;
// TODO: Implement robust logical expression parsing for summary pruning

/// Pruning statistics over Iceberg DataFiles
pub struct IcebergPruningStats {
    files: Vec<DataFile>,
    #[expect(unused)]
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
                // If stats are missing, we cannot safely prune the file.
                result.push(true);
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
    partition_specs: &HashMap<i32, PartitionSpec>,
    filters: &[Expr],
) -> Vec<&'a crate::spec::manifest_list::ManifestFile> {
    manifest_list
        .entries()
        .iter()
        .filter(|mf| mf.content == ManifestContentType::Data)
        .filter(|mf| {
            let Some(spec) = partition_specs.get(&mf.partition_spec_id) else {
                return true;
            };
            let predicates = partition_predicates_for_spec(table_schema, spec, filters);
            if predicates.is_empty() {
                return true;
            }
            let Some(part_summaries) = mf.partitions.as_ref() else {
                return true;
            };
            let part_type = match spec.partition_type(table_schema) {
                Ok(t) => t,
                Err(_) => return true,
            };
            predicates.iter().all(|predicate| {
                let Some(summary) = part_summaries.get(predicate.field_index) else {
                    return true;
                };
                let Some(Type::Primitive(prim_ty)) = part_type
                    .fields()
                    .get(predicate.field_index)
                    .map(|nf| nf.field_type.as_ref())
                else {
                    return true;
                };
                let lower = summary
                    .lower_bound_bytes
                    .as_ref()
                    .and_then(|b| prim_ty.literal_from_bytes(b).ok());
                let upper = summary
                    .upper_bound_bytes
                    .as_ref()
                    .and_then(|b| prim_ty.literal_from_bytes(b).ok());
                partition_predicate_may_match_bounds(predicate, lower.as_ref(), upper.as_ref())
            })
        })
        .collect()
}

pub fn prune_data_files_by_partition_values(
    files: Vec<DataFile>,
    table_schema: &Schema,
    partition_spec: &PartitionSpec,
    filters: &[Expr],
) -> Vec<DataFile> {
    let predicates = partition_predicates_for_spec(table_schema, partition_spec, filters);
    if predicates.is_empty() {
        return files;
    }

    files
        .into_iter()
        .filter(|file| {
            predicates.iter().all(|predicate| {
                let Some(value) = file.partition().get(predicate.field_index) else {
                    return true;
                };
                partition_predicate_may_match_value(predicate, value.as_ref())
            })
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

fn collect_source_eq_filters(schema: &Schema, filters: &[Expr]) -> Vec<(i32, PrimitiveLiteral)> {
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

fn collect_source_in_filters(
    schema: &Schema,
    filters: &[Expr],
) -> HashMap<i32, Vec<PrimitiveLiteral>> {
    fn strip(expr: &Expr) -> &Expr {
        match expr {
            Expr::Cast(c) => strip(&c.expr),
            Expr::Alias(a) => strip(&a.expr),
            _ => expr,
        }
    }

    let mut result: HashMap<_, Vec<_>> = HashMap::new();

    fn visit_expr(acc: &mut HashMap<i32, Vec<PrimitiveLiteral>>, schema: &Schema, e: &Expr) {
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

#[derive(Clone)]
struct PartitionPredicate {
    field_index: usize,
    constraint: PartitionConstraint,
}

#[derive(Clone)]
enum PartitionConstraint {
    Eq(PrimitiveLiteral),
    In(Vec<PrimitiveLiteral>),
    Range(RangeConstraint),
}

#[derive(Clone, Default)]
struct RangeConstraint {
    min: Option<(PrimitiveLiteral, bool)>,
    max: Option<(PrimitiveLiteral, bool)>,
}

fn partition_predicates_for_spec(
    schema: &Schema,
    spec: &PartitionSpec,
    filters: &[Expr],
) -> Vec<PartitionPredicate> {
    let eq_filters = collect_source_eq_filters(schema, filters);
    let in_filters = collect_source_in_filters(schema, filters);
    let range_filters = collect_source_range_filters(schema, filters);

    let mut predicates = Vec::new();
    for (field_index, partition_field) in spec.fields().iter().enumerate() {
        if matches!(
            partition_field.transform,
            Transform::Void | Transform::Unknown
        ) {
            continue;
        }
        let Some(source_field) = schema.field_by_id(partition_field.source_id) else {
            continue;
        };
        let source_type = source_field.field_type.as_ref();

        for (_, literal) in eq_filters
            .iter()
            .filter(|(source_id, _)| *source_id == partition_field.source_id)
        {
            if let Some(value) =
                transform_primitive_literal(partition_field.transform, source_type, literal.clone())
            {
                predicates.push(PartitionPredicate {
                    field_index,
                    constraint: PartitionConstraint::Eq(value),
                });
            }
        }

        if let Some(literals) = in_filters.get(&partition_field.source_id) {
            let mut values = Vec::new();
            let mut seen = HashSet::new();
            for literal in literals {
                if let Some(value) = transform_primitive_literal(
                    partition_field.transform,
                    source_type,
                    literal.clone(),
                ) {
                    if seen.insert(value.clone()) {
                        values.push(value);
                    }
                }
            }
            if !values.is_empty() {
                predicates.push(PartitionPredicate {
                    field_index,
                    constraint: PartitionConstraint::In(values),
                });
            }
        }

        if let Some(range) = range_filters.get(&partition_field.source_id) {
            if let Some(range) =
                transform_range_constraint(partition_field.transform, source_type, range)
            {
                predicates.push(PartitionPredicate {
                    field_index,
                    constraint: PartitionConstraint::Range(range),
                });
            }
        }
    }

    predicates
}

fn transform_primitive_literal(
    transform: Transform,
    source_type: &Type,
    literal: PrimitiveLiteral,
) -> Option<PrimitiveLiteral> {
    match apply_transform(transform, source_type, Some(Literal::Primitive(literal))) {
        Some(Literal::Primitive(value)) => Some(value),
        _ => None,
    }
}

fn transform_range_constraint(
    transform: Transform,
    source_type: &Type,
    range: &RangeConstraint,
) -> Option<RangeConstraint> {
    if !transform.preserves_order() {
        return None;
    }

    let min = range.min.as_ref().and_then(|(literal, inclusive)| {
        transform_bound_literal(transform, source_type, literal, true, *inclusive)
            .map(|value| (value, true))
    });
    let max = range.max.as_ref().and_then(|(literal, inclusive)| {
        transform_bound_literal(transform, source_type, literal, false, *inclusive)
            .map(|value| (value, true))
    });

    if min.is_none() && max.is_none() {
        None
    } else {
        Some(RangeConstraint { min, max })
    }
}

fn transform_bound_literal(
    transform: Transform,
    source_type: &Type,
    literal: &PrimitiveLiteral,
    is_lower_bound: bool,
    inclusive: bool,
) -> Option<PrimitiveLiteral> {
    let literal = if inclusive {
        literal.clone()
    } else {
        adjust_exclusive_bound(source_type, literal, is_lower_bound)
            .unwrap_or_else(|| literal.clone())
    };
    transform_primitive_literal(transform, source_type, literal)
}

fn adjust_exclusive_bound(
    source_type: &Type,
    literal: &PrimitiveLiteral,
    is_lower_bound: bool,
) -> Option<PrimitiveLiteral> {
    let primitive_type = source_type.as_primitive_type()?;
    match (primitive_type, literal) {
        (PrimitiveType::Int | PrimitiveType::Date, PrimitiveLiteral::Int(value)) => {
            shift_i32(*value, is_lower_bound).map(PrimitiveLiteral::Int)
        }
        (
            PrimitiveType::Long
            | PrimitiveType::Time
            | PrimitiveType::Timestamp
            | PrimitiveType::Timestamptz
            | PrimitiveType::TimestampNs
            | PrimitiveType::TimestamptzNs,
            PrimitiveLiteral::Long(value),
        ) => shift_i64(*value, is_lower_bound).map(PrimitiveLiteral::Long),
        _ => None,
    }
}

fn shift_i32(value: i32, increment: bool) -> Option<i32> {
    if increment {
        value.checked_add(1)
    } else {
        value.checked_sub(1)
    }
}

fn shift_i64(value: i64, increment: bool) -> Option<i64> {
    if increment {
        value.checked_add(1)
    } else {
        value.checked_sub(1)
    }
}

fn partition_predicate_may_match_bounds(
    predicate: &PartitionPredicate,
    lower: Option<&PrimitiveLiteral>,
    upper: Option<&PrimitiveLiteral>,
) -> bool {
    match &predicate.constraint {
        PartitionConstraint::Eq(value) => literal_may_match_bounds(value, lower, upper),
        PartitionConstraint::In(values) => values
            .iter()
            .any(|value| literal_may_match_bounds(value, lower, upper)),
        PartitionConstraint::Range(range) => range_may_match_bounds(range, lower, upper),
    }
}

fn partition_predicate_may_match_value(
    predicate: &PartitionPredicate,
    value: Option<&Literal>,
) -> bool {
    let Some(Literal::Primitive(value)) = value else {
        return true;
    };
    match &predicate.constraint {
        PartitionConstraint::Eq(expected) => value == expected,
        PartitionConstraint::In(values) => values.contains(value),
        PartitionConstraint::Range(range) => literal_may_match_range(value, range),
    }
}

fn literal_may_match_bounds(
    value: &PrimitiveLiteral,
    lower: Option<&PrimitiveLiteral>,
    upper: Option<&PrimitiveLiteral>,
) -> bool {
    if let Some(lower) = lower {
        if value < lower {
            return false;
        }
    }
    if let Some(upper) = upper {
        if value > upper {
            return false;
        }
    }
    true
}

fn range_may_match_bounds(
    range: &RangeConstraint,
    lower: Option<&PrimitiveLiteral>,
    upper: Option<&PrimitiveLiteral>,
) -> bool {
    if let (Some((min, inclusive)), Some(upper)) = (&range.min, upper) {
        if min > upper || (min == upper && !inclusive) {
            return false;
        }
    }
    if let (Some((max, inclusive)), Some(lower)) = (&range.max, lower) {
        if max < lower || (max == lower && !inclusive) {
            return false;
        }
    }
    true
}

fn literal_may_match_range(value: &PrimitiveLiteral, range: &RangeConstraint) -> bool {
    if let Some((min, inclusive)) = &range.min {
        if value < min || (value == min && !inclusive) {
            return false;
        }
    }
    if let Some((max, inclusive)) = &range.max {
        if value > max || (value == max && !inclusive) {
            return false;
        }
    }
    true
}

fn collect_source_range_filters(
    schema: &Schema,
    filters: &[Expr],
) -> HashMap<i32, RangeConstraint> {
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
                if cand.0 > *v || (cand.0 == *v && !cand.1 && *incl) {
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
                if cand.0 < *v || (cand.0 == *v && !cand.1 && *incl) {
                    *v = cand.0;
                    *incl = cand.1;
                }
            }
        }
    }

    let mut result: HashMap<i32, RangeConstraint> = HashMap::new();

    fn add_min(
        acc: &mut HashMap<i32, RangeConstraint>,
        schema: &Schema,
        column_name: &str,
        literal: &datafusion::common::scalar::ScalarValue,
        inclusive: bool,
    ) {
        if let Some(field) = schema.field_by_name(column_name) {
            if let Ok(pl) = scalar_to_primitive_literal(literal) {
                let entry = acc.entry(field.id).or_default();
                tighten_min(&mut entry.min, (pl, inclusive));
            }
        }
    }

    fn add_max(
        acc: &mut HashMap<i32, RangeConstraint>,
        schema: &Schema,
        column_name: &str,
        literal: &datafusion::common::scalar::ScalarValue,
        inclusive: bool,
    ) {
        if let Some(field) = schema.field_by_name(column_name) {
            if let Ok(pl) = scalar_to_primitive_literal(literal) {
                let entry = acc.entry(field.id).or_default();
                tighten_max(&mut entry.max, (pl, inclusive));
            }
        }
    }

    fn visit_expr(acc: &mut HashMap<i32, RangeConstraint>, schema: &Schema, e: &Expr) {
        if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = e {
            let l = strip(left);
            let r = strip(right);
            match op {
                Operator::Gt | Operator::GtEq => {
                    if let (Expr::Column(c), Expr::Literal(sv, _)) = (l, r) {
                        add_min(acc, schema, &c.name, sv, *op == Operator::GtEq);
                    }
                    if let (Expr::Literal(sv, _), Expr::Column(c)) = (l, r) {
                        add_max(acc, schema, &c.name, sv, *op == Operator::GtEq);
                    }
                }
                Operator::Lt | Operator::LtEq => {
                    if let (Expr::Column(c), Expr::Literal(sv, _)) = (l, r) {
                        add_max(acc, schema, &c.name, sv, *op == Operator::LtEq);
                    }
                    if let (Expr::Literal(sv, _), Expr::Column(c)) = (l, r) {
                        add_min(acc, schema, &c.name, sv, *op == Operator::LtEq);
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
