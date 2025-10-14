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
use crate::spec::types::values::{Datum, Literal};
use crate::spec::types::{PrimitiveType, Type};
use crate::spec::{DataFile, Manifest, ManifestContentType, ManifestList, Schema};
// TODO: Consider parsing logical expressions more robustly for summary pruning

pub(crate) fn literal_to_scalar_value_local(
    literal: &Literal,
) -> datafusion::common::scalar::ScalarValue {
    // TODO: Extend conversion to cover Decimal/UUID/Fixed with precise semantics and timezones
    match literal {
        Literal::Primitive(p) => match p {
            crate::spec::types::values::PrimitiveLiteral::Boolean(v) => {
                datafusion::common::scalar::ScalarValue::Boolean(Some(*v))
            }
            crate::spec::types::values::PrimitiveLiteral::Int(v) => {
                datafusion::common::scalar::ScalarValue::Int32(Some(*v))
            }
            crate::spec::types::values::PrimitiveLiteral::Long(v) => {
                datafusion::common::scalar::ScalarValue::Int64(Some(*v))
            }
            crate::spec::types::values::PrimitiveLiteral::Float(v) => {
                datafusion::common::scalar::ScalarValue::Float32(Some(v.into_inner()))
            }
            crate::spec::types::values::PrimitiveLiteral::Double(v) => {
                datafusion::common::scalar::ScalarValue::Float64(Some(v.into_inner()))
            }
            crate::spec::types::values::PrimitiveLiteral::String(v) => {
                datafusion::common::scalar::ScalarValue::Utf8(Some(v.clone()))
            }
            crate::spec::types::values::PrimitiveLiteral::Binary(v) => {
                datafusion::common::scalar::ScalarValue::Binary(Some(v.clone()))
            }
            crate::spec::types::values::PrimitiveLiteral::Int128(v) => {
                datafusion::common::scalar::ScalarValue::Decimal128(Some(*v), 38, 0)
            }
            crate::spec::types::values::PrimitiveLiteral::UInt128(v) => {
                if *v <= i128::MAX as u128 {
                    datafusion::common::scalar::ScalarValue::Decimal128(Some(*v as i128), 38, 0)
                } else {
                    datafusion::common::scalar::ScalarValue::Utf8(Some(v.to_string()))
                }
            }
        },
        Literal::Struct(fields) => {
            let json_repr = serde_json::to_string(fields).unwrap_or_default();
            datafusion::common::scalar::ScalarValue::Utf8(Some(json_repr))
        }
        Literal::List(items) => {
            let json_repr = serde_json::to_string(items).unwrap_or_default();
            datafusion::common::scalar::ScalarValue::Utf8(Some(json_repr))
        }
        Literal::Map(pairs) => {
            let json_repr = serde_json::to_string(pairs).unwrap_or_default();
            datafusion::common::scalar::ScalarValue::Utf8(Some(json_repr))
        }
    }
}

/// Pruning statistics over Iceberg DataFiles
pub struct IcebergPruningStats {
    files: Vec<DataFile>,
    #[allow(unused)]
    arrow_schema: Arc<ArrowSchema>,
    /// Arrow field name -> Iceberg field id
    name_to_field_id: HashMap<String, i32>,
}

impl IcebergPruningStats {
    pub fn new(
        files: Vec<DataFile>,
        arrow_schema: Arc<ArrowSchema>,
        iceberg_schema: &Schema,
    ) -> Self {
        let mut name_to_field_id = HashMap::new();
        for f in iceberg_schema.fields().iter() {
            name_to_field_id.insert(f.name.clone(), f.id);
        }
        Self {
            files,
            arrow_schema,
            name_to_field_id,
        }
    }

    fn field_id_for(&self, column: &Column) -> Option<i32> {
        self.name_to_field_id.get(&column.name).copied()
    }

    fn datum_to_scalar(&self, datum: &Datum) -> datafusion::common::scalar::ScalarValue {
        // Reuse existing literal conversion via Datum.literal
        // TODO: Avoid allocations by caching conversions per field id
        literal_to_scalar_value_local(&Literal::Primitive(datum.literal.clone()))
    }
}

impl PruningStatistics for IcebergPruningStats {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        // TODO: Materialize arrays only for columns referenced by the predicate
        let field_id = self.field_id_for(column)?;
        let scalars = self.files.iter().map(|f| {
            f.lower_bounds()
                .get(&field_id)
                .map(|d| self.datum_to_scalar(d))
        });
        // Build an Arrow array from Option<ScalarValue>
        let values =
            scalars.map(|opt| opt.unwrap_or(datafusion::common::scalar::ScalarValue::Null));
        datafusion::common::scalar::ScalarValue::iter_to_array(values).ok()
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        let field_id = self.field_id_for(column)?;
        let scalars = self.files.iter().map(|f| {
            f.upper_bounds()
                .get(&field_id)
                .map(|d| self.datum_to_scalar(d))
        });
        let values =
            scalars.map(|opt| opt.unwrap_or(datafusion::common::scalar::ScalarValue::Null));
        datafusion::common::scalar::ScalarValue::iter_to_array(values).ok()
    }

    fn num_containers(&self) -> usize {
        self.files.len()
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        let field_id = self.field_id_for(column)?;
        let counts: Vec<u64> = self
            .files
            .iter()
            .map(|f| f.null_value_counts().get(&field_id).copied().unwrap_or(0))
            .collect();
        Some(Arc::new(UInt64Array::from(counts)) as ArrayRef)
    }

    fn row_counts(&self, _column: &Column) -> Option<ArrayRef> {
        let rows: Vec<u64> = self.files.iter().map(|f| f.record_count()).collect();
        Some(Arc::new(UInt64Array::from(rows)) as ArrayRef)
    }

    fn contained(
        &self,
        _column: &Column,
        _value: &std::collections::HashSet<datafusion::common::scalar::ScalarValue>,
    ) -> Option<BooleanArray> {
        // Basic contained() for equality/IN pruning using lower/upper bounds equality for strings and integers
        // When both bounds are equal to the value, we can mark "contained" true; otherwise, unknown
        let field_id = self.field_id_for(_column)?;
        let mut result = Vec::with_capacity(self.files.len());
        for f in &self.files {
            let lower = f.lower_bounds().get(&field_id);
            let upper = f.upper_bounds().get(&field_id);
            if let (Some(lb), Some(ub)) = (lower, upper) {
                let lb_sv = self.datum_to_scalar(lb);
                let ub_sv = self.datum_to_scalar(ub);
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
    // TODO: Support non-identity transforms (day/month/hour/bucket/truncate)
    let eq_filters = collect_identity_eq_filters(table_schema, filters);
    let in_filters = collect_identity_in_filters(table_schema, filters);
    let range_filters = collect_identity_range_filters(table_schema, filters);
    manifest_list
        .entries()
        .iter()
        .filter(|mf| mf.content == ManifestContentType::Data)
        .filter(|mf| {
            // If no simple identity eq filters, keep manifest
            if eq_filters.is_empty() {
                return true;
            }
            let Some(spec) = partition_specs.get(&mf.partition_spec_id) else {
                return true;
            };
            let Some(part_summaries) = mf.partitions.as_ref() else {
                return true;
            };
            // Build partition field result types
            let part_type = match spec.partition_type(table_schema) {
                Ok(t) => t,
                Err(_) => return true,
            };
            // Evaluate equality filters; if any contradicts summaries, drop manifest
            for (source_id, lit) in &eq_filters {
                // find partition field with identity transform sourcing this column
                if let Some((idx, _pf)) = spec.fields().iter().enumerate().find(|(_, pf)| {
                    pf.source_id == *source_id
                        && matches!(pf.transform, crate::spec::transform::Transform::Identity)
                }) {
                    if let Some(summary) = part_summaries.get(idx) {
                        // decode bounds according to partition field type
                        let field_ty = part_type.fields().get(idx).map(|nf| nf.field_type.as_ref());
                        if let Some(Type::Primitive(prim_ty)) = field_ty {
                            // TODO: Handle contains_null/contains_nan from FieldSummary
                            let lower = summary
                                .lower_bound_bytes
                                .as_ref()
                                .and_then(|b| decode_bound_bytes(prim_ty, b).ok());
                            let upper = summary
                                .upper_bound_bytes
                                .as_ref()
                                .and_then(|b| decode_bound_bytes(prim_ty, b).ok());

                            if let (Some(lb), Some(ub)) = (lower.as_ref(), upper.as_ref()) {
                                // if lit < lb or lit > ub, cannot match
                                if lt_prim(lit, lb) || gt_prim(lit, ub) {
                                    return false;
                                }
                            } else if let Some(lb) = lower.as_ref() {
                                // if we have only a lower bound, drop manifest if lit < lb
                                if lt_prim(lit, lb) {
                                    return false;
                                }
                            } else if let Some(ub) = upper.as_ref() {
                                // if we have only an upper bound, drop manifest if lit > ub
                                if gt_prim(lit, ub) {
                                    return false;
                                }
                            }
                        }
                    }
                }
            }

            // Evaluate IN-list filters: require intersection with [lb, ub]
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
                                .and_then(|b| decode_bound_bytes(prim_ty, b).ok());
                            let upper = summary
                                .upper_bound_bytes
                                .as_ref()
                                .and_then(|b| decode_bound_bytes(prim_ty, b).ok());
                            if let (Some(lb), Some(ub)) = (lower.as_ref(), upper.as_ref()) {
                                let mut any_in = false;
                                for lit in lits {
                                    if !(lt_prim(lit, lb) || gt_prim(lit, ub)) {
                                        any_in = true;
                                        break;
                                    }
                                }
                                if !any_in {
                                    return false;
                                }
                            } else if let Some(lb) = lower.as_ref() {
                                // with only lower bound, require any value >= lb
                                let any_in = lits.iter().any(|v| !lt_prim(v, lb));
                                if !any_in {
                                    return false;
                                }
                            } else if let Some(ub) = upper.as_ref() {
                                // with only upper bound, require any value <= ub
                                let any_in = lits.iter().any(|v| !gt_prim(v, ub));
                                if !any_in {
                                    return false;
                                }
                            }
                        }
                    }
                }
            }

            // Evaluate simple range filters: require overlap with [lb, ub]
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
                                .and_then(|b| decode_bound_bytes(prim_ty, b).ok());
                            let upper = summary
                                .upper_bound_bytes
                                .as_ref()
                                .and_then(|b| decode_bound_bytes(prim_ty, b).ok());
                            if let (Some(lb), Some(ub)) = (lower.as_ref(), upper.as_ref()) {
                                // manifest range [lb, ub]
                                // query range [min, max]
                                if let Some((ref qmin, _incl_min)) = range.min {
                                    if gt_prim(qmin, ub) {
                                        return false;
                                    }
                                }
                                if let Some((ref qmax, _incl_max)) = range.max {
                                    if lt_prim(qmax, lb) {
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
    // TODO: Partition-transform awareness and metrics-only prune at manifest entry granularity
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

fn collect_identity_eq_filters(
    schema: &Schema,
    filters: &[Expr],
) -> Vec<(i32, crate::spec::types::values::PrimitiveLiteral)> {
    // returns Vec of (source_id, literal) for Exprs of form col = literal
    fn strip(expr: &Expr) -> &Expr {
        match expr {
            Expr::Cast(c) => strip(&c.expr),
            Expr::Alias(a) => strip(&a.expr),
            _ => expr,
        }
    }

    let mut result = Vec::new();

    fn visit_expr(
        acc: &mut Vec<(i32, crate::spec::types::values::PrimitiveLiteral)>,
        schema: &Schema,
        e: &Expr,
    ) {
        match e {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) if *op == Operator::Eq => {
                let l = strip(left);
                let r = strip(right);

                // col = lit
                if let Expr::Column(c) = l {
                    if let Expr::Literal(sv, _) = r {
                        let col_name = c.name.clone();
                        if let Some(field) = schema.field_by_name(&col_name) {
                            if let Some(pl) = scalar_to_primitive_literal(sv) {
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
                            if let Some(pl) = scalar_to_primitive_literal(sv) {
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
) -> std::collections::HashMap<i32, Vec<crate::spec::types::values::PrimitiveLiteral>> {
    fn strip(expr: &Expr) -> &Expr {
        match expr {
            Expr::Cast(c) => strip(&c.expr),
            Expr::Alias(a) => strip(&a.expr),
            _ => expr,
        }
    }

    let mut result: std::collections::HashMap<_, Vec<_>> = std::collections::HashMap::new();

    fn visit_expr(
        acc: &mut std::collections::HashMap<i32, Vec<crate::spec::types::values::PrimitiveLiteral>>,
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
                                if let Some(pl) = scalar_to_primitive_literal(sv) {
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
    min: Option<(crate::spec::types::values::PrimitiveLiteral, bool)>,
    max: Option<(crate::spec::types::values::PrimitiveLiteral, bool)>,
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

    fn tighten_min(
        cur: &mut Option<(crate::spec::types::values::PrimitiveLiteral, bool)>,
        cand: (crate::spec::types::values::PrimitiveLiteral, bool),
    ) {
        match cur {
            None => *cur = Some(cand),
            Some((ref mut v, ref mut incl)) => {
                if gt_prim(&cand.0, v) || (eq_prim(&cand.0, v) && cand.1 && !*incl) {
                    *v = cand.0;
                    *incl = cand.1;
                }
            }
        }
    }
    fn tighten_max(
        cur: &mut Option<(crate::spec::types::values::PrimitiveLiteral, bool)>,
        cand: (crate::spec::types::values::PrimitiveLiteral, bool),
    ) {
        match cur {
            None => *cur = Some(cand),
            Some((ref mut v, ref mut incl)) => {
                if lt_prim(&cand.0, v) || (eq_prim(&cand.0, v) && cand.1 && !*incl) {
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
                            if let Some(pl) = scalar_to_primitive_literal(sv) {
                                let entry = acc.entry(field.id).or_default();
                                tighten_min(&mut entry.min, (pl, *op == Operator::GtEq));
                            }
                        }
                    }
                }
                Operator::Lt | Operator::LtEq => {
                    if let (Expr::Column(c), Expr::Literal(sv, _)) = (l, r) {
                        if let Some(field) = schema.field_by_name(&c.name) {
                            if let Some(pl) = scalar_to_primitive_literal(sv) {
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

fn scalar_to_primitive_literal(
    sv: &datafusion::common::scalar::ScalarValue,
) -> Option<crate::spec::types::values::PrimitiveLiteral> {
    use crate::spec::types::values::PrimitiveLiteral::*;
    match sv {
        datafusion::common::scalar::ScalarValue::Boolean(Some(v)) => Some(Boolean(*v)),
        datafusion::common::scalar::ScalarValue::Int32(Some(v)) => Some(Int(*v)),
        datafusion::common::scalar::ScalarValue::Int64(Some(v)) => Some(Long(*v)),
        datafusion::common::scalar::ScalarValue::Float32(Some(v)) => {
            Some(Float(ordered_float::OrderedFloat(*v)))
        }
        datafusion::common::scalar::ScalarValue::Float64(Some(v)) => {
            Some(Double(ordered_float::OrderedFloat(*v)))
        }
        datafusion::common::scalar::ScalarValue::Utf8(Some(s)) => Some(String(s.clone())),
        datafusion::common::scalar::ScalarValue::LargeUtf8(Some(s)) => Some(String(s.clone())),
        datafusion::common::scalar::ScalarValue::Binary(Some(b)) => Some(Binary(b.clone())),
        _ => None,
    }
}

fn decode_bound_bytes(
    ty: &PrimitiveType,
    bytes: &[u8],
) -> Result<crate::spec::types::values::PrimitiveLiteral, String> {
    use crate::spec::types::values::PrimitiveLiteral as PL;
    let pl = match ty {
        PrimitiveType::Boolean => {
            let val = !(bytes.len() == 1 && bytes[0] == 0u8);
            PL::Boolean(val)
        }
        PrimitiveType::Int | PrimitiveType::Date => {
            let val = i32::from_le_bytes(bytes.try_into().map_err(|_| "Invalid i32 bytes")?);
            PL::Int(val)
        }
        PrimitiveType::Long
        | PrimitiveType::Time
        | PrimitiveType::Timestamp
        | PrimitiveType::Timestamptz
        | PrimitiveType::TimestampNs
        | PrimitiveType::TimestamptzNs => {
            let val = if bytes.len() == 4 {
                i32::from_le_bytes(bytes.try_into().map_err(|_| "Invalid i32 bytes")?) as i64
            } else {
                i64::from_le_bytes(bytes.try_into().map_err(|_| "Invalid i64 bytes")?)
            };
            PL::Long(val)
        }
        PrimitiveType::Float => {
            let val = f32::from_le_bytes(bytes.try_into().map_err(|_| "Invalid f32 bytes")?);
            PL::Float(ordered_float::OrderedFloat(val))
        }
        PrimitiveType::Double => {
            let val = if bytes.len() == 4 {
                f32::from_le_bytes(bytes.try_into().map_err(|_| "Invalid f32 bytes")?) as f64
            } else {
                f64::from_le_bytes(bytes.try_into().map_err(|_| "Invalid f64 bytes")?)
            };
            PL::Double(ordered_float::OrderedFloat(val))
        }
        PrimitiveType::String => {
            let val = std::str::from_utf8(bytes)
                .map_err(|_| "Invalid UTF-8")?
                .to_string();
            PL::String(val)
        }
        PrimitiveType::Uuid => {
            return Err("uuid bound decoding not supported".to_string());
        }
        PrimitiveType::Fixed(_) | PrimitiveType::Binary => {
            // Treat bounds as raw bytes for conservative comparisons (equality filters only)
            PL::Binary(bytes.to_vec())
        }
        PrimitiveType::Decimal { .. } => {
            return Err("decimal bound decoding not supported".to_string());
        }
    };
    Ok(pl)
}

fn lt_prim(
    a: &crate::spec::types::values::PrimitiveLiteral,
    b: &crate::spec::types::values::PrimitiveLiteral,
) -> bool {
    use crate::spec::types::values::PrimitiveLiteral as PL;
    match (a, b) {
        (PL::Int(x), PL::Int(y)) => x < y,
        (PL::Long(x), PL::Long(y)) => x < y,
        (PL::Float(x), PL::Float(y)) => x < y,
        (PL::Double(x), PL::Double(y)) => x < y,
        (PL::String(x), PL::String(y)) => x < y,
        _ => false,
    }
}

fn gt_prim(
    a: &crate::spec::types::values::PrimitiveLiteral,
    b: &crate::spec::types::values::PrimitiveLiteral,
) -> bool {
    use crate::spec::types::values::PrimitiveLiteral as PL;
    match (a, b) {
        (PL::Int(x), PL::Int(y)) => x > y,
        (PL::Long(x), PL::Long(y)) => x > y,
        (PL::Float(x), PL::Float(y)) => x > y,
        (PL::Double(x), PL::Double(y)) => x > y,
        (PL::String(x), PL::String(y)) => x > y,
        _ => false,
    }
}

fn eq_prim(
    a: &crate::spec::types::values::PrimitiveLiteral,
    b: &crate::spec::types::values::PrimitiveLiteral,
) -> bool {
    use crate::spec::types::values::PrimitiveLiteral as PL;
    match (a, b) {
        (PL::Int(x), PL::Int(y)) => x == y,
        (PL::Long(x), PL::Long(y)) => x == y,
        (PL::Float(x), PL::Float(y)) => x == y,
        (PL::Double(x), PL::Double(y)) => x == y,
        (PL::String(x), PL::String(y)) => x == y,
        (PL::Binary(x), PL::Binary(y)) => x == y,
        (PL::Boolean(x), PL::Boolean(y)) => x == y,
        _ => false,
    }
}
