use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, BooleanArray, UInt64Array};
use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::catalog::Session;
use datafusion::common::pruning::PruningStatistics;
use datafusion::common::{Column, Result, ToDFSchema};
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::Expr;
use datafusion::physical_optimizer::pruning::PruningPredicate;

use crate::spec::types::values::{Datum, Literal};
use crate::spec::{DataFile, Manifest, ManifestContentType, ManifestList, Schema};

pub(crate) fn literal_to_scalar_value_local(
    literal: &Literal,
) -> datafusion::common::scalar::ScalarValue {
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
        literal_to_scalar_value_local(&Literal::Primitive(datum.literal.clone()))
    }
}

impl PruningStatistics for IcebergPruningStats {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
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
        // TODO: Partition-aware contained pruning
        None
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
    _schema: &Schema,
    _filters: &[Expr],
) -> Vec<&'a crate::spec::manifest_list::ManifestFile> {
    // TODO: Evaluate filters against `ManifestFile.partitions` FieldSummary to drop manifests early
    manifest_list
        .entries()
        .iter()
        .filter(|mf| mf.content == ManifestContentType::Data)
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
