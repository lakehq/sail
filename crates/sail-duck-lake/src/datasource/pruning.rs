use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, BooleanArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Schema as ArrowSchema};
use datafusion::catalog::Session;
use datafusion::common::pruning::PruningStatistics;
use datafusion::common::scalar::ScalarValue;
use datafusion::common::{Column, Result as DataFusionResult, ToDFSchema};
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::Expr;
use datafusion::physical_optimizer::pruning::PruningPredicate;

use crate::spec::{ColumnInfo, ColumnStatsInfo, FieldIndex, FileInfo};

struct DuckLakePruningStats {
    files: Vec<FileInfo>,
    arrow_schema: Arc<ArrowSchema>,
    name_to_field_id: HashMap<String, FieldIndex>,
    field_id_to_datatype: HashMap<FieldIndex, ArrowDataType>,
    min_cache: RefCell<HashMap<FieldIndex, ArrayRef>>,
    max_cache: RefCell<HashMap<FieldIndex, ArrayRef>>,
    nulls_cache: RefCell<HashMap<FieldIndex, ArrayRef>>,
    rows_cache: RefCell<Option<ArrayRef>>,
}

impl DuckLakePruningStats {
    fn new(files: Vec<FileInfo>, arrow_schema: Arc<ArrowSchema>, columns: &[ColumnInfo]) -> Self {
        let mut name_to_field_id: HashMap<String, FieldIndex> = HashMap::new();
        let mut field_id_to_datatype: HashMap<FieldIndex, ArrowDataType> = HashMap::new();
        for c in columns {
            name_to_field_id.insert(c.column_name.clone(), c.column_id);
        }
        for f in arrow_schema.fields() {
            if let Some(fid) = name_to_field_id.get(f.name()) {
                field_id_to_datatype.insert(*fid, f.data_type().clone());
            }
        }
        Self {
            files,
            arrow_schema,
            name_to_field_id,
            field_id_to_datatype,
            min_cache: RefCell::new(HashMap::new()),
            max_cache: RefCell::new(HashMap::new()),
            nulls_cache: RefCell::new(HashMap::new()),
            rows_cache: RefCell::new(None),
        }
    }

    fn field_id_for(&self, column: &Column) -> Option<FieldIndex> {
        self.name_to_field_id.get(&column.name).copied()
    }

    fn field_type_for(&self, field_id: &FieldIndex) -> Option<ArrowDataType> {
        self.field_id_to_datatype.get(field_id).cloned()
    }

    fn parse_stat_value(&self, field_id: &FieldIndex, value: &str) -> Option<ScalarValue> {
        let dt = self.field_type_for(field_id)?;
        ScalarValue::try_from_string(value.to_string(), &dt).ok()
    }

    fn get_column_stats<'a>(
        file: &'a FileInfo,
        field_id: &FieldIndex,
    ) -> Option<&'a ColumnStatsInfo> {
        file.column_stats.iter().find(|s| s.column_id == *field_id)
    }
}

impl PruningStatistics for DuckLakePruningStats {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        let field_id = self.field_id_for(column)?;
        if let Some(arr) = self.min_cache.borrow().get(&field_id) {
            return Some(arr.clone());
        }
        let values = self.files.iter().map(|f| {
            let v = Self::get_column_stats(f, &field_id)
                .and_then(|s| s.min_value.as_deref())
                .and_then(|s| self.parse_stat_value(&field_id, s))
                .unwrap_or(ScalarValue::Null);
            v
        });
        let arr = ScalarValue::iter_to_array(values).ok()?;
        self.min_cache.borrow_mut().insert(field_id, arr.clone());
        Some(arr)
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        let field_id = self.field_id_for(column)?;
        if let Some(arr) = self.max_cache.borrow().get(&field_id) {
            return Some(arr.clone());
        }
        let values = self.files.iter().map(|f| {
            let v = Self::get_column_stats(f, &field_id)
                .and_then(|s| s.max_value.as_deref())
                .and_then(|s| self.parse_stat_value(&field_id, s))
                .unwrap_or(ScalarValue::Null);
            v
        });
        let arr = ScalarValue::iter_to_array(values).ok()?;
        self.max_cache.borrow_mut().insert(field_id, arr.clone());
        Some(arr)
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        let field_id = self.field_id_for(column)?;
        if let Some(arr) = self.nulls_cache.borrow().get(&field_id) {
            return Some(arr.clone());
        }
        let counts: Vec<u64> = self
            .files
            .iter()
            .map(|f| {
                Self::get_column_stats(f, &field_id)
                    .and_then(|s| s.null_count)
                    .unwrap_or(0)
            })
            .collect();
        let arr: ArrayRef = Arc::new(UInt64Array::from(counts));
        self.nulls_cache.borrow_mut().insert(field_id, arr.clone());
        Some(arr)
    }

    fn row_counts(&self, _column: &Column) -> Option<ArrayRef> {
        if let Some(arr) = self.rows_cache.borrow().as_ref() {
            return Some(arr.clone());
        }
        let rows: Vec<u64> = self.files.iter().map(|f| f.record_count).collect();
        let arr: ArrayRef = Arc::new(UInt64Array::from(rows));
        *self.rows_cache.borrow_mut() = Some(arr.clone());
        Some(arr)
    }

    fn contained(
        &self,
        _column: &Column,
        _value: &std::collections::HashSet<ScalarValue>,
    ) -> Option<BooleanArray> {
        None
    }

    fn num_containers(&self) -> usize {
        self.files.len()
    }
}

pub fn prune_files(
    session: &dyn Session,
    filters: &[Expr],
    limit: Option<usize>,
    logical_schema: Arc<ArrowSchema>,
    files: Vec<FileInfo>,
    columns: &[ColumnInfo],
) -> DataFusionResult<(Vec<FileInfo>, Option<Vec<bool>>)> {
    let filter_expr = conjunction(filters.iter().cloned());
    if filter_expr.is_none() && limit.is_none() {
        return Ok((files, None));
    }

    let stats = DuckLakePruningStats::new(files, logical_schema.clone(), columns);
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
                    rows_collected = rows_collected.saturating_add(file.record_count);
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
