// SPDX-License-Identifier: Apache-2.0

//! The tables of a temporary view.
//!
//! A temporary view stores the plan that its definition resolved to. A scan of a catalog
//! table in the stored plan does not keep the table source that the definition resolved
//! to, which is one version of the table. It keeps a [`ViewTableReference`]: what the
//! scan reads ([`TableOrigin`]) and the columns that it read when the view was created.
//! Each read of the view obtains the source of each reference as a direct read of the
//! table does, with the catalog lookups and the sources of the query that reads the view,
//! so that the view, the other references to the table, and the target of a write read
//! one version of the table. Only the columns that the stored plan reads must be as they
//! were when the view was created.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::Hash;
use std::sync::Arc;

use arrow_schema::extension::{EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY};
use datafusion::arrow::compute::can_cast_types;
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use datafusion::datasource::source_as_provider;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, ScalarValue};
use datafusion_expr::expr::Cast;
use datafusion_expr::{
    Distinct, Expr, LogicalPlan, Projection, TableScan, TableScanBuilder, TableSource, lit, when,
};
use datafusion_functions::core::getfield::GetFieldFunc;
use datafusion_functions::core::{get_field, named_struct};
use sail_catalog::error::CatalogError;
use sail_catalog::manager::CatalogManager;
use sail_catalog::manager::table::SessionCatalogTable;
use sail_catalog::utils::quote_name_if_needed;
use sail_common::spec::{
    SAIL_SPARK_INTERVAL_METADATA_KEY, SAIL_SPARK_UDT_METADATA_KEY, SPARK_METADATA_JSON_KEY,
};
use sail_common_datafusion::catalog::{TableKind, TableStatus};
use sail_common_datafusion::datasource::DataSourceRegistry;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::lakerelation::LakeRelationResolution;
use sail_common_datafusion::rename::table_provider::RenameTableProvider;
use sail_common_datafusion::session::plan::PlanService;
use sail_common_datafusion::variant::VARIANT_METADATA_MARKER_KEY;

use super::read::{CatalogTableRead, rename_source};
use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::state::{PlanResolverState, ViewDefinitionReads, table_source_address};
use crate::shared_cte::{
    CteReference, recompute_plan_schemas, renew_cte_definitions_with_scans, visit_nodes,
    visit_scans,
};

/// What the source of a table scan reads, for the sources that a temporary view obtains
/// again at each read.
#[derive(Debug)]
pub(in crate::resolver) enum TableOrigin {
    CatalogTable(CatalogTableOrigin),
    LakePath(LakePathOrigin),
}

/// A read of a Delta Lake or Iceberg table by its path.
#[derive(Debug)]
pub(in crate::resolver) struct LakePathOrigin {
    pub format: String,
    pub schema: Option<Schema>,
    pub options: Vec<(String, String)>,
    pub paths: Vec<String>,
    pub read_case_sensitive: bool,
}

/// A read of a catalog table, as the query wrote it.
#[derive(Debug)]
pub(in crate::resolver) struct CatalogTableOrigin {
    /// The fully qualified name of the table: the catalog, the database, and the table.
    pub name: Vec<String>,
    /// The table in the catalog instance of the session, if each session has its own
    /// instance of the catalog.
    pub session_table: Option<SessionCatalogTable>,
    /// The name of the relation of the table that is read, such as a metadata table.
    pub lake_relation: Option<String>,
    /// The format of the table, which the time travel options are written for.
    pub format: String,
    pub options: Vec<(String, String)>,
    pub temporal_options: Vec<(String, String)>,
    pub read_case_sensitive: bool,
}

/// The source of a table scan in the stored plan of a temporary view. It is not a table
/// provider, so a stored plan cannot be executed before each reference is bound to the
/// source of a read (see [`PlanResolver::resolve_stored_view_plan`]).
#[derive(Debug)]
struct ViewTableReference {
    /// The columns that the scan read when the view was created, as the scan names them.
    schema: SchemaRef,
    /// All the columns of the table when the view was created.
    table: SchemaRef,
    /// The position in the table of each column of the schema.
    columns: Vec<usize>,
    /// How the stored plan reads each column of the table, if it reads the column.
    required: Vec<Option<ColumnUse>>,
    origin: Arc<TableOrigin>,
}

impl TableSource for ViewTableReference {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl ViewTableReference {
    fn try_from_scan(scan: &TableScan) -> Option<&Self> {
        scan.source.downcast_ref::<Self>()
    }

    /// Returns the scan with a reference in the place of its source. `required` tells how
    /// the stored plan reads each column of the source by its position, and every column
    /// of the scan is read whole without it.
    fn store(
        scan: TableScan,
        origin: &Arc<TableOrigin>,
        required: Option<&HashMap<usize, ColumnUse>>,
    ) -> PlanResult<TableScan> {
        let source = match scan.source.downcast_ref::<ViewDefinitionSource>() {
            Some(source) => &source.inner,
            None => &scan.source,
        };
        let schema = source.schema();
        // A source with duplicate column names is read under other names.
        let table_schema = source_as_provider(source)
            .ok()
            .and_then(|provider| {
                provider
                    .downcast_ref::<RenameTableProvider>()
                    .map(|rename| rename.inner().schema())
            })
            .unwrap_or_else(|| Arc::clone(&schema));
        let (schema, columns) = match &scan.projection {
            Some(projection) => (Arc::new(schema.project(projection)?), projection.clone()),
            None => (schema, (0..table_schema.fields().len()).collect()),
        };
        let mut uses = vec![None; table_schema.fields().len()];
        for &i in &columns {
            uses[i] = match required {
                Some(required) => required.get(&i).cloned(),
                None => Some(ColumnUse::Whole),
            };
        }
        let reference = Arc::new(Self {
            schema,
            table: table_schema,
            columns,
            required: uses,
            origin: Arc::clone(origin),
        });
        // The stored scan has the nullability of the table when the view was created.
        Ok(TableScanBuilder::new(scan.table_name, reference)
            .with_filters(scan.filters)
            .with_fetch(scan.fetch)
            .build()?)
    }

    /// Returns the position in the table of each column that the scan read when the view
    /// was created, or how the columns that the stored plan reads changed. A column that
    /// the plan reads keeps its type (Spark's `SchemaUtils.validateSchemaCompatibility`),
    /// or only the fields that it reads for a struct, or only exists for a column that
    /// only tells rows apart. A column that the plan does not read has a position only if
    /// it did not change.
    fn match_columns(
        &self,
        table: &Schema,
        case_sensitive: bool,
        type_name: &dyn Fn(&DataType) -> String,
    ) -> Result<Vec<Option<usize>>, Vec<String>> {
        let mut changes = vec![];
        let matches = match_fields(self.table.fields(), table.fields(), case_sensitive);
        let mut positions = Vec::with_capacity(matches.len());
        for ((field, found), used) in self.table.fields().iter().zip(matches).zip(&self.required) {
            let path = vec![field.name().clone()];
            let position = match (used, found) {
                (Some(ColumnUse::Identity), FieldMatch::Found(position)) => Some(position),
                (Some(used), found) => field_changes(
                    field,
                    found,
                    table.fields(),
                    &path,
                    used.fields(),
                    case_sensitive,
                    type_name,
                    &mut changes,
                ),
                // A column that the view does not read is read only if it did not change.
                (None, found) => {
                    let count = changes.len();
                    let position = field_changes(
                        field,
                        found,
                        table.fields(),
                        &path,
                        None,
                        case_sensitive,
                        type_name,
                        &mut changes,
                    );
                    let changed = changes.len() > count;
                    changes.truncate(count);
                    position.filter(|_| !changed)
                }
            };
            positions.push(position);
        }
        if !changes.is_empty() {
            return Err(changes);
        }
        Ok(self.columns.iter().map(|&i| positions[i]).collect())
    }

    /// Returns the plan of a scan of the current table with the output of the stored scan:
    /// the names and the types that the view reads, and the metadata that clients see in
    /// its schema. `positions` are those of [`Self::match_columns`], and the scan reads the
    /// columns that have a position, in their order.
    fn with_stored_output(
        &self,
        scan: TableScan,
        positions: &[Option<usize>],
        case_sensitive: bool,
    ) -> PlanResult<LogicalPlan> {
        let source = scan.source.schema();
        let columns = self
            .schema
            .fields()
            .iter()
            .zip(positions)
            .zip(&self.columns)
            .map(|((stored, position), &i)| {
                let current = position.map(|position| source.field(position));
                (stored, current, self.required[i].as_ref())
            })
            .collect::<Vec<_>>();
        let rebuilt = |stored: &Field, current: &Field, used: Option<&ColumnUse>| match used {
            Some(ColumnUse::Fields(_)) => stored.data_type() != current.data_type(),
            // The fields of a struct that is read whole keep the names of the view.
            Some(ColumnUse::Whole) => field_names_changed(stored.data_type(), current.data_type()),
            _ => false,
        };
        let same = columns.iter().all(|(stored, current, used)| {
            current.is_some_and(|current| {
                stored.name() == current.name()
                    && !user_metadata_differs(stored, current)
                    && !encoding_changed(stored.data_type(), current.data_type())
                    && !rebuilt(stored, current, *used)
            })
        });
        if same {
            return Ok(LogicalPlan::TableScan(scan));
        }
        let qualifier = Some(scan.table_name.clone());
        let expressions = columns
            .into_iter()
            .map(|(stored, current, used)| {
                let Some(current) = current else {
                    // A column that the view does not read and that changed is a null of
                    // its stored type. The plans above the scan compute their schemas again.
                    let null = Expr::Literal(ScalarValue::try_new_null(stored.data_type())?, None);
                    let target = stored.as_ref().clone().with_nullable(true);
                    return Ok(
                        Expr::Cast(Cast::new_from_field(Box::new(null), Arc::new(target)))
                            .alias_qualified(qualifier.clone(), stored.name()),
                    );
                };
                let column = Expr::Column(Column::new(qualifier.clone(), current.name().clone()));
                let expression = match used {
                    Some(ColumnUse::Identity) => column,
                    Some(used) if rebuilt(stored, current, Some(used)) => {
                        let value = rebuild_struct(column, stored, current, used, case_sensitive)?;
                        let target = nullable_field(stored).with_nullable(current.is_nullable());
                        Expr::Cast(Cast::new_from_field(Box::new(value), Arc::new(target)))
                    }
                    // A column whose values are stored in another way, such as `LargeUtf8`
                    // instead of `Utf8`, is read in the type of the view.
                    _ if user_metadata_differs(stored, current)
                        || encoding_changed(stored.data_type(), current.data_type()) =>
                    {
                        // The fields of a struct can have nulls now that had none.
                        let data_type = if stored.data_type() == current.data_type() {
                            stored.data_type().clone()
                        } else if can_cast_types(current.data_type(), stored.data_type()) {
                            nullable_field(stored).data_type().clone()
                        } else {
                            current.data_type().clone()
                        };
                        let target = stored
                            .as_ref()
                            .clone()
                            .with_data_type(data_type)
                            .with_nullable(current.is_nullable());
                        Expr::Cast(Cast::new_from_field(Box::new(column), Arc::new(target)))
                    }
                    _ => column,
                };
                Ok(expression.alias_qualified(qualifier.clone(), stored.name()))
            })
            .collect::<PlanResult<Vec<_>>>()?;
        Ok(LogicalPlan::Projection(Projection::try_new(
            expressions,
            Arc::new(LogicalPlan::TableScan(scan)),
        )?))
    }
}

/// The field metadata that is part of the logical type of a field, for types that share an
/// Arrow type with other logical types.
const LOGICAL_TYPE_METADATA_KEYS: [&str; 5] = [
    SAIL_SPARK_INTERVAL_METADATA_KEY,
    SAIL_SPARK_UDT_METADATA_KEY,
    EXTENSION_TYPE_NAME_KEY,
    EXTENSION_TYPE_METADATA_KEY,
    VARIANT_METADATA_MARKER_KEY,
];

/// Returns whether the metadata that clients see in a schema differs, at any level.
fn user_metadata_differs(stored: &Field, current: &Field) -> bool {
    if stored.metadata().get(SPARK_METADATA_JSON_KEY)
        != current.metadata().get(SPARK_METADATA_JSON_KEY)
    {
        return true;
    }
    match (stored.data_type(), current.data_type()) {
        (DataType::Struct(stored), DataType::Struct(current)) => stored.iter().any(|field| {
            current
                .iter()
                .find(|x| same_name(x.name(), field.name(), false))
                .is_some_and(|x| user_metadata_differs(field, x))
        }),
        (DataType::List(stored), DataType::List(current))
        | (DataType::LargeList(stored), DataType::LargeList(current))
        | (DataType::ListView(stored), DataType::ListView(current))
        | (DataType::LargeListView(stored), DataType::LargeListView(current))
        | (DataType::FixedSizeList(stored, _), DataType::FixedSizeList(current, _)) => {
            user_metadata_differs(stored, current)
        }
        (DataType::Map(stored, _), DataType::Map(current, _)) => {
            match (stored.data_type(), current.data_type()) {
                (DataType::Struct(stored), DataType::Struct(current)) => stored
                    .iter()
                    .zip(current.iter())
                    .any(|(a, b)| user_metadata_differs(a, b)),
                _ => false,
            }
        }
        _ => false,
    }
}

/// Adds the change of the logical type of a field whose Arrow type can be the same.
fn logical_type_changes(
    stored: &Field,
    current: &Field,
    path: &[String],
    type_name: &dyn Fn(&DataType) -> String,
    changes: &mut Vec<String>,
) {
    let change = LOGICAL_TYPE_METADATA_KEYS.iter().find_map(|key| {
        let (a, b) = (stored.metadata().get(*key), current.metadata().get(*key));
        let same = match (a, b) {
            (Some(a), Some(b)) => {
                a == b
                    || matches!(
                        (
                            serde_json::from_str::<serde_json::Value>(a),
                            serde_json::from_str::<serde_json::Value>(b),
                        ),
                        (Ok(a), Ok(b)) if a == b
                    )
            }
            (None, None) => true,
            _ => false,
        };
        (!same).then_some((a, b))
    });
    if let Some((a, b)) = change {
        let describe = |field: &Field, value: Option<&String>| match value {
            Some(value) => format!("{} {value}", type_name(field.data_type())),
            None => type_name(field.data_type()),
        };
        changes.push(format!(
            "{} type has changed from {} to {}",
            quote_names(path),
            describe(stored, a),
            describe(current, b)
        ));
    }
}

/// The field that a field of a stored schema is in a current schema.
#[derive(Debug, PartialEq, Eq)]
enum FieldMatch {
    Found(usize),
    Missing,
    /// The name matches these fields, and nothing tells them apart.
    Several(Vec<usize>),
}

/// Matches the fields of a stored schema to those of a current schema. A name that is
/// written the same way comes first, by position among the fields of that name. A name
/// that is not is then matched without letter case, unless the names are case sensitive,
/// among the fields that no other name matched.
fn match_fields(stored: &Fields, current: &Fields, case_sensitive: bool) -> Vec<FieldMatch> {
    let mut exact: HashMap<&str, VecDeque<usize>> = HashMap::new();
    for (i, field) in current.iter().enumerate() {
        exact.entry(field.name()).or_default().push_back(i);
    }
    let mut claimed = vec![false; current.len()];
    let mut matches = stored
        .iter()
        .map(|field| {
            match exact
                .get_mut(field.name().as_str())
                .and_then(VecDeque::pop_front)
            {
                Some(i) => {
                    claimed[i] = true;
                    FieldMatch::Found(i)
                }
                None => FieldMatch::Missing,
            }
        })
        .collect::<Vec<_>>();
    if case_sensitive {
        return matches;
    }
    // Names that differ only in letter case compete for the same fields.
    let mut unmatched: HashMap<String, Vec<usize>> = HashMap::new();
    for (i, field) in stored.iter().enumerate() {
        if matches[i] == FieldMatch::Missing {
            unmatched
                .entry(field.name().to_lowercase())
                .or_default()
                .push(i);
        }
    }
    for (name, fields) in unmatched {
        let candidates = current
            .iter()
            .enumerate()
            .filter(|(i, x)| !claimed[*i] && x.name().to_lowercase() == name)
            .map(|(i, _)| i)
            .collect::<Vec<_>>();
        if candidates.len() > fields.len() {
            for i in fields {
                matches[i] = FieldMatch::Several(candidates.clone());
            }
        } else {
            for (i, candidate) in fields.into_iter().zip(candidates) {
                matches[i] = FieldMatch::Found(candidate);
            }
        }
    }
    matches
}

/// Adds the changes of a stored field to its match in the current fields, and returns the
/// position of the match.
fn field_changes(
    field: &Field,
    found: FieldMatch,
    current: &Fields,
    path: &[String],
    fields: Option<&HashMap<String, ColumnUse>>,
    case_sensitive: bool,
    type_name: &dyn Fn(&DataType) -> String,
    changes: &mut Vec<String>,
) -> Option<usize> {
    match found {
        FieldMatch::Found(position) => {
            let other = &current[position];
            column_changes(
                field.data_type(),
                other.data_type(),
                path,
                fields,
                case_sensitive,
                type_name,
                changes,
            );
            logical_type_changes(field, other, path, type_name, changes);
            Some(position)
        }
        FieldMatch::Missing => {
            changes.push(format!(
                "{} has been removed",
                describe_field(path, field, type_name)
            ));
            None
        }
        FieldMatch::Several(positions) => {
            let names = positions
                .iter()
                .map(|&i| current[i].name().as_str())
                .collect::<Vec<_>>();
            changes.push(format!(
                "{} matches more than one field: [{}]",
                quote_names(path),
                names.join(", ")
            ));
            None
        }
    }
}

fn same_name(a: &str, b: &str, case_sensitive: bool) -> bool {
    a == b || (!case_sensitive && a.to_lowercase() == b.to_lowercase())
}

fn describe_field(
    path: &[String],
    field: &Field,
    type_name: &dyn Fn(&DataType) -> String,
) -> String {
    let null = if field.is_nullable() { "" } else { " NOT NULL" };
    format!(
        "{} {}{null}",
        quote_names(path),
        type_name(field.data_type())
    )
}

/// Adds the changes of a column or of a nested field from the type that a view was created
/// with, to that of the table now. The nullability of fields, their metadata, and the
/// names of the fields that Spark does not name (the elements of lists and the entries of
/// maps) are not compared. The definition of the view was resolved as if every field had
/// nulls (see [`ViewDefinitionSource`]).
fn column_changes(
    stored: &DataType,
    current: &DataType,
    path: &[String],
    fields: Option<&HashMap<String, ColumnUse>>,
    case_sensitive: bool,
    type_name: &dyn Fn(&DataType) -> String,
    changes: &mut Vec<String>,
) {
    let child = |name: &str| {
        let mut path = path.to_vec();
        path.push(name.to_string());
        path
    };
    let nested = |stored: &Field, current: &Field, name: &str, changes: &mut Vec<String>| {
        let path = child(name);
        column_changes(
            stored.data_type(),
            current.data_type(),
            &path,
            None,
            case_sensitive,
            type_name,
            changes,
        );
        logical_type_changes(stored, current, &path, type_name, changes);
    };
    match (stored, current) {
        (DataType::Struct(stored), DataType::Struct(current)) => {
            let matches = match_fields(stored, current, case_sensitive);
            for (field, found) in stored.iter().zip(matches) {
                // A struct that the view reads only through some of its fields keeps those
                // fields, and the other fields can change.
                let used = match fields {
                    Some(fields) => match fields
                        .iter()
                        .find(|(name, _)| same_name(name, field.name(), case_sensitive))
                    {
                        Some((_, used)) => used.fields(),
                        None => continue,
                    },
                    None => None,
                };
                field_changes(
                    field,
                    found,
                    current,
                    &child(field.name()),
                    used,
                    case_sensitive,
                    type_name,
                    changes,
                );
            }
            if fields.is_some() {
                return;
            }
            for field in current {
                if !stored
                    .iter()
                    .any(|x| same_name(x.name(), field.name(), case_sensitive))
                {
                    changes.push(format!(
                        "{} has been added",
                        describe_field(&child(field.name()), field, type_name)
                    ));
                }
            }
        }
        (
            DataType::List(stored)
            | DataType::LargeList(stored)
            | DataType::ListView(stored)
            | DataType::LargeListView(stored),
            DataType::List(current)
            | DataType::LargeList(current)
            | DataType::ListView(current)
            | DataType::LargeListView(current),
        ) => nested(stored, current, "element", changes),
        (DataType::FixedSizeList(stored, n), DataType::FixedSizeList(current, m)) if n == m => {
            nested(stored, current, "element", changes)
        }
        (DataType::Map(stored, x), DataType::Map(current, y)) if x == y => {
            match (stored.data_type(), current.data_type()) {
                (DataType::Struct(stored), DataType::Struct(current))
                    if stored.len() == 2 && current.len() == 2 =>
                {
                    nested(&stored[0], &current[0], "key", changes);
                    nested(&stored[1], &current[1], "value", changes);
                }
                _ => nested(stored, current, "entries", changes),
            }
        }
        (DataType::Dictionary(x, stored), DataType::Dictionary(y, current)) if x == y => {
            column_changes(
                stored,
                current,
                path,
                None,
                case_sensitive,
                type_name,
                changes,
            )
        }
        (DataType::RunEndEncoded(x, stored), DataType::RunEndEncoded(y, current))
            if x.data_type() == y.data_type() =>
        {
            nested(stored, current, "values", changes)
        }
        (DataType::Union(stored, x), DataType::Union(current, y))
            if x == y
                && stored.len() == current.len()
                && stored
                    .iter()
                    .zip(current.iter())
                    .all(|((i, a), (j, b))| i == j && a.name() == b.name()) =>
        {
            for ((_, stored), (_, current)) in stored.iter().zip(current.iter()) {
                nested(stored, current, stored.name(), changes);
            }
        }
        _ if stored != current && !same_values(stored, current) => changes.push(format!(
            "{} type has changed from {} to {}",
            quote_names(path),
            type_name(stored),
            type_name(current)
        )),
        _ => {}
    }
}

/// Returns whether a field of a struct, or of a struct in a struct, has a name of another
/// letter case than the stored field that it matches.
fn field_names_changed(stored: &DataType, current: &DataType) -> bool {
    let (DataType::Struct(stored), DataType::Struct(current)) = (stored, current) else {
        return false;
    };
    stored.iter().any(|field| {
        current
            .iter()
            .find(|x| same_name(x.name(), field.name(), false))
            .is_some_and(|x| {
                x.name() != field.name() || field_names_changed(field.data_type(), x.data_type())
            })
    })
}

/// Returns whether two string types or two binary types store the same values in other ways.
fn same_values(a: &DataType, b: &DataType) -> bool {
    use DataType::*;
    matches!(
        (a, b),
        (Utf8 | LargeUtf8 | Utf8View, Utf8 | LargeUtf8 | Utf8View)
            | (
                Binary | LargeBinary | BinaryView,
                Binary | LargeBinary | BinaryView
            )
    )
}

/// Returns whether a type that has no column change stores its values in another way than
/// the stored type at any level, as a string, binary or list type of another encoding.
fn encoding_changed(stored: &DataType, current: &DataType) -> bool {
    use DataType::*;
    match (stored, current) {
        (Struct(stored), Struct(current)) => stored.iter().any(|field| {
            current
                .iter()
                .find(|x| same_name(x.name(), field.name(), false))
                .is_some_and(|x| encoding_changed(field.data_type(), x.data_type()))
        }),
        (
            List(a) | LargeList(a) | ListView(a) | LargeListView(a),
            List(b) | LargeList(b) | ListView(b) | LargeListView(b),
        ) => {
            std::mem::discriminant(stored) != std::mem::discriminant(current)
                || encoding_changed(a.data_type(), b.data_type())
        }
        (FixedSizeList(a, _), FixedSizeList(b, _))
        | (Map(a, _), Map(b, _))
        | (RunEndEncoded(_, a), RunEndEncoded(_, b)) => {
            encoding_changed(a.data_type(), b.data_type())
        }
        (Dictionary(_, a), Dictionary(_, b)) => encoding_changed(a, b),
        (Union(a, _), Union(b, _)) => a
            .iter()
            .zip(b.iter())
            .any(|((_, a), (_, b))| encoding_changed(a.data_type(), b.data_type())),
        _ => stored != current && same_values(stored, current),
    }
}

fn quote_names(names: &[String]) -> String {
    names
        .iter()
        .map(|x| format!("`{}`", x.replace('`', "``")))
        .collect::<Vec<_>>()
        .join(".")
}

impl PlanResolver<'_> {
    /// Returns the plan that a temporary view stores for the plan of its definition: each
    /// scan of a source that the view obtains again at each read has a reference to what
    /// it reads in the place of the source, in the plan, in its subqueries, and in the
    /// CTE definitions that the plan refers to. A plan without such a scan is stored as
    /// it is.
    pub(in crate::resolver) fn store_view_plan(
        plan: LogicalPlan,
        reads: &ViewDefinitionReads,
    ) -> PlanResult<LogicalPlan> {
        let mut found = false;
        visit_scans(&plan, &mut |scan| {
            found |= reads.table_origin(&scan.source).is_some();
            Ok(())
        })?;
        if !found {
            return Ok(plan);
        }
        let required = required_columns(&plan, reads)?;
        // The stored plan has its own CTE definitions, in the order of the definitions.
        let mut index = 0;
        let plan = renew_cte_definitions_with_scans(
            plan,
            &mut || {
                index += 1;
                index - 1
            },
            &mut |_| {},
            &mut |scan| {
                Ok(match reads.table_origin(&scan.source) {
                    Some(origin) => {
                        let required = required.get(&table_source_address(&scan.source));
                        Transformed::yes(LogicalPlan::TableScan(
                            ViewTableReference::store(scan, origin, required).map_err(|e| {
                                datafusion_common::DataFusionError::External(e.into())
                            })?,
                        ))
                    }
                    None => Transformed::no(LogicalPlan::TableScan(scan)),
                })
            },
            // The stored scans have the nullability of the tables, which the plan above
            // them was resolved without.
            true,
        )?;
        Ok(plan)
    }

    /// Returns the plan of a read of a temporary view, for the stored plan of the view.
    /// This is the only way from a stored plan to a plan of a query: each reference to a
    /// table is bound to the source of a read of the table in this query, and the CTE
    /// definitions of the view become CTE definitions of this query.
    pub(super) async fn resolve_stored_view_plan(
        &self,
        plan: &LogicalPlan,
        view: &TableStatus,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let mut references = vec![];
        visit_scans(plan, &mut |scan| {
            if ViewTableReference::try_from_scan(scan).is_some() {
                references.push(scan.clone());
            }
            Ok(())
        })?;
        let plan = plan.clone();
        let mut scans = HashMap::new();
        // Whether a scan has another schema than the scan that the view was created with,
        // which is the case when only the metadata of its columns or the constraints of
        // the table changed. The nodes above the scan then compute their schemas again.
        let mut recompute_schemas = false;
        for scan in references {
            let address = table_source_address(&scan.source);
            if scans.contains_key(&address) {
                continue;
            }
            let bound = self.bind_view_table_scan(&scan, view, state).await?;
            recompute_schemas |= bound.schema() != &scan.projected_schema;
            scans.insert(address, bound);
        }
        let mut bind = |scan: TableScan| {
            Ok(match scans.get(&table_source_address(&scan.source)) {
                Some(bound) => Transformed::yes(bound.clone()),
                None => Transformed::no(LogicalPlan::TableScan(scan)),
            })
        };
        let plan = if self.share_ctes {
            self.renew_shared_cte_definitions(plan, &mut bind, recompute_schemas, state)?
        } else {
            let plan = Self::inline_shared_cte_references(plan)?
                .transform_down_with_subqueries(|plan| match plan {
                    LogicalPlan::TableScan(scan) => bind(scan),
                    plan => Ok(Transformed::no(plan)),
                })
                .data()?;
            if recompute_schemas {
                recompute_plan_schemas(plan)?
            } else {
                plan
            }
        };
        Ok(plan)
    }

    /// Returns the scan of a read of the view, for a scan of the stored plan of the view.
    async fn bind_view_table_scan(
        &self,
        scan: &TableScan,
        view: &TableStatus,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let Some(reference) = ViewTableReference::try_from_scan(scan) else {
            return Err(PlanError::internal("scan without a view table reference"));
        };
        let (source, table) = match reference.origin.as_ref() {
            TableOrigin::CatalogTable(origin) => (
                self.resolve_view_catalog_table_source(origin, view, state)
                    .await?,
                origin.name.clone(),
            ),
            TableOrigin::LakePath(origin) => (
                self.create_path_source(
                    &origin.format,
                    origin.schema.clone(),
                    origin.options.clone(),
                    origin.paths.clone(),
                    origin.read_case_sensitive,
                    state,
                )
                .await?,
                vec![origin.format.clone(), origin.paths.join(",")],
            ),
        };
        let table_schema = source.schema();
        let service = self.ctx.extension::<PlanService>()?;
        let type_name = |data_type: &DataType| {
            let name = service
                .plan_formatter()
                .data_type_to_simple_string(data_type)
                .unwrap_or_else(|_| data_type.to_string());
            if data_type.is_nested() {
                name
            } else {
                name.to_uppercase()
            }
        };
        let positions = reference
            .match_columns(&table_schema, self.config.case_sensitive, &type_name)
            .map_err(|changes| {
                PlanError::AnalysisError(format!(
                    "[INCOMPATIBLE_COLUMN_CHANGES_AFTER_VIEW_WITH_PLAN_CREATION] View {} plan \
                    references table {} whose data columns changed since the view plan was \
                    initially captured.\nColumn changes:\n- {}\nThis indicates the table has \
                    evolved and the view based on the plan must be recreated.",
                    view_name(view),
                    quote_names(&table),
                    changes.join("\n- "),
                ))
            })?;
        // A table with duplicate column names can only be scanned under other names. The
        // scan then reads each column under the name that the view was created with, and a
        // column that the view does not read keeps its name unless two columns would have it.
        let mut seen = HashSet::new();
        let has_duplicates = table_schema.fields().iter().any(|x| !seen.insert(x.name()));
        let source = if has_duplicates {
            let mut names = table_schema
                .fields()
                .iter()
                .map(|x| x.name().clone())
                .collect::<Vec<_>>();
            for (position, field) in positions.iter().zip(reference.schema.fields()) {
                if let Some(position) = position {
                    names[*position] = field.name().clone();
                }
            }
            let mut seen = positions
                .iter()
                .flatten()
                .map(|&i| names[i].clone())
                .collect::<HashSet<_>>();
            for (i, name) in names.iter_mut().enumerate() {
                if !positions.contains(&Some(i)) && !seen.insert(name.clone()) {
                    *name = state.register_field_name(name.clone());
                }
            }
            rename_source(&source, names)?
        } else {
            source
        };
        // A scan of all the columns of the table in their order has no projection, as the
        // scan of a direct read.
        let projection = positions.iter().flatten().copied().collect::<Vec<_>>();
        let projection = (projection.len() != table_schema.fields().len()
            || projection.iter().enumerate().any(|(i, &x)| i != x))
        .then_some(projection);
        // A view that is created over this view reads the table again as well, and the
        // columns that this view reads.
        let source = match state.view_definition_mut() {
            Some(reads) => {
                let source = view_definition_source(source);
                let uses = reference
                    .columns
                    .iter()
                    .zip(&positions)
                    .filter_map(|(&i, position)| {
                        Some(((*position)?, reference.required[i].clone()?))
                    })
                    .collect();
                reads.view_reads.insert(table_source_address(&source), uses);
                reads.record_table_origin(&source, Arc::clone(&reference.origin));
                source
            }
            None => source,
        };
        let bound = TableScanBuilder::new(scan.table_name.clone(), source)
            .with_projection(projection)
            .with_filters(scan.filters.clone())
            .with_fetch(scan.fetch)
            .build()?;
        reference.with_stored_output(bound, &positions, self.config.case_sensitive)
    }

    /// Returns the source of a read of a catalog table that a temporary view was created
    /// over, with the catalog table lookups and the table sources of the query.
    async fn resolve_view_catalog_table_source(
        &self,
        origin: &CatalogTableOrigin,
        view: &TableStatus,
        state: &mut PlanResolverState,
    ) -> PlanResult<Arc<dyn TableSource>> {
        let manager = self.ctx.extension::<CatalogManager>()?;
        // A table in a catalog of the session that created the view, which is not this
        // session, is read in that catalog. Any other table is read in the catalog of that
        // name in this session, as a direct read of the table.
        let session_table = match &origin.session_table {
            Some(table) if !manager.has_session_catalog_table(table)? => Some(table),
            _ => None,
        };
        let lookup = match session_table {
            Some(table) => match state.get_session_catalog_table_lookup(table, &origin.name) {
                Some(lookup) => Ok(lookup),
                None => {
                    let request = Self::catalog_table_lookup_request(&origin.name);
                    match table.get_lakehouse_table(request).await {
                        Ok(lookup) => {
                            state.insert_session_catalog_table_lookup(
                                table,
                                origin.name.clone(),
                                lookup.clone(),
                            );
                            Ok(lookup)
                        }
                        Err(CatalogError::NotFound(object, name)) => Err(CatalogError::NotFound(
                            object,
                            format!("[TABLE_OR_VIEW_NOT_FOUND] Table or view not found: {name}"),
                        )),
                        Err(error) => Err(error),
                    }
                }
            }
            .map(|(status, resolved)| (status, Some(resolved))),
            None => Self::lookup_catalog_object(&manager, origin.name.clone(), state).await,
        };
        let (status, resolved) = match lookup {
            Ok(lookup) => lookup,
            Err(CatalogError::NotFound(object, message)) => {
                return Err(CatalogError::NotFound(
                    object,
                    format!("{message} (read by the temporary view {})", view_name(view)),
                )
                .into());
            }
            Err(error) => return Err(error.into()),
        };
        let TableKind::Table { format, .. } = &status.kind else {
            return Err(PlanError::AnalysisError(format!(
                "[TABLE_OR_VIEW_NOT_FOUND] {} is now a view, and the temporary view {} reads it \
                as a table. Create the temporary view again.",
                quote_names(&origin.name),
                view_name(view),
            )));
        };
        if !origin.temporal_options.is_empty() && !format.eq_ignore_ascii_case(&origin.format) {
            return Err(PlanError::AnalysisError(format!(
                "the temporary view {} reads a version of {}, which has been created again \
                with another format",
                view_name(view),
                quote_names(&origin.name),
            )));
        }
        let lake_relation = match &origin.lake_relation {
            Some(name) => {
                let relation = self
                    .ctx
                    .extension::<DataSourceRegistry>()?
                    .get_lake_source_if_supported(format)?
                    .and_then(|x| x.relation_provider())
                    .and_then(|provider| match provider.resolve_relation(name) {
                        LakeRelationResolution::Supported(relation) => Some((provider, relation)),
                        _ => None,
                    });
                let Some(relation) = relation else {
                    return Err(PlanError::AnalysisError(format!(
                        "[TABLE_OR_VIEW_NOT_FOUND] Table or view not found: {} (read by the \
                        temporary view {})",
                        quote_name_if_needed(name),
                        view_name(view),
                    )));
                };
                Some(relation)
            }
            None => None,
        };
        self.resolve_catalog_table_source(
            CatalogTableRead {
                status,
                resolved,
                origin,
                session_table,
                lake_relation,
            },
            state,
        )
        .await
    }
}

/// Returns the quoted name of a temporary view.
fn view_name(view: &TableStatus) -> String {
    let mut name = view.database.clone();
    name.push(view.name.clone());
    quote_names(&name)
}

/// How a stored plan reads a column.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::resolver) enum ColumnUse {
    /// The plan reads the value of the column.
    Whole,
    /// The plan reads the values of some fields of a struct.
    Fields(HashMap<String, ColumnUse>),
    /// The plan only tells rows apart by the column, as a key of `DISTINCT` or `GROUP BY`
    /// whose value is not read. The current values of the column tell the rows apart, of
    /// whatever type they are now.
    Identity,
}

impl ColumnUse {
    /// The use of the field at the end of a path of struct fields.
    fn path(path: &[String]) -> Self {
        path.iter().rev().fold(ColumnUse::Whole, |used, name| {
            ColumnUse::Fields(HashMap::from([(name.clone(), used)]))
        })
    }

    /// Returns the fields that the plan reads, if it reads only some fields of a struct.
    fn fields(&self) -> Option<&HashMap<String, ColumnUse>> {
        match self {
            ColumnUse::Fields(fields) => Some(fields),
            _ => None,
        }
    }

    /// Adds another use of the column, and returns whether this use changed.
    fn merge(&mut self, other: &ColumnUse) -> bool {
        match (&mut *self, other) {
            (ColumnUse::Whole, _) | (ColumnUse::Identity, ColumnUse::Identity) => false,
            (ColumnUse::Fields(fields), ColumnUse::Fields(others)) => {
                let mut changed = false;
                for (name, other) in others {
                    changed |= add_use(fields, name.clone(), other);
                }
                changed
            }
            // Rows told apart by a struct whose fields are also read need the struct as
            // it was.
            _ => {
                *self = ColumnUse::Whole;
                true
            }
        }
    }
}

/// Adds a use of a column or a field, and returns whether the uses changed.
fn add_use<K: Eq + Hash>(uses: &mut HashMap<K, ColumnUse>, key: K, used: &ColumnUse) -> bool {
    match uses.entry(key) {
        Entry::Occupied(mut entry) => entry.get_mut().merge(used),
        Entry::Vacant(entry) => {
            entry.insert(used.clone());
            true
        }
    }
}

/// Returns the column and the path of fields of a chain of struct field accesses, also of a
/// column of an outer query.
fn field_access(expr: &Expr) -> Option<(&Column, Vec<String>)> {
    let Expr::ScalarFunction(function) = expr else {
        return None;
    };
    if !function.func.inner().is::<GetFieldFunc>() {
        return None;
    }
    let [base, Expr::Literal(name, _)] = function.args.as_slice() else {
        return None;
    };
    let name = name.try_as_str().flatten()?.to_string();
    match base {
        Expr::Column(column) | Expr::OuterReferenceColumn(_, column) => Some((column, vec![name])),
        base => {
            let (column, mut path) = field_access(base)?;
            path.push(name);
            Some((column, path))
        }
    }
}

/// Adds the columns that an expression reads, and the fields of the structs that it
/// reads only through field accesses.
fn add_expression_uses(
    expr: &Expr,
    uses: &mut HashMap<String, ColumnUse>,
) -> datafusion_common::Result<()> {
    expr.apply(|e| {
        if let Some((column, path)) = field_access(e) {
            add_use(uses, column.name.clone(), &ColumnUse::path(&path));
            return Ok(TreeNodeRecursion::Jump);
        }
        if let Expr::Column(column) | Expr::OuterReferenceColumn(_, column) = e {
            add_use(uses, column.name.clone(), &ColumnUse::Whole);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(())
}

/// How an output of a projection or of a scan reads the columns that it comes from, by
/// their names or by their positions in a source.
#[derive(Debug, Clone)]
enum Derivation<K> {
    /// The output is the column, so it reads the column as the output is read.
    Column(K),
    /// The output is the column, which must keep its type, as an input of `UNION`.
    Value(K),
    /// The output is computed, and it reads these columns when it is read.
    Computed(HashMap<K, ColumnUse>),
}

impl Derivation<String> {
    fn of(expr: &Expr) -> datafusion_common::Result<Self> {
        // An output that passes a column on as it is, under its name or other ones.
        let mut passed = expr;
        while let Expr::Alias(alias) = passed {
            passed = &alias.expr;
        }
        Ok(match passed.try_as_col() {
            Some(column) => Derivation::Column(column.name.clone()),
            None => {
                let mut reads = HashMap::new();
                add_expression_uses(expr, &mut reads)?;
                Derivation::Computed(reads)
            }
        })
    }
}

impl<K: Eq + Hash + Clone> Derivation<K> {
    /// Adds the uses of the columns that the output reads when it is read as `used`, and
    /// returns the columns whose uses changed.
    fn add_reads(&self, used: &ColumnUse, uses: &mut HashMap<K, ColumnUse>) -> Vec<K> {
        let reads = match self {
            Derivation::Column(column) => vec![(column, used)],
            Derivation::Value(column) if used == &ColumnUse::Identity => {
                vec![(column, &ColumnUse::Whole)]
            }
            Derivation::Value(column) => vec![(column, used)],
            Derivation::Computed(reads) => reads.iter().collect(),
        };
        reads
            .into_iter()
            .filter(|(column, read)| add_use(uses, (*column).clone(), read))
            .map(|(column, _)| column.clone())
            .collect()
    }
}

/// Returns whether the projection gives names of the query to the outputs of the plan of a
/// temporary view that the plan reads. A rewrite of the plan, such as type coercion, can
/// compute the schema of the plan of the view again, with the same names. The names of
/// the query are new, so another projection gives them to the same inputs only if they
/// are the names of the inputs.
fn reads_view_plan(projection: &Projection, reads: &ViewDefinitionReads) -> bool {
    let input = projection.input.schema();
    reads.view_plans.iter().any(|(schema, names)| {
        Arc::ptr_eq(schema, input)
            || (!schema.fields().iter().map(|x| x.name()).eq(names)
                && schema
                    .iter()
                    .map(|(q, f)| (q, f.name()))
                    .eq(input.iter().map(|(q, f)| (q, f.name())))
                && projection
                    .schema
                    .fields()
                    .iter()
                    .map(|x| x.name())
                    .eq(names))
    })
}

/// Returns the address of the source of a scan that a view obtains again, and how each
/// output reads the source, for the scan itself or for projections over it that compute
/// their outputs from the scan alone. A scan with filters is not described, and all its
/// columns are read.
fn scan_lineage(
    plan: &LogicalPlan,
    reads: &ViewDefinitionReads,
) -> Option<(usize, HashMap<String, Derivation<usize>>)> {
    match plan {
        LogicalPlan::TableScan(scan)
            if scan.filters.is_empty() && reads.table_origin(&scan.source).is_some() =>
        {
            let outputs = scan
                .projected_schema
                .fields()
                .iter()
                .enumerate()
                .map(|(i, field)| {
                    let position = scan.projection.as_ref().map_or(i, |p| p[i]);
                    (field.name().clone(), Derivation::Column(position))
                })
                .collect();
            Some((table_source_address(&scan.source), outputs))
        }
        LogicalPlan::Projection(projection) if !reads_view_plan(projection, reads) => {
            let (address, inputs) = scan_lineage(&projection.input, reads)?;
            let mut outputs = HashMap::new();
            for (expr, field) in projection.expr.iter().zip(projection.schema.fields()) {
                let lineage = match Derivation::of(expr).ok()? {
                    Derivation::Column(name) | Derivation::Value(name) => {
                        inputs.get(&name)?.clone()
                    }
                    Derivation::Computed(reads) => {
                        // A subquery or an outer reference reads columns of other plans.
                        let other = expr.exists(|e| {
                            Ok(matches!(
                                e,
                                Expr::ScalarSubquery(_)
                                    | Expr::Exists(_)
                                    | Expr::InSubquery(_)
                                    | Expr::SetComparison(_)
                                    | Expr::OuterReferenceColumn(..)
                            ))
                        });
                        if other.ok()? {
                            return None;
                        }
                        let mut positions = HashMap::new();
                        for (name, read) in &reads {
                            inputs.get(name)?.add_reads(read, &mut positions);
                        }
                        Derivation::Computed(positions)
                    }
                };
                outputs.insert(field.name().clone(), lineage);
            }
            // The outputs of the projections below are read too when a function of this
            // projection no longer refers to them (see `PlanResolverState::folded_columns`).
            if matches!(projection.input.as_ref(), LogicalPlan::Projection(_)) {
                for (name, lineage) in inputs {
                    outputs.entry(name).or_insert(lineage);
                }
            }
            Some((address, outputs))
        }
        _ => None,
    }
}

/// Returns how the plan reads the columns of each source that a temporary view obtains
/// again, by the address of the source and the position of the column in the source. All
/// the columns of a source without an entry are read.
///
/// The plan reads its outputs and the columns of the expressions of its operators. A
/// projection or a union reads its inputs only for the outputs that are read. `DISTINCT`
/// and grouping columns tell rows apart. Operators that do not pass their inputs on read
/// all of them. The uses are propagated once the plan, its subqueries and its CTE
/// definitions are visited, so the order of the visit does not matter.
fn required_columns(
    plan: &LogicalPlan,
    reads: &ViewDefinitionReads,
) -> PlanResult<HashMap<usize, HashMap<usize, ColumnUse>>> {
    let mut uses = HashMap::new();
    let folded = reads.folded_columns.iter();
    for name in plan
        .schema()
        .fields()
        .iter()
        .map(|x| x.name())
        .chain(folded)
    {
        add_use(&mut uses, name.clone(), &ColumnUse::Whole);
    }
    let mut derivations: HashMap<String, Vec<Derivation<String>>> = HashMap::new();
    let mut lineages = vec![];
    let mut whole = HashSet::new();
    visit_nodes(plan, &mut |node| {
        match node {
            // The plan of a view that the plan reads has the field IDs of another resolver
            // state, and the view reads the columns that it read when it was created.
            LogicalPlan::Projection(projection) if reads_view_plan(projection, reads) => {
                return Ok(TreeNodeRecursion::Jump);
            }
            LogicalPlan::Projection(projection) => {
                if let Some(lineage) = scan_lineage(node, reads) {
                    // The projections below it and the scan are part of the lineage.
                    lineages.push(lineage);
                    return Ok(TreeNodeRecursion::Jump);
                }
                for (expr, field) in projection.expr.iter().zip(projection.schema.fields()) {
                    derivations
                        .entry(field.name().clone())
                        .or_default()
                        .push(Derivation::of(expr)?);
                }
            }
            LogicalPlan::TableScan(scan) => {
                if let Some(reference) = CteReference::try_from_scan(scan) {
                    for field in reference.definition().resolved().schema().fields() {
                        add_use(&mut uses, field.name().clone(), &ColumnUse::Whole);
                    }
                } else if reads.table_origin(&scan.source).is_some() {
                    whole.insert(table_source_address(&scan.source));
                }
                for filter in &scan.filters {
                    add_expression_uses(filter, &mut uses)?;
                }
            }
            LogicalPlan::Union(union) => {
                // Each output reads the column at its position in each input.
                for input in &union.inputs {
                    for (output, field) in union.schema.fields().iter().zip(input.schema().fields())
                    {
                        derivations
                            .entry(output.name().clone())
                            .or_default()
                            .push(Derivation::Value(field.name().clone()));
                    }
                }
            }
            LogicalPlan::Distinct(Distinct::All(input)) => {
                for field in input.schema().fields() {
                    add_use(&mut uses, field.name().clone(), &ColumnUse::Identity);
                }
            }
            LogicalPlan::Distinct(Distinct::On(on)) if on.sort_expr.is_none() => {
                for expr in &on.on_expr {
                    match expr {
                        Expr::Column(column) => {
                            add_use(&mut uses, column.name.clone(), &ColumnUse::Identity);
                        }
                        expr => add_expression_uses(expr, &mut uses)?,
                    }
                }
                for (expr, field) in on.select_expr.iter().zip(on.schema.fields()) {
                    derivations
                        .entry(field.name().clone())
                        .or_default()
                        .push(Derivation::of(expr)?);
                }
            }
            LogicalPlan::Aggregate(aggregate) => {
                for expr in &aggregate.group_expr {
                    match expr {
                        Expr::Column(column) => {
                            add_use(&mut uses, column.name.clone(), &ColumnUse::Identity);
                        }
                        expr => add_expression_uses(expr, &mut uses)?,
                    }
                }
                for expr in &aggregate.aggr_expr {
                    add_expression_uses(expr, &mut uses)?;
                }
            }
            node => {
                node.apply_expressions(|e| {
                    add_expression_uses(e, &mut uses)?;
                    Ok(TreeNodeRecursion::Continue)
                })?;
                // These operators pass their input columns on, and any other operator
                // may read all of them.
                let passes_on = matches!(
                    node,
                    LogicalPlan::Filter(_)
                        | LogicalPlan::Sort(_)
                        | LogicalPlan::Window(_)
                        | LogicalPlan::Join(_)
                        | LogicalPlan::Limit(_)
                        | LogicalPlan::SubqueryAlias(_)
                        | LogicalPlan::Repartition(_)
                );
                if !passes_on {
                    for input in node.inputs() {
                        for field in input.schema().fields() {
                            add_use(&mut uses, field.name().clone(), &ColumnUse::Whole);
                        }
                    }
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    // A projection output that is read reads the input of the projection.
    let mut queue = uses.keys().cloned().collect::<Vec<_>>();
    while let Some(name) = queue.pop() {
        let (Some(inputs), Some(used)) = (derivations.get(&name), uses.get(&name).cloned()) else {
            continue;
        };
        for input in inputs {
            queue.extend(input.add_reads(&used, &mut uses));
        }
    }
    let mut required: HashMap<usize, HashMap<usize, ColumnUse>> = HashMap::new();
    for (address, outputs) in lineages {
        let columns = required.entry(address).or_default();
        for (name, lineage) in outputs {
            let Some(used) = uses.get(&name) else {
                continue;
            };
            lineage.add_reads(used, columns);
        }
    }
    for (address, reads) in &reads.view_reads {
        let columns = required.entry(*address).or_default();
        for (position, used) in reads {
            add_use(columns, *position, used);
        }
    }
    for address in whole {
        required.remove(&address);
    }
    Ok(required)
}

/// Returns the value of a struct with the type that the view was created with, from the
/// current value of a struct that the view reads through some of its fields or whole. A
/// field that the view does not read is null, so every field of the value can have nulls.
fn rebuild_struct(
    expr: Expr,
    stored: &Field,
    current: &Field,
    used: &ColumnUse,
    case_sensitive: bool,
) -> PlanResult<Expr> {
    let (DataType::Struct(stored_fields), DataType::Struct(current_fields)) =
        (stored.data_type(), current.data_type())
    else {
        return Ok(expr);
    };
    // A struct that is read whole reads all its fields.
    let whole;
    let fields = match used {
        ColumnUse::Fields(fields) => fields,
        ColumnUse::Whole => {
            whole = stored_fields
                .iter()
                .map(|x| (x.name().clone(), ColumnUse::Whole))
                .collect();
            &whole
        }
        ColumnUse::Identity => return Ok(expr),
    };
    if stored.data_type() == current.data_type() {
        return Ok(expr);
    }
    let matches = match_fields(stored_fields, current_fields, case_sensitive);
    let mut args = Vec::with_capacity(stored_fields.len() * 2);
    for (field, found) in stored_fields.iter().zip(matches) {
        let used = fields
            .iter()
            .find(|(name, _)| same_name(name, field.name(), case_sensitive));
        let value = match (used, found) {
            (Some((_, used)), FieldMatch::Found(position)) => {
                let current = &current_fields[position];
                let value = get_field().call(vec![expr.clone(), lit(current.name().as_str())]);
                rebuild_struct(value, field, current, used, case_sensitive)?
            }
            _ => Expr::Literal(ScalarValue::try_new_null(field.data_type())?, None),
        };
        args.push(lit(field.name().as_str()));
        args.push(Expr::Cast(Cast::new_from_field(
            Box::new(value),
            Arc::new(nullable_field(field)),
        )));
    }
    let null = Expr::Literal(
        ScalarValue::try_new_null(nullable_field(stored).data_type())?,
        None,
    );
    Ok(when(expr.is_null(), null).otherwise(named_struct().call(args))?)
}

/// The source of a scan of a table that a temporary view reads again at each read, while
/// the definition of the view is resolved. All its fields can have nulls, at every level,
/// so that no decision of the definition depends on a column without nulls, which it may
/// have when the view is read. It is never executed, since the stored plan has a
/// [`ViewTableReference`] in its place.
struct ViewDefinitionSource {
    inner: Arc<dyn TableSource>,
    schema: SchemaRef,
}

impl std::fmt::Debug for ViewDefinitionSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ViewDefinitionSource")
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl TableSource for ViewDefinitionSource {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// Returns the plan with the source that each [`ViewDefinitionSource`] wraps in its place,
/// for a plan that the resolver executes while the definition of a temporary view is
/// resolved, such as the plan that finds the values of a pivot.
pub(in crate::resolver) fn executable_plan(plan: LogicalPlan) -> PlanResult<LogicalPlan> {
    let mut found = false;
    visit_scans(&plan, &mut |scan| {
        found |= scan.source.downcast_ref::<ViewDefinitionSource>().is_some();
        Ok(())
    })?;
    if !found {
        return Ok(plan);
    }
    let mut index = 0;
    let plan = renew_cte_definitions_with_scans(
        plan,
        &mut || {
            index += 1;
            index - 1
        },
        &mut |_| {},
        &mut |scan| {
            let Some(source) = scan.source.downcast_ref::<ViewDefinitionSource>() else {
                return Ok(Transformed::no(LogicalPlan::TableScan(scan)));
            };
            let scan = TableScanBuilder::new(scan.table_name, Arc::clone(&source.inner))
                .with_projection(scan.projection)
                .with_filters(scan.filters)
                .with_fetch(scan.fetch)
                .build()?;
            Ok(Transformed::yes(LogicalPlan::TableScan(scan)))
        },
        // The scans have the nullability of the tables, which the plan above them was
        // resolved without.
        true,
    )?;
    Ok(plan)
}

pub(in crate::resolver) fn view_definition_source(
    source: Arc<dyn TableSource>,
) -> Arc<dyn TableSource> {
    let schema = source.schema();
    let fields = schema
        .fields()
        .iter()
        .map(|field| nullable_field(field))
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
    Arc::new(ViewDefinitionSource {
        inner: source,
        schema,
    })
}

/// Returns the field with nulls allowed at every level, except for the keys of maps.
/// Unlike `sail_common_datafusion::schema::nullable_query_field`, it also allows nulls in
/// the storage of extension types and in list views, run-end encoded values and unions.
fn nullable_field(field: &Field) -> Field {
    let nullable = |field: &Arc<Field>| Arc::new(nullable_field(field));
    let data_type = match field.data_type() {
        DataType::Struct(fields) => DataType::Struct(fields.iter().map(nullable).collect()),
        DataType::List(element) => DataType::List(nullable(element)),
        DataType::LargeList(element) => DataType::LargeList(nullable(element)),
        DataType::ListView(element) => DataType::ListView(nullable(element)),
        DataType::LargeListView(element) => DataType::LargeListView(nullable(element)),
        DataType::FixedSizeList(element, n) => DataType::FixedSizeList(nullable(element), *n),
        DataType::Map(entries, sorted) => match entries.data_type() {
            DataType::Struct(fields) if fields.len() == 2 => {
                let fields = vec![Arc::clone(&fields[0]), nullable(&fields[1])];
                let entries = entries
                    .as_ref()
                    .clone()
                    .with_data_type(DataType::Struct(fields.into()));
                DataType::Map(Arc::new(entries), *sorted)
            }
            _ => field.data_type().clone(),
        },
        DataType::RunEndEncoded(run_ends, values) => {
            DataType::RunEndEncoded(Arc::clone(run_ends), nullable(values))
        }
        DataType::Union(fields, mode) => DataType::Union(
            fields
                .iter()
                .map(|(id, field)| (id, nullable(field)))
                .collect(),
            *mode,
        ),
        data_type => data_type.clone(),
    };
    field.clone().with_data_type(data_type).with_nullable(true)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{
        DataType, Field, Fields, IntervalUnit, Schema, TimeUnit, UnionFields, UnionMode,
    };
    use datafusion::datasource::{MemTable, provider_as_source};
    use datafusion::functions_aggregate::expr_fn::sum;
    use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion_common::{Column, JoinType, ScalarValue};
    use datafusion_expr::expr::Exists;
    use datafusion_expr::{Expr, LogicalPlan, LogicalPlanBuilder, Subquery, TableScan, col, lit};
    use datafusion_functions::core::get_field;

    use super::{
        ColumnUse, TableOrigin, ViewTableReference, required_columns, user_metadata_differs,
        view_definition_source,
    };
    use crate::resolver::query::temp_view::CatalogTableOrigin;
    use crate::resolver::state::{ViewDefinitionReads, table_source_address};
    use crate::shared_cte::CteDefinition;

    fn origin() -> Arc<TableOrigin> {
        Arc::new(TableOrigin::CatalogTable(CatalogTableOrigin {
            name: vec![],
            session_table: None,
            lake_relation: None,
            format: String::new(),
            options: vec![],
            temporal_options: vec![],
            read_case_sensitive: false,
        }))
    }

    fn reference(schema: &Schema) -> ViewTableReference {
        ViewTableReference {
            schema: Arc::new(schema.clone()),
            table: Arc::new(schema.clone()),
            columns: (0..schema.fields().len()).collect(),
            required: vec![Some(ColumnUse::Whole); schema.fields().len()],
            origin: origin(),
        }
    }

    fn type_name(data_type: &DataType) -> String {
        format!("{data_type}")
    }

    /// Matches the columns of a scan that reads all of them whole.
    fn match_columns(
        stored: &Schema,
        table: &Schema,
        case_sensitive: bool,
    ) -> Result<Vec<usize>, Vec<String>> {
        let positions = reference(stored).match_columns(table, case_sensitive, &type_name)?;
        Ok(positions.into_iter().flatten().collect())
    }

    fn field(name: &str, data_type: DataType) -> Field {
        Field::new(name, data_type, true)
    }

    fn with_metadata(field: Field) -> Field {
        field.with_metadata(HashMap::from([("comment".to_string(), "x".to_string())]))
    }

    fn list(element: Field) -> DataType {
        DataType::List(Arc::new(element))
    }

    fn map(key: Field, value: Field) -> DataType {
        let entries = Field::new("entries", DataType::Struct(vec![key, value].into()), false);
        DataType::Map(Arc::new(entries), false)
    }

    fn nested(data_type: DataType) -> Vec<DataType> {
        let item = || Arc::new(field("item", data_type.clone()));
        vec![
            DataType::List(item()),
            DataType::LargeList(item()),
            DataType::ListView(item()),
            DataType::LargeListView(item()),
            DataType::FixedSizeList(item(), 2),
            DataType::Struct(vec![field("a", data_type.clone())].into()),
            map(
                Field::new("key", DataType::Utf8, false),
                field("value", data_type.clone()),
            ),
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(data_type.clone())),
            DataType::RunEndEncoded(
                Arc::new(Field::new("run_ends", DataType::Int32, false)),
                Arc::new(field("values", data_type.clone())),
            ),
            DataType::Union(
                [(0, field("x", data_type)), (1, field("y", DataType::Null))]
                    .into_iter()
                    .map(|(id, field)| (id, Arc::new(field)))
                    .collect::<UnionFields>(),
                UnionMode::Dense,
            ),
        ]
    }

    fn data_types() -> Vec<DataType> {
        let mut types = vec![
            DataType::Null,
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Float16,
            DataType::Float32,
            DataType::Float64,
            DataType::Decimal128(10, 2),
            DataType::Decimal256(40, 3),
            DataType::Date32,
            DataType::Date64,
            DataType::Time32(TimeUnit::Second),
            DataType::Time32(TimeUnit::Millisecond),
            DataType::Time64(TimeUnit::Microsecond),
            DataType::Time64(TimeUnit::Nanosecond),
            DataType::Interval(IntervalUnit::YearMonth),
            DataType::Interval(IntervalUnit::DayTime),
            DataType::Interval(IntervalUnit::MonthDayNano),
            DataType::Binary,
            DataType::LargeBinary,
            DataType::BinaryView,
            DataType::FixedSizeBinary(4),
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Utf8View,
        ];
        for unit in [
            TimeUnit::Second,
            TimeUnit::Millisecond,
            TimeUnit::Microsecond,
            TimeUnit::Nanosecond,
        ] {
            types.push(DataType::Timestamp(unit, None));
            types.push(DataType::Timestamp(unit, Some("UTC".into())));
            types.push(DataType::Duration(unit));
        }
        types.extend(nested(DataType::Int32));
        types.extend(nested(list(field("item", DataType::Utf8))));
        types
    }

    /// Returns the type with one encoding for each kind of string, binary and list.
    fn one_encoding(data_type: &DataType) -> DataType {
        let field = |field: &Arc<Field>| {
            let data_type = one_encoding(field.data_type());
            Arc::new(field.as_ref().clone().with_data_type(data_type))
        };
        match data_type {
            DataType::LargeUtf8 | DataType::Utf8View => DataType::Utf8,
            DataType::LargeBinary | DataType::BinaryView => DataType::Binary,
            DataType::List(element)
            | DataType::LargeList(element)
            | DataType::ListView(element)
            | DataType::LargeListView(element) => DataType::List(field(element)),
            DataType::FixedSizeList(element, n) => DataType::FixedSizeList(field(element), *n),
            DataType::Struct(fields) => DataType::Struct(fields.iter().map(field).collect()),
            DataType::Map(entries, sorted) => DataType::Map(field(entries), *sorted),
            data_type => data_type.clone(),
        }
    }

    #[test]
    fn test_columns_of_each_type_match_the_encodings_of_the_type_and_no_other_type() {
        let types = data_types();
        for stored in &types {
            for current in &types {
                let result = match_columns(
                    &Schema::new(vec![field("c", stored.clone())]),
                    &Schema::new(vec![field("c", current.clone())]),
                    true,
                );
                if one_encoding(stored) == one_encoding(current) {
                    assert_eq!(result, Ok(vec![0]), "{stored}");
                } else {
                    assert!(result.is_err(), "{stored} and {current}");
                }
            }
        }
    }

    #[test]
    fn test_nullability_changes_of_each_type_are_not_column_changes() {
        for data_type in data_types() {
            let nullable = Schema::new(vec![field("c", data_type.clone())]);
            let required = Schema::new(vec![Field::new("c", data_type.clone(), false)]);
            assert_eq!(
                match_columns(&nullable, &required, true),
                Ok(vec![0]),
                "{data_type}"
            );
            assert_eq!(
                match_columns(&required, &nullable, true),
                Ok(vec![0]),
                "{data_type}"
            );
        }
    }

    /// Asserts that the field and its nested fields have nulls, except for map keys.
    fn assert_nullable(field: &Field) {
        assert!(field.is_nullable(), "{field}");
        match field.data_type() {
            DataType::Struct(fields) => fields.iter().for_each(|x| assert_nullable(x)),
            DataType::List(x)
            | DataType::LargeList(x)
            | DataType::ListView(x)
            | DataType::LargeListView(x)
            | DataType::FixedSizeList(x, _) => assert_nullable(x),
            DataType::Map(entries, _) => {
                let fields = match entries.data_type() {
                    DataType::Struct(fields) => fields.clone(),
                    _ => Fields::empty(),
                };
                assert_eq!(fields.len(), 2, "{entries}");
                assert!(!fields[0].is_nullable(), "{entries}");
                assert_nullable(&fields[1]);
            }
            DataType::RunEndEncoded(_, values) => assert_nullable(values),
            DataType::Union(fields, _) => fields.iter().for_each(|(_, x)| assert_nullable(x)),
            _ => {}
        }
    }

    #[test]
    fn test_view_definition_sources_have_nulls_at_every_level() -> TestResult {
        fn required(data_type: DataType) -> DataType {
            let required = |field: &Arc<Field>| {
                Arc::new(
                    field
                        .as_ref()
                        .clone()
                        .with_data_type(required(field.data_type().clone()))
                        .with_nullable(false),
                )
            };
            match data_type {
                DataType::Struct(fields) => DataType::Struct(fields.iter().map(required).collect()),
                DataType::List(x) => DataType::List(required(&x)),
                DataType::FixedSizeList(x, n) => DataType::FixedSizeList(required(&x), n),
                other => other,
            }
        }
        let fields = data_types()
            .into_iter()
            .enumerate()
            .map(|(i, x)| Field::new(format!("c{i}"), required(x), false))
            .collect::<Vec<_>>();
        let table = MemTable::try_new(Arc::new(Schema::new(fields)), vec![vec![]])?;
        let source = view_definition_source(provider_as_source(Arc::new(table)));
        for field in source.schema().fields() {
            assert_nullable(field);
        }
        Ok(())
    }

    #[test]
    fn test_metadata_and_unnamed_field_names_are_not_column_changes() {
        for data_type in data_types() {
            // The metadata of the column, and of the fields of each nested type.
            let stored = Schema::new(vec![field("c", data_type.clone())]);
            let current = Schema::new(vec![with_metadata(field("c", data_type.clone()))]);
            assert_eq!(match_columns(&stored, &current, true), Ok(vec![0]));
            assert_eq!(match_columns(&current, &stored, true), Ok(vec![0]));
            for (plain, annotated) in [
                (
                    list(field("item", data_type.clone())),
                    // The element of a list has no name in Spark.
                    list(with_metadata(field("element", data_type.clone()))),
                ),
                (
                    DataType::Struct(vec![field("a", data_type.clone())].into()),
                    DataType::Struct(vec![with_metadata(field("a", data_type.clone()))].into()),
                ),
                (
                    map(
                        Field::new("key", DataType::Utf8, false),
                        field("value", data_type.clone()),
                    ),
                    DataType::Map(
                        Arc::new(Field::new(
                            "key_value",
                            DataType::Struct(
                                vec![
                                    with_metadata(Field::new("k", DataType::Utf8, false)),
                                    with_metadata(field("v", data_type.clone())),
                                ]
                                .into(),
                            ),
                            false,
                        )),
                        false,
                    ),
                ),
            ] {
                let stored = Schema::new(vec![field("c", plain)]);
                let current = Schema::new(vec![field("c", annotated)]);
                assert_eq!(match_columns(&stored, &current, true), Ok(vec![0]));
                assert_eq!(match_columns(&current, &stored, true), Ok(vec![0]));
            }
        }
    }

    #[test]
    fn test_table_columns_can_be_added_and_reordered() {
        let stored = Schema::new(vec![
            field("a", DataType::Int32),
            field("b", DataType::Utf8),
        ]);
        let current = Schema::new(vec![
            field("x", DataType::Int32),
            field("b", DataType::Utf8),
            field("y", DataType::Int32),
            field("a", DataType::Int32),
        ]);
        assert_eq!(match_columns(&stored, &current, true), Ok(vec![3, 1]));
        // A column that the scan read is removed, or has another name.
        assert_eq!(
            match_columns(&current, &stored, true),
            Err(vec![
                "`x` Int32 has been removed".to_string(),
                "`y` Int32 has been removed".to_string()
            ])
        );
        let required = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        assert_eq!(
            match_columns(&required, &Schema::empty(), true),
            Err(vec!["`a` Int32 NOT NULL has been removed".to_string()])
        );
    }

    #[test]
    fn test_column_names_match_by_the_letter_case_setting() {
        let stored = Schema::new(vec![field("Id", DataType::Int32)]);
        let current = Schema::new(vec![
            field("x", DataType::Int32),
            field("ID", DataType::Int32),
        ]);
        assert_eq!(match_columns(&stored, &current, false), Ok(vec![1]));
        assert_eq!(
            match_columns(&stored, &current, true),
            Err(vec!["`Id` Int32 has been removed".to_string()])
        );
        // The name as it is written comes first.
        let current = Schema::new(vec![
            field("ID", DataType::Int32),
            field("Id", DataType::Int32),
        ]);
        assert_eq!(match_columns(&stored, &current, false), Ok(vec![1]));
        // The fields of a struct.
        let stored = Schema::new(vec![field(
            "s",
            DataType::Struct(vec![field("a", DataType::Int32)].into()),
        )]);
        let current = Schema::new(vec![field(
            "s",
            DataType::Struct(vec![field("A", DataType::Int32)].into()),
        )]);
        assert_eq!(match_columns(&stored, &current, false), Ok(vec![0]));
        assert_eq!(
            match_columns(&stored, &current, true),
            Err(vec![
                "`s`.`a` Int32 has been removed".to_string(),
                "`s`.`A` Int32 has been added".to_string()
            ])
        );
    }

    #[test]
    fn test_logical_type_metadata_is_part_of_the_column_type() {
        let interval = |value: &str| {
            field("c", DataType::Interval(IntervalUnit::YearMonth)).with_metadata(HashMap::from([
                (
                    sail_common::spec::SAIL_SPARK_INTERVAL_METADATA_KEY.to_string(),
                    value.to_string(),
                ),
            ]))
        };
        let stored = Schema::new(vec![interval("year")]);
        let current = Schema::new(vec![interval("month")]);
        let changes = match_columns(&stored, &current, true)
            .err()
            .unwrap_or_default();
        assert_eq!(changes.len(), 1);
        assert!(
            changes[0].starts_with("`c` type has changed"),
            "{changes:?}"
        );
        assert_eq!(match_columns(&stored, &stored, true), Ok(vec![0]));
        // The same change in a nested field.
        let wrap = |x: Field| Schema::new(vec![field("l", list(x))]);
        let changes = match_columns(&wrap(interval("year")), &wrap(interval("month")), true)
            .err()
            .unwrap_or_default();
        assert_eq!(changes.len(), 1);
        assert!(changes[0].starts_with("`l`.`element` type"), "{changes:?}");
    }

    #[test]
    fn test_user_metadata_changes_are_found_at_each_level() {
        let user = |value: &str| {
            field("a", DataType::Int32).with_metadata(HashMap::from([(
                sail_common::spec::SPARK_METADATA_JSON_KEY.to_string(),
                value.to_string(),
            )]))
        };
        assert!(user_metadata_differs(&user("{}"), &user("{\"x\":1}")));
        assert!(!user_metadata_differs(
            &with_metadata(field("a", DataType::Int32)),
            &field("a", DataType::Int32)
        ));
        let nested = |x: Field| field("s", DataType::Struct(vec![x].into()));
        assert!(user_metadata_differs(
            &nested(user("{}")),
            &nested(user("{\"x\":1}"))
        ));
        let in_list = |x: Field| field("l", list(x));
        assert!(user_metadata_differs(
            &in_list(user("{}")),
            &in_list(user("{\"x\":1}"))
        ));
    }

    #[test]
    fn test_columns_that_the_view_does_not_read_can_change() -> TestResult {
        let table = Schema::new(vec![
            field("a", DataType::Int32),
            field("b", DataType::Utf8),
            field("c", DataType::Int32),
        ]);
        // The scan captured `a` and `b`, and the view reads only `a`.
        let reference = ViewTableReference {
            schema: Arc::new(table.project(&[0, 1])?),
            columns: vec![0, 1],
            required: vec![Some(ColumnUse::Whole), None, None],
            ..reference(&table)
        };
        let current = Schema::new(vec![
            field("b", DataType::Utf8),
            field("a", DataType::Int32),
        ]);
        assert_eq!(
            reference.match_columns(&current, true, &type_name),
            Ok(vec![Some(1), Some(0)])
        );
        for current in [
            Schema::new(vec![field("a", DataType::Int32)]),
            Schema::new(vec![
                field("a", DataType::Int32),
                field("b", DataType::Int64),
            ]),
            Schema::new(vec![
                field("a", DataType::Int32),
                Field::new("b", DataType::Int64, false),
            ]),
        ] {
            assert_eq!(
                reference.match_columns(&current, true, &type_name),
                Ok(vec![Some(0), None]),
                "{current}"
            );
        }
        // A column without nulls now is read, as the same column.
        let current = Schema::new(vec![
            field("a", DataType::Int32),
            Field::new("b", DataType::Utf8, false),
        ]);
        assert_eq!(
            reference.match_columns(&current, true, &type_name),
            Ok(vec![Some(0), Some(1)])
        );
        assert_eq!(
            reference.match_columns(
                &Schema::new(vec![field("b", DataType::Utf8)]),
                true,
                &type_name
            ),
            Err(vec!["`a` Int32 has been removed".to_string()])
        );
        Ok(())
    }

    #[test]
    fn test_columns_that_only_tell_rows_apart_can_change() -> TestResult {
        let table = Schema::new(vec![
            field("a", DataType::Int32),
            field("b", DataType::Int32),
        ]);
        let reference = ViewTableReference {
            required: vec![Some(ColumnUse::Whole), Some(ColumnUse::Identity)],
            ..reference(&table)
        };
        for current in [
            Schema::new(vec![
                field("b", DataType::Utf8),
                field("a", DataType::Int32),
            ]),
            Schema::new(vec![
                Field::new("b", DataType::Int64, false),
                field("a", DataType::Int32),
            ]),
        ] {
            assert_eq!(
                reference.match_columns(&current, true, &type_name),
                Ok(vec![Some(1), Some(0)]),
                "{current}"
            );
        }
        assert_eq!(
            reference.match_columns(
                &Schema::new(vec![field("a", DataType::Int32)]),
                true,
                &type_name
            ),
            Err(vec!["`b` Int32 has been removed".to_string()])
        );
        Ok(())
    }

    #[test]
    fn test_a_name_that_matches_several_columns_is_a_column_change() {
        let stored = Schema::new(vec![field("id", DataType::Int32)]);
        let current = Schema::new(vec![
            field("ID", DataType::Int32),
            field("Id", DataType::Int32),
        ]);
        assert_eq!(
            match_columns(&stored, &current, false),
            Err(vec![
                "`id` matches more than one field: [ID, Id]".to_string()
            ])
        );
        // A column that another name matches exactly is not a candidate.
        let stored = Schema::new(vec![
            field("a", DataType::Int32),
            field("A", DataType::Int32),
        ]);
        let current = Schema::new(vec![field("a", DataType::Int32)]);
        assert_eq!(
            match_columns(&stored, &current, false),
            Err(vec!["`A` Int32 has been removed".to_string()])
        );
    }

    #[test]
    fn test_columns_of_the_same_name_match_by_position_among_them() {
        let stored = Schema::new(vec![
            field("k", DataType::Int32),
            field("k", DataType::Utf8),
            field("v", DataType::Int32),
        ]);
        let current = Schema::new(vec![
            field("v", DataType::Int32),
            field("k", DataType::Int32),
            field("w", DataType::Int32),
            field("k", DataType::Utf8),
        ]);
        assert_eq!(match_columns(&stored, &current, true), Ok(vec![1, 3, 0]));
        let current = Schema::new(vec![
            field("k", DataType::Int32),
            field("v", DataType::Int32),
        ]);
        assert_eq!(
            match_columns(&stored, &current, true),
            Err(vec!["`k` Utf8 has been removed".to_string()])
        );
    }

    #[test]
    fn test_nested_field_changes_are_column_changes() {
        let fields = |fields: Vec<Field>| DataType::Struct(Fields::from(fields));
        let stored = fields(vec![
            field("a", DataType::Int32),
            field("b", DataType::Utf8),
        ]);
        let cases = [
            (
                fields(vec![field("a", DataType::Int32)]),
                vec!["`c`.`b` Utf8 has been removed"],
            ),
            (
                fields(vec![
                    field("a", DataType::Int32),
                    field("b", DataType::Utf8),
                    Field::new("d", DataType::Int64, false),
                ]),
                vec!["`c`.`d` Int64 NOT NULL has been added"],
            ),
            (
                fields(vec![
                    field("a", DataType::Int64),
                    field("b", DataType::Utf8),
                ]),
                vec!["`c`.`a` type has changed from Int32 to Int64"],
            ),
            (
                fields(vec![
                    Field::new("a", DataType::Int32, false),
                    field("b", DataType::Utf8),
                ]),
                vec![],
            ),
            (DataType::Int32, vec!["`c` type has changed from"]),
        ];
        for (current, expected) in cases {
            for wrap in [
                (|x: DataType| x) as fn(DataType) -> DataType,
                |x| list(field("item", x)),
                |x| map(Field::new("key", DataType::Utf8, false), field("value", x)),
            ] {
                let stored = Schema::new(vec![field("c", wrap(stored.clone()))]);
                let current = Schema::new(vec![field("c", wrap(current.clone()))]);
                let changes = match_columns(&stored, &current, true)
                    .err()
                    .unwrap_or_default();
                assert_eq!(changes.len(), expected.len(), "{changes:?}");
                for (change, expected) in changes.iter().zip(&expected) {
                    let expected = expected.strip_prefix("`c`").unwrap_or(expected);
                    assert!(change.starts_with("`c`"), "{change}");
                    assert!(change.contains(expected), "{change}");
                }
            }
        }
        // The nullability of the elements of a list and of the values of a map.
        let stored = Schema::new(vec![field("c", list(field("item", DataType::Int32)))]);
        let current = Schema::new(vec![field(
            "c",
            list(Field::new("item", DataType::Int32, false)),
        )]);
        assert_eq!(match_columns(&stored, &current, true), Ok(vec![0]));
        let value = |nullable| {
            Schema::new(vec![field(
                "c",
                map(
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Int32, nullable),
                ),
            )])
        };
        assert_eq!(
            match_columns(&value(false), &value(true), true),
            Ok(vec![0])
        );
    }

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    /// How a plan reads the columns of a source, by position, or `None` if it reads all
    /// of them.
    type Uses = Option<Vec<(usize, ColumnUse)>>;

    /// A scan of a table `(a INT, b INT, c INT, s STRUCT<x INT, y INT>)` that a view obtains
    /// again, under the projection that renames its columns after `name` (`t_a` for `t`),
    /// as the resolver reads a table. Returns the plan and the address of the source.
    fn read_table(
        reads: &mut ViewDefinitionReads,
        name: &str,
    ) -> Result<(LogicalPlan, usize), Box<dyn std::error::Error>> {
        let fields = Fields::from(vec![
            field("x", DataType::Int32),
            field("y", DataType::Int32),
        ]);
        let schema = Arc::new(Schema::new(vec![
            field("a", DataType::Int32),
            field("b", DataType::Int32),
            field("c", DataType::Int32),
            field("s", DataType::Struct(fields)),
        ]));
        let source = provider_as_source(Arc::new(MemTable::try_new(schema, vec![vec![]])?));
        reads.record_table_origin(&source, origin());
        let address = table_source_address(&source);
        let plan = LogicalPlanBuilder::scan(name, source, None)?
            .project(["a", "b", "c", "s"].map(|x| col(x).alias(format!("{name}_{x}"))))?
            .build()?;
        Ok((plan, address))
    }

    /// Returns the columns that the plan reads of the source, by position, or `None` if it
    /// reads all of them.
    fn uses(
        plan: &LogicalPlan,
        reads: &ViewDefinitionReads,
        address: usize,
    ) -> Result<Uses, Box<dyn std::error::Error>> {
        let required = required_columns(plan, reads)?;
        Ok(required.get(&address).map(|columns| {
            let mut columns = columns
                .iter()
                .map(|(i, used)| (*i, used.clone()))
                .collect::<Vec<_>>();
            columns.sort_by_key(|(i, _)| *i);
            columns
        }))
    }

    fn whole(positions: &[usize]) -> Uses {
        Some(positions.iter().map(|i| (*i, ColumnUse::Whole)).collect())
    }

    fn get(expr: Expr, name: &str) -> Expr {
        get_field().call(vec![expr, lit(name)])
    }

    #[test]
    fn test_projections_read_only_the_columns_of_the_outputs_that_are_read() -> TestResult {
        let mut reads = ViewDefinitionReads::default();
        let (t, address) = read_table(&mut reads, "t")?;
        // `SELECT a FROM (SELECT * FROM t)`.
        let plan = LogicalPlanBuilder::from(t.clone())
            .project(vec![col("t_a"), col("t_b"), col("t_c"), col("t_s")])?
            .project(vec![col("t_a").alias("a")])?
            .build()?;
        assert_eq!(uses(&plan, &reads, address)?, whole(&[0]));
        // An output that is computed and not read does not read its columns, also above
        // an operator that only passes its input on.
        let plan = LogicalPlanBuilder::from(t.clone())
            .filter(col("t_c").gt(lit(0)))?
            .project(vec![col("t_a"), (col("t_b") + lit(1)).alias("y")])?
            .project(vec![col("t_a")])?
            .build()?;
        assert_eq!(uses(&plan, &reads, address)?, whole(&[0, 2]));
        // The computed output is read.
        let plan = LogicalPlanBuilder::from(t)
            .filter(col("t_c").gt(lit(0)))?
            .project(vec![col("t_a"), (col("t_b") + lit(1)).alias("y")])?
            .project(vec![col("y")])?
            .build()?;
        assert_eq!(uses(&plan, &reads, address)?, whole(&[1, 2]));
        Ok(())
    }

    #[test]
    fn test_columns_that_functions_fold_away_are_read() -> TestResult {
        let mut reads = ViewDefinitionReads::default();
        let (t, address) = read_table(&mut reads, "t")?;
        // `SELECT a, typeof(b) AS tb FROM t`, where `typeof(b)` is a literal.
        reads.folded_columns.insert("t_b".to_string());
        let plan = LogicalPlanBuilder::from(t)
            .project(vec![col("t_a"), lit("int").alias("tb")])?
            .build()?;
        assert_eq!(uses(&plan, &reads, address)?, whole(&[0, 1]));
        Ok(())
    }

    #[test]
    fn test_operators_read_the_columns_of_their_expressions() -> TestResult {
        let mut reads = ViewDefinitionReads::default();
        let (t, address) = read_table(&mut reads, "t")?;
        let plan = LogicalPlanBuilder::from(t.clone())
            .aggregate(vec![col("t_a")], vec![sum(col("t_b")).alias("total")])?
            .project(vec![col("t_a")])?
            .build()?;
        assert_eq!(uses(&plan, &reads, address)?, whole(&[0, 1]));
        let plan = LogicalPlanBuilder::from(t.clone())
            .sort(vec![col("t_c").sort(true, false)])?
            .limit(0, Some(1))?
            .project(vec![col("t_a")])?
            .build()?;
        assert_eq!(uses(&plan, &reads, address)?, whole(&[0, 2]));
        let (u, other) = read_table(&mut reads, "u")?;
        let plan = LogicalPlanBuilder::from(t)
            .join_on(u, JoinType::Inner, vec![col("t_c").eq(col("u_b"))])?
            .project(vec![col("t_a")])?
            .build()?;
        assert_eq!(uses(&plan, &reads, address)?, whole(&[0, 2]));
        assert_eq!(uses(&plan, &reads, other)?, whole(&[1]));
        Ok(())
    }

    #[test]
    fn test_rows_are_told_apart_by_distinct_and_grouping_columns() -> TestResult {
        let mut reads = ViewDefinitionReads::default();
        let (t, address) = read_table(&mut reads, "t")?;
        let identity = |positions: &[usize]| {
            positions
                .iter()
                .map(|i| (*i, ColumnUse::Identity))
                .collect::<Vec<_>>()
        };
        let plan = LogicalPlanBuilder::from(t.clone())
            .distinct()?
            .project(vec![col("t_a")])?
            .build()?;
        let mut expected = vec![(0, ColumnUse::Whole)];
        expected.extend(identity(&[1, 2, 3]));
        assert_eq!(uses(&plan, &reads, address)?, Some(expected));
        let plan = LogicalPlanBuilder::from(t.clone())
            .aggregate(
                vec![col("t_a"), col("t_c")],
                vec![sum(col("t_b")).alias("total")],
            )?
            .project(vec![col("t_a"), col("total")])?
            .build()?;
        assert_eq!(
            uses(&plan, &reads, address)?,
            Some(vec![
                (0, ColumnUse::Whole),
                (1, ColumnUse::Whole),
                (2, ColumnUse::Identity)
            ])
        );
        // A struct whose fields are read, and that tells rows apart, is read whole.
        let plan = LogicalPlanBuilder::from(t.clone())
            .filter(get(col("t_s"), "x").gt(lit(0)))?
            .distinct()?
            .project(vec![col("t_a")])?
            .build()?;
        let mut expected = vec![(0, ColumnUse::Whole)];
        expected.extend(identity(&[1, 2]));
        expected.push((3, ColumnUse::Whole));
        assert_eq!(uses(&plan, &reads, address)?, Some(expected));
        // `dropDuplicates(["a", "b"])` of a DataFrame.
        let columns = ["t_a", "t_b", "t_c", "t_s"].map(col).to_vec();
        let plan = LogicalPlanBuilder::from(t.clone())
            .distinct_on(vec![col("t_a"), col("t_b")], columns, None)?
            .project(vec![col("t_a")])?
            .build()?;
        let mut expected = vec![(0, ColumnUse::Whole)];
        expected.extend(identity(&[1]));
        assert_eq!(uses(&plan, &reads, address)?, Some(expected.clone()));
        // `SELECT h0 FROM (SELECT DISTINCT a AS h0, b AS h1 FROM t)`, with the aliases of
        // the resolver around the aliases of the query.
        let plan = LogicalPlanBuilder::from(t)
            .project(vec![
                col("t_a").alias("h0").alias("x0"),
                col("t_b").alias("h1").alias("x1"),
            ])?
            .distinct()?
            .project(vec![col("x0")])?
            .build()?;
        assert_eq!(uses(&plan, &reads, address)?, Some(expected));
        Ok(())
    }

    #[test]
    fn test_unions_read_the_columns_at_the_positions_that_are_read() -> TestResult {
        let mut reads = ViewDefinitionReads::default();
        let (t, address) = read_table(&mut reads, "t")?;
        let (u, other) = read_table(&mut reads, "u")?;
        let union = LogicalPlanBuilder::from(t)
            .project(vec![col("t_a"), col("t_b")])?
            .union(
                LogicalPlanBuilder::from(u)
                    .project(vec![col("u_c"), col("u_a")])?
                    .build()?,
            )?
            .build()?;
        let plan = LogicalPlanBuilder::from(union.clone())
            .project(vec![col("t_a")])?
            .build()?;
        assert_eq!(uses(&plan, &reads, address)?, whole(&[0]));
        assert_eq!(uses(&plan, &reads, other)?, whole(&[2]));
        // The inputs of a union keep the types of the columns that tell rows apart.
        let plan = LogicalPlanBuilder::from(union)
            .distinct()?
            .project(vec![col("t_a")])?
            .build()?;
        assert_eq!(uses(&plan, &reads, address)?, whole(&[0, 1]));
        assert_eq!(uses(&plan, &reads, other)?, whole(&[0, 2]));
        Ok(())
    }

    #[test]
    fn test_other_operators_read_all_the_columns_of_their_inputs() -> TestResult {
        let mut reads = ViewDefinitionReads::default();
        let (t, address) = read_table(&mut reads, "t")?;
        // `DISTINCT ON` that keeps the first row of each key by an order.
        let plan = LogicalPlanBuilder::from(t.clone())
            .distinct_on(
                vec![col("t_a")],
                vec![col("t_a")],
                Some(vec![
                    col("t_a").sort(true, false),
                    col("t_b").sort(true, false),
                ]),
            )?
            .build()?;
        assert_eq!(uses(&plan, &reads, address)?, whole(&[0, 1, 2, 3]));
        // A scan that no projection renames, or that has filters, is read whole.
        let LogicalPlan::Projection(projection) = &t else {
            return Err("expected a projection".into());
        };
        let scan = projection.input.as_ref().clone();
        let plan = LogicalPlanBuilder::from(scan.clone())
            .filter(col("a").gt(lit(0)))?
            .project(vec![col("a")])?
            .build()?;
        assert_eq!(uses(&plan, &reads, address)?, None);
        let LogicalPlan::TableScan(scan) = scan else {
            return Err("expected a scan".into());
        };
        let filtered = LogicalPlan::TableScan(TableScan {
            filters: vec![col("b").gt(lit(0))],
            ..scan
        });
        let plan = LogicalPlanBuilder::from(filtered)
            .project(vec![col("a").alias("t_a")])?
            .build()?;
        assert_eq!(uses(&plan, &reads, address)?, None);
        Ok(())
    }

    #[test]
    fn test_struct_fields_are_read_through_field_accesses() -> TestResult {
        let mut reads = ViewDefinitionReads::default();
        let (t, address) = read_table(&mut reads, "t")?;
        let x = ColumnUse::Fields(HashMap::from([("x".to_string(), ColumnUse::Whole)]));
        let plan = LogicalPlanBuilder::from(t.clone())
            .project(vec![get(col("t_s"), "x").alias("sx")])?
            .build()?;
        assert_eq!(uses(&plan, &reads, address)?, Some(vec![(3, x.clone())]));
        // The struct passes through a projection, and is read in a filter.
        let plan = LogicalPlanBuilder::from(t.clone())
            .filter(get(col("t_s"), "x").gt(lit(0)))?
            .project(vec![col("t_a"), col("t_s").alias("s2")])?
            .project(vec![col("t_a")])?
            .build()?;
        assert_eq!(
            uses(&plan, &reads, address)?,
            Some(vec![(0, ColumnUse::Whole), (3, x.clone())])
        );
        // The struct is read whole as an output.
        let plan = LogicalPlanBuilder::from(t)
            .filter(get(col("t_s"), "y").gt(lit(0)))?
            .project(vec![col("t_s")])?
            .build()?;
        assert_eq!(uses(&plan, &reads, address)?, whole(&[3]));
        Ok(())
    }

    #[test]
    fn test_outer_references_read_the_columns_of_the_outer_query() -> TestResult {
        let mut reads = ViewDefinitionReads::default();
        let (t, address) = read_table(&mut reads, "t")?;
        let (u, other) = read_table(&mut reads, "u")?;
        let outer = |name: &str| {
            Expr::OuterReferenceColumn(
                Arc::new(field(name, DataType::Int32)),
                Column::from_name(name),
            )
        };
        // `SELECT a FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.a = t.b)`.
        let subquery = LogicalPlanBuilder::from(u)
            .filter(col("u_a").eq(outer("t_b")))?
            .project(vec![lit(1)])?
            .build()?;
        let exists = Expr::Exists(Exists::new(
            Subquery {
                subquery: Arc::new(subquery),
                outer_ref_columns: vec![outer("t_b")],
                spans: Default::default(),
            },
            false,
        ));
        let plan = LogicalPlanBuilder::from(t.clone())
            .project(vec![col("t_a"), col("t_b")])?
            .filter(exists)?
            .project(vec![col("t_a")])?
            .build()?;
        assert_eq!(uses(&plan, &reads, address)?, whole(&[0, 1]));
        assert_eq!(uses(&plan, &reads, other)?, whole(&[0]));
        // A lateral join reads the columns of its left side in its right side, which is
        // visited after its left side.
        let left = LogicalPlanBuilder::from(t)
            .filter(lit(true))?
            .project(vec![col("t_a"), col("t_b").alias("b2")])?
            .build()?;
        let right = LogicalPlanBuilder::empty(true)
            .project(vec![outer("b2").alias("z")])?
            .build()?;
        let plan = LogicalPlanBuilder::from(left)
            .cross_join(right)?
            .project(vec![col("t_a"), col("z")])?
            .build()?;
        assert_eq!(uses(&plan, &reads, address)?, whole(&[0, 1]));
        Ok(())
    }

    #[test]
    fn test_outer_references_read_the_struct_fields_that_they_access() -> TestResult {
        let mut reads = ViewDefinitionReads::default();
        let (t, address) = read_table(&mut reads, "t")?;
        let (u, _) = read_table(&mut reads, "u")?;
        let fields = Fields::from(vec![
            field("x", DataType::Int32),
            field("y", DataType::Int32),
        ]);
        let outer = Expr::OuterReferenceColumn(
            Arc::new(field("t_s", DataType::Struct(fields))),
            Column::from_name("t_s"),
        );
        // `SELECT a FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.a = t.s.x)`.
        let subquery = LogicalPlanBuilder::from(u)
            .filter(col("u_a").eq(get(outer.clone(), "x")))?
            .project(vec![lit(1)])?
            .build()?;
        let exists = Expr::Exists(Exists::new(
            Subquery {
                subquery: Arc::new(subquery),
                outer_ref_columns: vec![outer],
                spans: Default::default(),
            },
            false,
        ));
        let plan = LogicalPlanBuilder::from(t)
            .filter(exists)?
            .project(vec![col("t_a")])?
            .build()?;
        assert_eq!(
            uses(&plan, &reads, address)?,
            Some(vec![
                (0, ColumnUse::Whole),
                (3, ColumnUse::path(&["x".to_string()]))
            ])
        );
        Ok(())
    }

    #[test]
    fn test_cte_definitions_are_read_whole() -> TestResult {
        let mut reads = ViewDefinitionReads::default();
        let (t, address) = read_table(&mut reads, "t")?;
        let body = LogicalPlanBuilder::from(t)
            .project(vec![col("t_a"), col("t_b")])?
            .build()?;
        let definition = Arc::new(CteDefinition::new(0, "c".into(), Arc::new(body), false));
        let plan = LogicalPlanBuilder::from(definition.scan()?)
            .project(vec![col("t_a")])?
            .build()?;
        assert_eq!(uses(&plan, &reads, address)?, whole(&[0, 1]));
        Ok(())
    }

    #[test]
    fn test_columns_that_are_no_longer_read_are_typed_nulls_of_each_type() -> TestResult {
        for data_type in data_types() {
            let stored = Schema::new(vec![
                field("a", DataType::Int32),
                field("c", data_type.clone()),
            ]);
            let table = Schema::new(vec![field("a", DataType::Int32)]);
            let reference = ViewTableReference {
                required: vec![Some(ColumnUse::Whole), None],
                ..reference(&stored)
            };
            let positions = reference
                .match_columns(&table, true, &type_name)
                .map_err(|e| e.join(", "))?;
            assert_eq!(positions, vec![Some(0), None], "{data_type}");
            let source =
                provider_as_source(Arc::new(MemTable::try_new(Arc::new(table), vec![vec![]])?));
            let scan = LogicalPlanBuilder::scan("t", source, Some(vec![0]))?.build()?;
            let LogicalPlan::TableScan(scan) = scan else {
                return Err("expected a scan".into());
            };
            let plan = reference.with_stored_output(scan, &positions, true)?;
            let output = plan.schema().field(1);
            assert_eq!(output.name(), "c");
            assert_eq!(output.data_type(), &data_type);
            assert!(output.is_nullable(), "{data_type}");
            let LogicalPlan::Projection(projection) = &plan else {
                return Err("expected a projection".into());
            };
            let mut null = None;
            projection.expr[1].apply(|e| {
                if let Expr::Literal(value, _) = e {
                    null = Some(value.clone());
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
            assert!(
                null.as_ref().is_some_and(ScalarValue::is_null),
                "{data_type}"
            );
        }
        Ok(())
    }
}
