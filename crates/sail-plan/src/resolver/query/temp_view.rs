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

use std::borrow::Cow;
use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_schema::extension::{EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY};
use datafusion::arrow::compute::can_cast_types;
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use datafusion::datasource::source_as_provider;
use datafusion_common::nested_struct::{
    requires_nested_struct_cast, validate_data_type_compatibility,
};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, JoinType, ScalarValue, TableReference};
use datafusion_expr::expr::{Cast, Exists, InSubquery, SetComparison};
use datafusion_expr::{
    Distinct, Expr, LogicalPlan, Projection, SubqueryAlias, TableScan, TableScanBuilder,
    TableSource,
};
use sail_catalog::error::CatalogError;
use sail_catalog::manager::CatalogManager;
use sail_catalog::manager::scope::ViewDefinitionScope;
use sail_catalog::manager::table::SessionCatalogTable;
use sail_catalog::utils::quote_name_if_needed;
use sail_common::spec::{
    SAIL_SPARK_INTERVAL_METADATA_KEY, SAIL_SPARK_UDT_METADATA_KEY, SPARK_METADATA_JSON_KEY,
};
use sail_common_datafusion::catalog::{TableKind, TableStatus, TemporaryViewDefinition};
use sail_common_datafusion::datasource::DataSourceRegistry;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::lakerelation::LakeRelationResolution;
use sail_common_datafusion::rename::table_provider::RenameTableProvider;
use sail_common_datafusion::session::plan::PlanService;
use sail_common_datafusion::variant::VARIANT_METADATA_MARKER_KEY;

use super::read::{CatalogTableRead, rename_source};
use crate::config::PlanConfig;
use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::state::{
    CatalogTableLookup, PlanResolverState, ViewDefinitionReads, table_source_address,
};
use crate::shared_cte::{CteReference, replace_scans, visit_scans};

/// Polls a future on a new stack when little of the stack of the thread remains, since a read
/// of views nested deeply resolves their definitions inside each other.
struct GrowStack<F>(Pin<Box<F>>);

impl<F: Future> Future for GrowStack<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let future = self.0.as_mut();
        stacker::maybe_grow(2 << 20, 16 << 20, || future.poll(cx))
    }
}

/// What the source of a table scan reads, for the sources that a temporary view obtains
/// again at each read.
#[derive(Debug)]
pub(in crate::resolver) enum TableOrigin {
    CatalogTable(CatalogTableOrigin),
    LakePath(LakePathOrigin),
}

impl TableOrigin {
    /// Whether the read matched the names of columns case-sensitively, as the session did
    /// when the query was resolved.
    fn read_case_sensitive(&self) -> bool {
        match self {
            Self::CatalogTable(origin) => origin.read_case_sensitive,
            Self::LakePath(origin) => origin.read_case_sensitive,
        }
    }

    /// Whether the read reads a version of the table, by a clause or a read option.
    fn travels_in_time(&self) -> bool {
        match self {
            Self::CatalogTable(origin) => origin.travels_in_time(),
            Self::LakePath(origin) => has_time_travel_option(&origin.options),
        }
    }
}

/// Whether the read options include a read option that Spark reads as time travel for a
/// table of any format.
fn has_time_travel_option(options: &[(String, String)]) -> bool {
    options.iter().any(|(key, _)| {
        key.eq_ignore_ascii_case("versionAsOf") || key.eq_ignore_ascii_case("timestampAsOf")
    })
}

/// A read of paths in a format. A temporary view stores it for a read of a Delta Lake or
/// Iceberg table by its path.
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
    /// instance of the catalog. It is set only while a temporary view is created, since
    /// only the origins that a view stores use it.
    pub session_table: Option<SessionCatalogTable>,
    /// The name of the relation of the table that is read, such as a metadata table.
    pub lake_relation: Option<String>,
    /// The format of the table, which the time travel options are written for.
    pub format: String,
    pub options: Vec<(String, String)>,
    pub temporal_options: Vec<(String, String)>,
    pub read_case_sensitive: bool,
    /// The ANSI mode and the session time zone that cast the values of the partitions that
    /// the catalog lists, as the session had them when the query was resolved.
    pub partition_value_cast: (bool, Arc<str>),
}

impl CatalogTableOrigin {
    /// Whether the read travels in time, by a clause or by a read option that Spark reads
    /// as time travel for a table of any format.
    fn travels_in_time(&self) -> bool {
        !self.temporal_options.is_empty() || has_time_travel_option(&self.options)
    }
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
    /// Whether the stored plan reads each column of the table.
    read: Vec<bool>,
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

    /// Returns the scan with a reference in the place of its source. `read` are the
    /// positions of the columns of the source that the stored plan reads, and every column
    /// of the scan is read without them.
    fn store(
        scan: TableScan,
        origin: &Arc<TableOrigin>,
        read: Option<&HashSet<usize>>,
    ) -> datafusion_common::Result<TableScan> {
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
        let mut reads = vec![false; table_schema.fields().len()];
        for &i in &columns {
            reads[i] = read.is_none_or(|read| read.contains(&i));
        }
        let reference = Arc::new(Self {
            schema,
            table: table_schema,
            columns,
            read: reads,
            origin: Arc::clone(origin),
        });
        // The stored scan has the nullability of the table when the view was created.
        TableScanBuilder::new(scan.table_name, reference)
            .with_filters(scan.filters)
            .with_fetch(scan.fetch)
            .build()
    }

    /// Returns, for each column that the scan read when the view was created, its position
    /// in the table and how the scan reads it, or how the columns that the stored plan reads
    /// changed. A column that the plan reads keeps its type (Spark's
    /// `SchemaUtils.validateSchemaCompatibility`), apart from the way its values are stored
    /// and the metadata that clients see. A column that the plan does not read has a
    /// position only if it did not change. A change of a column that the plan reads is a
    /// change, which the view resolves its query again for, since its plan would compute on
    /// other values than the query resolved again.
    fn fit_columns(
        &self,
        table: &Schema,
        case_sensitive: bool,
        type_name: &dyn Fn(&DataType) -> String,
    ) -> Result<Vec<Option<(usize, Read)>>, Vec<String>> {
        let mut fit = Fit::new(case_sensitive, type_name);
        let matches = match_fields(self.table.fields(), table.fields(), case_sensitive);
        let mut reads = Vec::with_capacity(matches.len());
        for ((field, found), &read) in self.table.fields().iter().zip(matches).zip(&self.read) {
            let read = if read {
                fit.column(field, found, table.fields())
            } else {
                // A column that the view does not read is read only if it did not change.
                let mut unread = Fit::new(case_sensitive, type_name);
                let read = unread.column(field, found, table.fields());
                read.filter(|_| unread.changes.is_empty())
            };
            reads.push(read);
        }
        if !fit.changes.is_empty() {
            return Err(fit.changes);
        }
        Ok(self.columns.iter().map(|&i| reads[i].take()).collect())
    }

    /// Returns the plan of a scan of the current table with the output of the stored scan:
    /// the names and the types that the view reads, and the metadata that clients see in
    /// its schema. `reads` are those of [`Self::fit_columns`], and the scan reads the
    /// columns that have a position, in their order.
    fn with_stored_output(
        &self,
        scan: TableScan,
        reads: &[Option<(usize, Read)>],
    ) -> PlanResult<LogicalPlan> {
        let source = scan.source.schema();
        let columns = self
            .schema
            .fields()
            .iter()
            .zip(reads)
            .map(|(stored, read)| {
                let current = read
                    .as_ref()
                    .map(|(position, read)| (source.field(*position), read));
                (stored, current)
            })
            .collect::<Vec<_>>();
        let same = columns.iter().all(|(stored, current)| {
            current.is_some_and(|(current, read)| {
                stored.name() == current.name() && read == &Read::Same
            })
        });
        if same {
            return Ok(LogicalPlan::TableScan(scan));
        }
        let qualifier = Some(scan.table_name.clone());
        let expressions = columns
            .into_iter()
            .map(|(stored, current)| {
                let Some((current, read)) = current else {
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
                if read == &Read::Same {
                    return Ok(column.alias_qualified(qualifier.clone(), stored.name()));
                }
                let target = cast_target(stored, current).with_nullable(current.is_nullable());
                let expression =
                    Expr::Cast(Cast::new_from_field(Box::new(column), Arc::new(target)));
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
    if unmatched.is_empty() {
        return matches;
    }
    let mut unclaimed: HashMap<String, Vec<usize>> = HashMap::new();
    for (i, field) in current.iter().enumerate() {
        if !claimed[i] {
            unclaimed
                .entry(field.name().to_lowercase())
                .or_default()
                .push(i);
        }
    }
    for (name, fields) in unmatched {
        let candidates = unclaimed.remove(&name).unwrap_or_default();
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

fn same_name(a: &str, b: &str, case_sensitive: bool) -> bool {
    a == b
        || (!case_sensitive
            && if a.is_ascii() && b.is_ascii() {
                a.eq_ignore_ascii_case(b)
            } else {
                a.to_lowercase() == b.to_lowercase()
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

/// How a scan reads the current value of a column or of a field of a struct as the value
/// that the stored plan reads.
#[derive(Debug, PartialEq)]
enum Read {
    /// The value as it is.
    Same,
    /// The value cast to the stored type: the values are stored in another way, such as
    /// `LargeUtf8` instead of `Utf8`, or the metadata that clients see differs.
    Cast,
}

/// Finds how the columns of a table now are read as the columns that a view was created
/// with, and the changes that keep the view from reading them. The nullability of a field
/// is part of its type, since the plan of the view was resolved for it. The metadata of
/// fields and the names of the fields that Spark does not name (the elements of lists and
/// the entries of maps) are not compared.
struct Fit<'a> {
    case_sensitive: bool,
    type_name: &'a dyn Fn(&DataType) -> String,
    changes: Vec<String>,
}

impl<'a> Fit<'a> {
    fn new(case_sensitive: bool, type_name: &'a dyn Fn(&DataType) -> String) -> Self {
        Self {
            case_sensitive,
            type_name,
            changes: vec![],
        }
    }

    /// Returns the position of the match of a stored column in the current columns and how
    /// the column is read (see [`Fit::field`]).
    fn column(
        &mut self,
        stored: &Field,
        found: FieldMatch,
        current: &Fields,
    ) -> Option<(usize, Read)> {
        self.field(stored, found, current, &[stored.name().clone()])
    }

    /// Returns the position of the match of a stored field in the current fields and how
    /// the field is read.
    fn field(
        &mut self,
        field: &Field,
        found: FieldMatch,
        current: &Fields,
        path: &[String],
    ) -> Option<(usize, Read)> {
        match found {
            FieldMatch::Found(position) => {
                Some((position, self.value(field, &current[position], path)))
            }
            FieldMatch::Missing => {
                let change = self.describe_field(path, field);
                self.changes.push(format!("{change} has been removed"));
                None
            }
            FieldMatch::Several(positions) => {
                let names = positions
                    .iter()
                    .map(|&i| current[i].name().as_str())
                    .collect::<Vec<_>>();
                self.changes.push(format!(
                    "{} matches more than one field: [{}]",
                    quote_names(path),
                    names.join(", ")
                ));
                None
            }
        }
    }

    /// Returns how a value of the current field is read as the stored field.
    fn value(&mut self, stored: &Field, current: &Field, path: &[String]) -> Read {
        if stored == current {
            return Read::Same;
        }
        if stored.is_nullable() != current.is_nullable() {
            let change = format!("{} nullability has changed", quote_names(path));
            self.changes.push(change);
        }
        self.logical_type_changes(stored, current, path);
        let read = self.data_type(stored.data_type(), current.data_type(), path);
        let key = SPARK_METADATA_JSON_KEY;
        if read == Read::Same && stored.metadata().get(key) != current.metadata().get(key) {
            Read::Cast
        } else {
            read
        }
    }

    /// Returns how a value of the current type is read as the stored type.
    fn data_type(&mut self, stored: &DataType, current: &DataType, path: &[String]) -> Read {
        use DataType::*;
        if stored == current {
            return Read::Same;
        }
        match (stored, current) {
            (Struct(stored), Struct(current)) => self.structs(stored, current, path),
            (
                List(a) | LargeList(a) | ListView(a) | LargeListView(a),
                List(b) | LargeList(b) | ListView(b) | LargeListView(b),
            ) => {
                let read = self.element(a, b, path, "element");
                if read == Read::Same
                    && std::mem::discriminant(stored) != std::mem::discriminant(current)
                {
                    Read::Cast
                } else {
                    read
                }
            }
            (FixedSizeList(a, n), FixedSizeList(b, m)) if n == m => {
                cast_if_changed(&[self.element(a, b, path, "element")])
            }
            (Map(a, x), Map(b, y)) if x == y => match (a.data_type(), b.data_type()) {
                (Struct(a), Struct(b)) if a.len() == 2 && b.len() == 2 => cast_if_changed(&[
                    self.element(&a[0], &b[0], path, "key"),
                    self.element(&a[1], &b[1], path, "value"),
                ]),
                _ => cast_if_changed(&[self.element(a, b, path, "entries")]),
            },
            (Dictionary(x, a), Dictionary(y, b)) if x == y => {
                cast_if_changed(&[self.data_type(a, b, path)])
            }
            (RunEndEncoded(x, a), RunEndEncoded(y, b)) if x.data_type() == y.data_type() => {
                cast_if_changed(&[self.element(a, b, path, "values")])
            }
            (Union(a, x), Union(b, y))
                if x == y
                    && a.len() == b.len()
                    && a.iter()
                        .zip(b.iter())
                        .all(|((i, a), (j, b))| i == j && a.name() == b.name()) =>
            {
                let reads = a
                    .iter()
                    .zip(b.iter())
                    .map(|((_, a), (_, b))| self.element(a, b, path, a.name()))
                    .collect::<Vec<_>>();
                cast_if_changed(&reads)
            }
            _ if same_values(stored, current) => Read::Cast,
            _ => {
                let (mut from, mut to) = ((self.type_name)(stored), (self.type_name)(current));
                // Types that have the same name in Spark are told apart by their Arrow types.
                if from == to {
                    from = format!("{from} ({stored})");
                    to = format!("{to} ({current})");
                }
                self.changes.push(format!(
                    "{} type has changed from {from} to {to}",
                    quote_names(path)
                ));
                // The reads are not used once there are changes.
                Read::Same
            }
        }
    }

    /// Returns how a current struct is read as a stored struct, which keeps all its fields,
    /// in their order and with their names.
    fn structs(&mut self, stored: &Fields, current: &Fields, path: &[String]) -> Read {
        let matches = match_fields(stored, current, self.case_sensitive);
        let changes = self.changes.len();
        let mut same = stored.len() == current.len();
        // Whether the fields are the stored fields in their order and with their names.
        let mut layout = same;
        for (i, (field, found)) in stored.iter().zip(matches).enumerate() {
            let read = self.field(field, found, current, &child_path(path, field.name()));
            if let Some((position, read)) = &read {
                let kept = *position == i && current[*position].name() == field.name();
                layout &= kept;
                same &= kept && read == &Read::Same;
            }
        }
        let fold = |name: &str| match self.case_sensitive {
            true => name.to_string(),
            false => name.to_lowercase(),
        };
        let names = stored
            .iter()
            .map(|x| fold(x.name()))
            .collect::<HashSet<_>>();
        for field in current {
            if !names.contains(&fold(field.name())) {
                let path = child_path(path, field.name());
                let change = self.describe_field(&path, field);
                self.changes.push(format!("{change} has been added"));
            }
        }
        if same {
            Read::Same
        } else if layout {
            Read::Cast
        } else {
            // The plan of the view would compute on the fields as stored, such as in their
            // stored order and with their stored names.
            if self.changes.len() == changes {
                let change = format!("{} fields have changed", quote_names(path));
                self.changes.push(change);
            }
            Read::Same
        }
    }

    /// Returns how a value inside a list, a map or another such type is read, whole.
    fn element(&mut self, stored: &Field, current: &Field, path: &[String], name: &str) -> Read {
        self.value(stored, current, &child_path(path, name))
    }

    /// Adds the change of the logical type of a field whose Arrow type can be the same.
    fn logical_type_changes(&mut self, stored: &Field, current: &Field, path: &[String]) {
        let change = LOGICAL_TYPE_METADATA_KEYS.iter().find_map(|key| {
            let (a, b) = (stored.metadata().get(key), current.metadata().get(key));
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
                Some(value) => format!("{} {value}", (self.type_name)(field.data_type())),
                None => (self.type_name)(field.data_type()),
            };
            self.changes.push(format!(
                "{} type has changed from {} to {}",
                quote_names(path),
                describe(stored, a),
                describe(current, b)
            ));
        }
    }

    fn describe_field(&self, path: &[String], field: &Field) -> String {
        let null = if field.is_nullable() { "" } else { " NOT NULL" };
        format!(
            "{} {}{null}",
            quote_names(path),
            (self.type_name)(field.data_type())
        )
    }
}

/// Returns a cast if any of the values inside a type is read in another way.
fn cast_if_changed(reads: &[Read]) -> Read {
    if reads.iter().all(|x| x == &Read::Same) {
        Read::Same
    } else {
        Read::Cast
    }
}

fn child_path(path: &[String], name: &str) -> Vec<String> {
    let mut path = path.to_vec();
    path.push(name.to_string());
    path
}

/// Returns the field of a cast of a value of `current` to the stored field: the stored
/// type, unless the type cannot be cast to.
fn cast_target(stored: &Field, current: &Field) -> Field {
    if casts_by_name(current.data_type(), stored.data_type()) {
        stored.clone()
    } else {
        stored.clone().with_data_type(current.data_type().clone())
    }
}

/// Whether a value of one type can be cast to another, matching the fields of structs by
/// name and leaving out the fields that the target does not have, as DataFusion plans a
/// cast.
fn casts_by_name(from: &DataType, to: &DataType) -> bool {
    if requires_nested_struct_cast(from, to) {
        validate_data_type_compatibility("", from, to).is_ok()
    } else {
        can_cast_types(from, to)
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
    /// it is. Also returns whether the view resolves its query at each read: the definition
    /// read a persistent view, a catalog table in several scans of its partitions, or a
    /// table that the plan does not scan, such as the table of a scalar subquery that
    /// `typeof` folded into a value.
    pub(in crate::resolver) fn store_view_plan(
        plan: LogicalPlan,
        reads: &ViewDefinitionReads,
    ) -> PlanResult<(LogicalPlan, bool)> {
        let read = read_columns(&plan, reads)?;
        let mut scanned = HashSet::new();
        visit_scans(&plan, &mut |scan| {
            scanned.insert(table_source_address(&scan.source));
            Ok(())
        })?;
        let resolves_at_each_read =
            reads.resolves_at_each_read || reads.sources().any(|x| !scanned.contains(&x));
        // The stored plan has its own CTE definitions.
        let plan = replace_scans(
            plan,
            &|scan| reads.table_origin(&scan.source).is_some(),
            &mut |scan| {
                let Some(origin) = reads.table_origin(&scan.source) else {
                    return Ok(LogicalPlan::TableScan(scan));
                };
                let read = read.get(&table_source_address(&scan.source));
                let scan = ViewTableReference::store(scan, origin, read)?;
                Ok(LogicalPlan::TableScan(scan))
            },
        )?;
        Ok((plan, resolves_at_each_read))
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
        let definition = Self::stored_view_definition(view);
        // A view whose query read persistent views, or tables that its plan does not scan,
        // resolves its query again at each read, as a view over it does, since its stored
        // plan would read what the query read when the view was created. A request for the
        // schema gets the columns of the stored plan if the query no longer resolves.
        if let Some(definition) = definition.as_ref().filter(|x| x.resolves_at_each_read) {
            match Box::pin(self.resolve_view_definition(plan, view, definition, state)).await {
                Ok(Some(plan)) => return Ok(plan),
                Err(error) if !self.schema_only => return Err(error),
                Ok(None) if !self.schema_only => {
                    let columns = definition
                        .output_names
                        .iter()
                        .map(|x| quote_names(std::slice::from_ref(x)))
                        .collect::<Vec<_>>();
                    return Err(PlanError::AnalysisError(format!(
                        "[INCOMPATIBLE_VIEW_SCHEMA_CHANGE] The query of view {} no longer has \
                        the columns of the view: {}. Create the view again.",
                        view_name(view),
                        columns.join(", "),
                    )));
                }
                _ => {}
            }
        }
        let plan = plan.clone();
        let mut scans = HashMap::new();
        // Whether a scan has another schema than the scan that the view was created with,
        // which is the case when only the metadata of its columns or the constraints of
        // the table changed, or a column that the view does not read is now a null. The
        // nodes above the scan then compute their schemas again.
        let mut recompute_schemas = false;
        // Whether a table gained columns, which a star or a natural join of the query of the
        // view would read when the query is resolved again, as a direct query reads them.
        let mut gained_columns = false;
        let mut recorded = state.view_definition_mut().map(|reads| reads.clone());
        for scan in &references {
            let address = table_source_address(&scan.source);
            if scans.contains_key(&address) {
                continue;
            }
            let error = match self.bind_view_table_scan(scan, view, state).await {
                Ok(Ok((bound, gained))) => {
                    gained_columns |= gained;
                    recompute_schemas |= bound.schema() != &scan.projected_schema;
                    scans.insert(address, bound);
                    continue;
                }
                // An error of the read itself, such as of a table created again in another
                // format, does not make the view resolve its query again, which would read
                // the table in another way.
                Err(error) => error,
                Ok(Err(error)) => {
                    // A view that the query creates does not read the scans bound so far.
                    state.replace_view_definition(recorded.take());
                    if let Some(definition) = &definition
                        && let Some(plan) = self
                            .resolve_view_definition_if_possible(&plan, view, definition, state)
                            .await
                    {
                        return Ok(plan);
                    }
                    error
                }
            };
            // A request for the schema gets the columns of the stored plan.
            if !self.schema_only {
                return Err(error);
            }
            scans.clear();
            (gained_columns, recompute_schemas) = (false, false);
            break;
        }
        if gained_columns && let Some(definition) = &definition {
            // A view that the query creates reads the scans of the query resolved again, or
            // the scans bound so far if the stored plan remains since the query no longer
            // resolves.
            let bound = state.replace_view_definition(recorded.take());
            if let Some(plan) = self
                .resolve_view_definition_if_possible(&plan, view, definition, state)
                .await
            {
                return Ok(plan);
            }
            state.replace_view_definition(bound);
        }
        let mut bind = |scan: TableScan| {
            Ok(match scans.get(&table_source_address(&scan.source)) {
                Some(bound) => Transformed::yes(bound.clone()),
                None => Transformed::no(LogicalPlan::TableScan(scan)),
            })
        };
        // Without CTE sharing, the plan refers to no CTE definition once its references are
        // inlined, so that only its scans are bound.
        let plan = if self.share_ctes {
            plan
        } else {
            Self::inline_shared_cte_references(plan)?
        };
        self.renew_shared_cte_definitions(plan, &mut bind, recompute_schemas, state)
    }

    /// Returns the definition of a temporary view that a read resolves again when the plan of
    /// the view no longer fits the tables that it reads. A global temporary view resolves it
    /// in the catalogs of the session that created it, in any session.
    fn stored_view_definition(view: &TableStatus) -> Option<Arc<TemporaryViewDefinition>> {
        let (TableKind::TemporaryView {
            definition: Some(definition),
            ..
        }
        | TableKind::GlobalTemporaryView {
            definition: Some(definition),
            ..
        }) = &view.kind
        else {
            return None;
        };
        Some(Arc::clone(definition))
    }

    /// Returns the plan of a read of a temporary view for its definition resolved again (see
    /// [`Self::resolve_view_definition`]), or `None` if the definition no longer resolves.
    async fn resolve_view_definition_if_possible(
        &self,
        plan: &LogicalPlan,
        view: &TableStatus,
        definition: &Arc<TemporaryViewDefinition>,
        state: &mut PlanResolverState,
    ) -> Option<LogicalPlan> {
        Box::pin(self.resolve_view_definition(plan, view, definition, state))
            .await
            .ok()
            .flatten()
    }

    /// Returns the plan of a read of a temporary view whose plan no longer fits the tables
    /// that it reads, for the definition of the view. The query is resolved again as
    /// when the view was created (see [`ViewDefinitionScope`]), in a state that has only the
    /// parts of the state of this query that belong to the whole statement, and the view
    /// returns the outputs of the query that have the names of its outputs then, as Spark
    /// reads a temporary view. Returns `None` if the query no longer has those outputs.
    async fn resolve_view_definition(
        &self,
        plan: &LogicalPlan,
        view: &TableStatus,
        definition: &Arc<TemporaryViewDefinition>,
        state: &mut PlanResolverState,
    ) -> PlanResult<Option<LogicalPlan>> {
        let Ok(config) = Arc::clone(&definition.config).downcast::<PlanConfig>() else {
            return Ok(None);
        };
        let case_sensitive = config.case_sensitive;
        let resolver = PlanResolver {
            ctx: self.ctx,
            config,
            share_ctes: self.share_ctes,
            schema_only: self.schema_only,
        };
        let scope = Arc::new(ViewDefinitionScope::Resolve(Arc::clone(definition)));
        let mut query_state = PlanResolverState::new();
        state.swap_statement(&mut query_state);
        // The views that the definition reads can resolve their definitions again as well.
        let query = GrowStack(Box::pin(
            scope.run(resolver.resolve_query_plan(definition.query.clone(), &mut query_state)),
        ))
        .await;
        state.swap_statement(&mut query_state);
        let query = query?;
        let names = Self::get_field_names(query.schema(), state)?;
        let names_like = |names: &[String], name: &str| {
            names
                .iter()
                .enumerate()
                .filter(|(_, x)| same_name(x, name, case_sensitive))
                .map(|(i, _)| i)
                .collect::<Vec<_>>()
        };
        let outputs = definition.output_names.iter().zip(plan.schema().fields());
        let outputs = outputs
            .enumerate()
            .map(|(i, (name, field))| {
                // The output of the name at the position of the stored output among those
                // of its name, if the query has as many outputs of the name.
                let stored = names_like(&definition.output_names, name);
                let found = names_like(&names, name);
                if found.len() != stored.len() {
                    return None;
                }
                let ordinal = stored.iter().position(|x| *x == i)?;
                let j = *found.get(ordinal)?;
                let column = Column::from(query.schema().qualified_field(j));
                Some(Expr::Column(column).alias(field.name()))
            })
            .collect::<Option<Vec<_>>>();
        let Some(outputs) = outputs else {
            return Ok(None);
        };
        let plan = LogicalPlan::Projection(Projection::try_new(outputs, Arc::new(query))?);
        Ok(Some(LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
            Arc::new(plan),
            TableReference::bare(view.name.as_str()),
        )?)))
    }

    /// Returns the scan of a read of the view, for a scan of the stored plan of the view, and
    /// whether the table gained columns, or the error of the columns that changed.
    async fn bind_view_table_scan(
        &self,
        scan: &TableScan,
        view: &TableStatus,
        state: &mut PlanResolverState,
    ) -> PlanResult<PlanResult<(LogicalPlan, bool)>> {
        let Some(reference) = ViewTableReference::try_from_scan(scan) else {
            return Err(PlanError::internal("scan without a view table reference"));
        };
        let (source, table) = match reference.origin.as_ref() {
            TableOrigin::CatalogTable(origin) => {
                match self
                    .resolve_view_catalog_table_source(origin, view, state)
                    .await?
                {
                    Ok(source) => (source, origin.name.clone()),
                    Err(error) => return Ok(Err(error)),
                }
            }
            TableOrigin::LakePath(origin) => (
                self.create_path_source(origin, true, state).await?,
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
        // The columns are matched as the definition of the view matched them.
        let case_sensitive = reference.origin.read_case_sensitive();
        let reads = match reference.fit_columns(&table_schema, case_sensitive, &type_name) {
            Ok(reads) => reads,
            Err(changes) => {
                return Ok(Err(PlanError::AnalysisError(format!(
                    "[INCOMPATIBLE_COLUMN_CHANGES_AFTER_VIEW_WITH_PLAN_CREATION] View {} plan \
                    references table {} whose data columns changed since the view plan was \
                    initially captured.\nColumn changes:\n- {}\nThis indicates the table has \
                    evolved and the view based on the plan must be recreated.",
                    view_name(view),
                    quote_names(&table),
                    changes.join("\n- "),
                ))));
            }
        };
        // A read of a version of the table reads columns that do not change, so a column
        // that the current table has is not one that the version gained.
        let gained = !reference.origin.travels_in_time()
            && has_new_fields(
                reference.table.fields(),
                table_schema.fields(),
                case_sensitive,
            );
        let positions = reads
            .iter()
            .map(|read| read.as_ref().map(|(position, _)| *position))
            .collect::<Vec<_>>();
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
                let read = reference
                    .columns
                    .iter()
                    .zip(&positions)
                    .filter(|(i, _)| reference.read[**i])
                    .filter_map(|(_, position)| *position)
                    .collect();
                reads.view_reads.insert(table_source_address(&source), read);
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
        Ok(Ok((reference.with_stored_output(bound, &reads)?, gained)))
    }

    /// Returns the source of a read of a catalog table that a temporary view was created
    /// over, with the catalog table lookups and the table sources of the query, or the error
    /// that the table is not found, that its name is now the name of a view, or that its
    /// partitions are now read in several scans, after which the read resolves the query of
    /// the view again.
    async fn resolve_view_catalog_table_source(
        &self,
        origin: &CatalogTableOrigin,
        view: &TableStatus,
        state: &mut PlanResolverState,
    ) -> PlanResult<PlanResult<Arc<dyn TableSource>>> {
        let manager = self.ctx.extension::<CatalogManager>()?;
        // A table in a catalog of the session that created the view, which is not this
        // session, is read in that catalog. Any other table is read in the catalog of that
        // name in this session, as a direct read of the table.
        let session_table = match &origin.session_table {
            Some(table) if !manager.has_session_catalog_table(table)? => Some(table),
            _ => None,
        };
        let lookup = match session_table {
            Some(table) => Self::lookup_session_catalog_table(table, &origin.name, state).await,
            None => Self::lookup_catalog_object(&manager, origin.name.clone(), state).await,
        };
        let (status, resolved) = match lookup {
            Ok(lookup) => lookup,
            Err(CatalogError::NotFound(object, _)) => {
                return Ok(Err(CatalogError::NotFound(
                    object,
                    format!(
                        "[TABLE_OR_VIEW_NOT_FOUND] Table or view not found: {} (read by the \
                        temporary view {})",
                        quote_names(&origin.name),
                        view_name(view),
                    ),
                )
                .into()));
            }
            Err(error) => return Err(error.into()),
        };
        let TableKind::Table { format, .. } = &status.kind else {
            // A view with a definition reads the view of the name instead.
            return Ok(Err(PlanError::AnalysisError(format!(
                "[TABLE_OR_VIEW_NOT_FOUND] {} is now a view, and the temporary view {} reads it \
                as a table. Create the temporary view again.",
                quote_names(&origin.name),
                view_name(view),
            ))));
        };
        if origin.travels_in_time() && !format.eq_ignore_ascii_case(&origin.format) {
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
                    return Ok(Err(PlanError::AnalysisError(format!(
                        "[TABLE_OR_VIEW_NOT_FOUND] Table or view not found: {} (read by the \
                        temporary view {})",
                        quote_name_if_needed(name),
                        view_name(view),
                    ))));
                };
                Some(relation)
            }
            None => None,
        };
        let sources = self
            .resolve_catalog_table_sources(
                CatalogTableRead {
                    status,
                    resolved,
                    origin,
                    session_table,
                    lake_relation,
                },
                state,
            )
            .await?;
        // The stored plan reads the table in one scan, and the query of the view resolved
        // again reads a table in several scans of its partitions.
        match <[_; 1]>::try_from(sources) {
            Ok([source]) => Ok(Ok(source)),
            Err(_) => Ok(Err(PlanError::AnalysisError(format!(
                "the temporary view {} reads {} in one scan, and the partitions of the table \
                are now read in several scans. Create the temporary view again.",
                view_name(view),
                quote_names(&origin.name),
            )))),
        }
    }

    /// Looks up the table `name` in the catalog of another session, reusing an earlier
    /// lookup of the table in the query.
    async fn lookup_session_catalog_table(
        table: &SessionCatalogTable,
        name: &[String],
        state: &mut PlanResolverState,
    ) -> Result<CatalogTableLookup, CatalogError> {
        let session_catalog = Some(table.catalog_id());
        if let Some(lookup) = state.get_catalog_table_lookup(session_catalog, name) {
            return Ok(lookup);
        }
        let request = Self::catalog_table_lookup_request(name);
        let (status, resolved) = table.get_lakehouse_table(request).await?;
        let (status, resolved) =
            state.share_catalog_table(session_catalog, name.to_vec(), status, resolved);
        Ok((status, Some(resolved)))
    }
}

/// Whether `current` has a field, also in a struct, that `stored` does not have. A field
/// matches the first stored field of its name (see [`same_name`]).
fn has_new_fields(stored: &Fields, current: &Fields, case_sensitive: bool) -> bool {
    // A name matches the names that it is equal to, or that have its lowercase letters if
    // the letter case is ignored, as `same_name` matches them.
    fn key(name: &str, case_sensitive: bool) -> Cow<'_, str> {
        if case_sensitive {
            Cow::Borrowed(name)
        } else {
            Cow::Owned(name.to_lowercase())
        }
    }
    if stored == current {
        return false;
    }
    let mut first = HashMap::with_capacity(stored.len());
    for field in stored.iter() {
        first
            .entry(key(field.name(), case_sensitive))
            .or_insert(field);
    }
    current.iter().any(|field| {
        match (
            first.get(key(field.name(), case_sensitive).as_ref()),
            field.data_type(),
        ) {
            (None, _) => true,
            (Some(old), DataType::Struct(new)) => match old.data_type() {
                DataType::Struct(old) => has_new_fields(old, new, case_sensitive),
                _ => false,
            },
            _ => false,
        }
    })
}

/// Returns the quoted name of a temporary view.
fn view_name(view: &TableStatus) -> String {
    let mut name = view.database.clone();
    name.push(view.name.clone());
    quote_names(&name)
}

/// Returns the columns that the plan reads of each source that a temporary view obtains
/// again, by the address of the source and the positions of the columns in the source.
///
/// The names that the plan reads are those that decide which rows the plan returns, and
/// those that a name that the plan reads depends on. The outputs of the plan, the columns
/// that a function folded away (see `PlanResolverState::folded_columns`), and the columns
/// that the filters, the join conditions, the sort keys, the grouping keys and the other
/// expressions of the operators that decide the rows refer to are read. An output of a
/// projection, an aggregate or a window function depends on the columns that its expression
/// refers to, an output of a union on the columns at its position in the inputs, an output
/// of an extension node on the input columns that the node needs for it, and a column of a
/// reference to a CTE definition on the output of the definition. A subquery in an
/// expression is read as the expression is. The other operators, such as `DISTINCT`, which
/// tells rows apart by all their columns, read all the columns of their inputs. The list of
/// operators below must be checked again when DataFusion adds or changes operators. A column of a
/// scan is read if its name is read, or the name of an output of the projection right
/// above the scan that depends on it, since the columns of a scan have the names of the
/// table, which the scans of other tables can have as well. The resolver gives each other
/// column a name of its own, so the names can only count more columns as read than the
/// plan reads, also for the plan of another view that the plan reads, whose names belong
/// to another resolver state. A scan of a table that such a view reads reads the columns
/// that the view reads of the table (see [`ViewDefinitionReads::view_reads`]), which are
/// all the columns that the plan of the view can read, since the plan reads the table
/// only through the view.
fn read_columns(
    plan: &LogicalPlan,
    reads: &ViewDefinitionReads,
) -> PlanResult<HashMap<usize, HashSet<usize>>> {
    let mut walk = ReadColumns {
        reads,
        names: plan
            .schema()
            .fields()
            .iter()
            .map(|x| x.name().clone())
            .chain(reads.folded_columns.iter().cloned())
            .collect(),
        depends: HashMap::new(),
        columns: HashMap::new(),
        outputs: HashMap::new(),
        definitions: HashSet::new(),
    };
    walk.plan(plan, None)?;
    let ReadColumns {
        mut names,
        mut depends,
        columns,
        mut outputs,
        ..
    } = walk;
    let mut pending = names.iter().cloned().collect::<Vec<_>>();
    while let Some(name) = pending.pop() {
        for input in depends.remove(&name).unwrap_or_default() {
            if names.insert(input.clone()) {
                pending.push(input);
            }
        }
    }
    let mut read: HashMap<usize, HashSet<usize>> = HashMap::new();
    // The scans of the tables that other views read read what those views read.
    let scans = columns
        .into_iter()
        .filter(|(address, _)| !reads.view_reads.contains_key(address));
    for (address, columns) in scans {
        let passed = outputs.remove(&address).unwrap_or_default();
        let positions = read.entry(address).or_default();
        for (name, position) in columns {
            let is_read = names.contains(&name)
                || passed
                    .iter()
                    .any(|(output, input)| input == &name && names.contains(output));
            if is_read {
                positions.insert(position);
            }
        }
    }
    for (address, positions) in &reads.view_reads {
        read.entry(*address).or_default().extend(positions);
    }
    Ok(read)
}

/// What a name of an expression is read for: the rows of the plan, an output of the plan
/// that depends on it, or an output of the projection right above a scan.
#[derive(Clone, Copy)]
enum ReadFor<'a> {
    Rows,
    Output(&'a str),
    ScanOutput(usize, &'a str),
}

impl<'a> ReadFor<'a> {
    /// The rows of the plan, or of a subquery in an expression of the output `read_by`.
    fn rows(read_by: Option<&'a str>) -> Self {
        read_by.map_or(Self::Rows, Self::Output)
    }
}

/// The names that a plan reads, while [`read_columns`] walks the plan.
struct ReadColumns<'a> {
    reads: &'a ViewDefinitionReads,
    /// The names that are read.
    names: HashSet<String>,
    /// The names that each name depends on.
    depends: HashMap<String, Vec<String>>,
    /// The name and the position in the source of each column of the scans.
    columns: HashMap<usize, Vec<(String, usize)>>,
    /// The outputs of the projections right above the scans and the columns of the scans
    /// that they depend on.
    outputs: HashMap<usize, Vec<(String, String)>>,
    /// The CTE definitions that the walk visited.
    definitions: HashSet<usize>,
}

impl ReadColumns<'_> {
    /// Walks the plan, whose names that decide its rows are read always, or if the output
    /// `read_by` is read, for the plan of a subquery in an expression of that output.
    fn plan(&mut self, plan: &LogicalPlan, read_by: Option<&str>) -> datafusion_common::Result<()> {
        plan.apply(|node| {
            self.node(node, read_by)?;
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(())
    }

    fn read(&mut self, name: String, read_for: ReadFor<'_>) {
        match read_for {
            ReadFor::Rows => {
                self.names.insert(name);
            }
            ReadFor::Output(output) => self.depend(output, [name]),
            ReadFor::ScanOutput(address, output) => self
                .outputs
                .entry(address)
                .or_default()
                .push((output.to_string(), name)),
        }
    }

    /// Records the columns that the expression refers to, and the subqueries that it reads.
    fn expr(&mut self, expr: &Expr, read_for: ReadFor<'_>) -> datafusion_common::Result<()> {
        // The other names that a subquery reads are read for the same output.
        let subquery_for = match read_for {
            ReadFor::Rows => None,
            ReadFor::Output(output) | ReadFor::ScanOutput(_, output) => Some(output),
        };
        expr.apply(|e| {
            match e {
                Expr::Column(column) => self.read(column.name.clone(), read_for),
                Expr::OuterReferenceColumn(_, column) => {
                    self.read(column.name.clone(), ReadFor::rows(subquery_for));
                }
                // The expression reads the values of the subquery.
                Expr::ScalarSubquery(subquery)
                | Expr::InSubquery(InSubquery { subquery, .. })
                | Expr::SetComparison(SetComparison { subquery, .. }) => {
                    for name in output_names(&subquery.subquery) {
                        self.read(name, ReadFor::rows(subquery_for));
                    }
                    self.plan(&subquery.subquery, subquery_for)?;
                }
                Expr::Exists(Exists { subquery, .. }) => {
                    self.plan(&subquery.subquery, subquery_for)?;
                }
                _ => {}
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(())
    }

    /// Records that the output depends on the input names.
    fn depend(&mut self, output: &str, inputs: impl IntoIterator<Item = String>) {
        self.depends
            .entry(output.to_string())
            .or_default()
            .extend(inputs);
    }

    fn node(&mut self, node: &LogicalPlan, read_by: Option<&str>) -> datafusion_common::Result<()> {
        let rows = ReadFor::rows(read_by);
        match node {
            LogicalPlan::TableScan(scan) => {
                if let Some(reference) = CteReference::try_from_scan(scan) {
                    let definition = reference.definition();
                    let outputs = definition.resolved().schema();
                    for (i, field) in scan.projected_schema.fields().iter().enumerate() {
                        let position = scan.projection.as_ref().map_or(i, |p| p[i]);
                        let input = outputs.field(position).name().clone();
                        self.depend(field.name(), [input]);
                    }
                    if self.definitions.insert(Arc::as_ptr(definition) as usize) {
                        self.plan(definition.resolved(), None)?;
                    }
                } else if self.reads.table_origin(&scan.source).is_some() {
                    let fields = scan.projected_schema.fields().iter().enumerate();
                    self.columns
                        .entry(table_source_address(&scan.source))
                        .or_default()
                        .extend(fields.map(|(i, field)| {
                            let position = scan.projection.as_ref().map_or(i, |p| p[i]);
                            (field.name().clone(), position)
                        }));
                }
                for filter in &scan.filters {
                    self.expr(filter, rows)?;
                }
            }
            LogicalPlan::Projection(projection) => {
                let scan = match projection.input.as_ref() {
                    LogicalPlan::TableScan(scan)
                        if self.reads.table_origin(&scan.source).is_some() =>
                    {
                        Some(table_source_address(&scan.source))
                    }
                    _ => None,
                };
                for (expr, field) in projection.expr.iter().zip(projection.schema.fields()) {
                    let read_for = match scan {
                        Some(address) => ReadFor::ScanOutput(address, field.name()),
                        None => ReadFor::Output(field.name()),
                    };
                    self.expr(expr, read_for)?;
                }
            }
            LogicalPlan::Aggregate(aggregate) => {
                for expr in &aggregate.group_expr {
                    self.expr(expr, rows)?;
                }
                let fields = aggregate.schema.fields();
                let first = fields.len() - aggregate.aggr_expr.len();
                for (expr, field) in aggregate.aggr_expr.iter().zip(&fields[first..]) {
                    self.expr(expr, ReadFor::Output(field.name()))?;
                }
            }
            LogicalPlan::Window(window) => {
                let fields = window.schema.fields();
                let first = fields.len() - window.window_expr.len();
                for (expr, field) in window.window_expr.iter().zip(&fields[first..]) {
                    self.expr(expr, ReadFor::Output(field.name()))?;
                }
            }
            LogicalPlan::Union(union) => {
                for (i, field) in union.schema.fields().iter().enumerate() {
                    let inputs = union
                        .inputs
                        .iter()
                        .map(|input| input.schema().field(i).name().clone())
                        .collect::<Vec<_>>();
                    self.depend(field.name(), inputs);
                }
            }
            LogicalPlan::Extension(extension) => {
                for expr in node.expressions() {
                    self.expr(&expr, rows)?;
                }
                let inputs = node.inputs();
                let fields = node.schema().fields();
                // The input columns that the node needs for itself, and for each output.
                let needed = (0..=fields.len())
                    .map(|j| {
                        let output = if j == 0 { vec![] } else { vec![j - 1] };
                        extension.node.necessary_children_exprs(&output)
                    })
                    .collect::<Option<Vec<_>>>();
                match needed {
                    Some(needed) => {
                        for (j, children) in needed.into_iter().enumerate() {
                            let names = children
                                .iter()
                                .zip(&inputs)
                                .flat_map(|(indices, input)| {
                                    let schema = input.schema();
                                    indices
                                        .iter()
                                        .map(|&i| schema.field(i).name().clone())
                                        .collect::<Vec<_>>()
                                })
                                .collect::<Vec<_>>();
                            if j == 0 {
                                for name in names {
                                    self.read(name, rows);
                                }
                            } else {
                                self.depend(fields[j - 1].name(), names);
                            }
                        }
                    }
                    None => {
                        for input in inputs {
                            for name in output_names(input) {
                                self.read(name, rows);
                            }
                        }
                    }
                }
            }
            // A mark join returns each row of its left input, and whether a row of its right
            // input matches it, as the join of an `EXISTS` subquery in a projection.
            LogicalPlan::Join(join) if join.join_type == JoinType::LeftMark => {
                if let Some(mark) = join.schema.fields().last() {
                    for expr in node.expressions() {
                        self.expr(&expr, ReadFor::Output(mark.name()))?;
                    }
                }
            }
            // These operators read the columns that their expressions refer to, and pass the
            // columns of their inputs on under their names.
            LogicalPlan::Filter(_)
            | LogicalPlan::Sort(_)
            | LogicalPlan::Join(_)
            | LogicalPlan::Repartition(_)
            | LogicalPlan::Subquery(_)
            | LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::Limit(_)
            | LogicalPlan::Values(_)
            | LogicalPlan::EmptyRelation(_)
            | LogicalPlan::Distinct(Distinct::On(_)) => {
                for expr in node.expressions() {
                    self.expr(&expr, rows)?;
                }
            }
            node => {
                for expr in node.expressions() {
                    self.expr(&expr, rows)?;
                }
                for input in node.inputs() {
                    for name in output_names(input) {
                        self.read(name, rows);
                    }
                }
            }
        }
        Ok(())
    }
}

/// Returns the names of the outputs of the plan.
fn output_names(plan: &LogicalPlan) -> Vec<String> {
    plan.schema()
        .fields()
        .iter()
        .map(|x| x.name().clone())
        .collect()
}

/// The source of a scan of a table that a temporary view reads again at each read, while
/// the definition of the view is resolved. It has the schema of the source that it wraps,
/// and an address of its own for each read of the definition (see [`table_source_address`]).
/// It is never executed, since the stored plan has a [`ViewTableReference`] in its place.
struct ViewDefinitionSource {
    inner: Arc<dyn TableSource>,
}

impl std::fmt::Debug for ViewDefinitionSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ViewDefinitionSource")
            .field("schema", &self.inner.schema())
            .finish_non_exhaustive()
    }
}

impl TableSource for ViewDefinitionSource {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

/// Returns the plan with the source that each [`ViewDefinitionSource`] wraps in its place,
/// for a plan that the resolver executes while the definition of a temporary view is
/// resolved, such as the plan that finds the values of a pivot.
pub(in crate::resolver) fn executable_plan(plan: LogicalPlan) -> PlanResult<LogicalPlan> {
    Ok(replace_scans(
        plan,
        &|scan| scan.source.downcast_ref::<ViewDefinitionSource>().is_some(),
        &mut |scan| {
            let Some(source) = scan.source.downcast_ref::<ViewDefinitionSource>() else {
                return Ok(LogicalPlan::TableScan(scan));
            };
            let scan = TableScanBuilder::new(scan.table_name, Arc::clone(&source.inner))
                .with_projection(scan.projection)
                .with_filters(scan.filters)
                .with_fetch(scan.fetch)
                .build()?;
            Ok(LogicalPlan::TableScan(scan))
        },
    )?)
}

pub(in crate::resolver) fn view_definition_source(
    source: Arc<dyn TableSource>,
) -> Arc<dyn TableSource> {
    Arc::new(ViewDefinitionSource { inner: source })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{
        DataType, Field, Fields, IntervalUnit, Schema, TimeUnit, UnionFields, UnionMode,
    };
    use datafusion::datasource::{MemTable, provider_as_source};
    use datafusion::functions_aggregate::expr_fn::{max, sum};
    use datafusion::functions_window::expr_fn::row_number;
    use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion_common::{Column, JoinType, ScalarValue};
    use datafusion_expr::expr::Exists;
    use datafusion_expr::expr_fn::{in_subquery, scalar_subquery};
    use datafusion_expr::{
        Expr, ExprFunctionExt, Extension, LogicalPlan, LogicalPlanBuilder, Subquery, TableScan,
        UserDefinedLogicalNode, col, lit,
    };
    use datafusion_functions::core::get_field;
    use sail_logical_plan::barrier::BarrierNode;
    use sail_logical_plan::repartition::{ExplicitRepartitionKind, ExplicitRepartitionNode};
    use sail_logical_plan::spark_partition_id::SparkPartitionIdNode;

    use super::{
        FieldMatch, Fit, Read, TableOrigin, ViewTableReference, has_new_fields, read_columns,
    };
    use crate::resolver::PlanResolver;
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
            partition_value_cast: (false, Arc::from("UTC")),
        }))
    }

    fn reference(schema: &Schema) -> ViewTableReference {
        ViewTableReference {
            schema: Arc::new(schema.clone()),
            table: Arc::new(schema.clone()),
            columns: (0..schema.fields().len()).collect(),
            read: vec![true; schema.fields().len()],
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
        Ok(positions(&reference(stored), table, case_sensitive)?
            .into_iter()
            .flatten()
            .collect())
    }

    /// Returns the position in the table of each column of the scan of the reference.
    fn positions(
        reference: &ViewTableReference,
        table: &Schema,
        case_sensitive: bool,
    ) -> Result<Vec<Option<usize>>, Vec<String>> {
        let reads = reference.fit_columns(table, case_sensitive, &type_name)?;
        Ok(reads
            .into_iter()
            .map(|read| read.map(|(position, _)| position))
            .collect())
    }

    /// Returns how a column of the field `current` is read as the field `stored`, if it did
    /// not change.
    fn read_of(stored: &Field, current: &Field) -> Result<Read, Vec<String>> {
        let mut fit = Fit::new(false, &type_name);
        let read = fit.value(stored, current, &[]);
        if fit.changes.is_empty() {
            Ok(read)
        } else {
            Err(fit.changes)
        }
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
                let (stored_field, current_field) =
                    (field("c", stored.clone()), field("c", current.clone()));
                let result = match_columns(
                    &Schema::new(vec![stored_field.clone()]),
                    &Schema::new(vec![current_field.clone()]),
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
    fn test_columns_whose_values_changed_type_are_column_changes() {
        use DataType::*;
        // Types whose values convert exactly to each other are other types as well.
        let pairs = [
            (Int32, Int64),
            (UInt8, Int16),
            (Int32, Float64),
            (Float32, Float64),
            (Decimal128(5, 2), Decimal128(10, 2)),
            (Int32, Decimal128(10, 0)),
            (
                Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                Timestamp(TimeUnit::Microsecond, Some("America/New_York".into())),
            ),
        ];
        for (current, stored) in pairs {
            let (current, stored) = (field("c", current), field("c", stored));
            assert!(read_of(&stored, &current).is_err(), "{current:?}");
            assert!(read_of(&current, &stored).is_err(), "{stored:?}");
        }
        // A change between types of the same name in Spark names the Arrow types.
        let spark_name = |_: &DataType| "TIMESTAMP".to_string();
        let mut fit = Fit::new(false, &spark_name);
        let unit = |unit| field("c", Timestamp(unit, Some("UTC".into())));
        fit.value(
            &unit(TimeUnit::Microsecond),
            &unit(TimeUnit::Nanosecond),
            &["c".to_string()],
        );
        let expected = format!(
            "`c` type has changed from TIMESTAMP ({}) to TIMESTAMP ({})",
            unit(TimeUnit::Microsecond).data_type(),
            unit(TimeUnit::Nanosecond).data_type()
        );
        assert_eq!(fit.changes, vec![expected]);
        // Values inside lists and structs.
        let wrap = |x: DataType| list(field("s", DataType::Struct(vec![field("a", x)].into())));
        assert!(read_of(&field("l", wrap(Int64)), &field("l", wrap(Int32))).is_err());
    }

    #[test]
    fn test_nullability_changes_of_each_type_are_column_changes() {
        for data_type in data_types() {
            let nullable = Schema::new(vec![field("c", data_type.clone())]);
            let required = Schema::new(vec![Field::new("c", data_type.clone(), false)]);
            let expected = Err(vec!["`c` nullability has changed".to_string()]);
            assert_eq!(
                match_columns(&nullable, &required, true),
                expected,
                "{data_type}"
            );
            assert_eq!(
                match_columns(&required, &nullable, true),
                expected,
                "{data_type}"
            );
        }
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
        assert_eq!(
            match_columns(&stored, &current, false),
            Err(vec!["`s` fields have changed".to_string()])
        );
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
        assert_eq!(read_of(&user("{}"), &user("{\"x\":1}")), Ok(Read::Cast));
        assert_eq!(
            read_of(
                &with_metadata(field("a", DataType::Int32)),
                &field("a", DataType::Int32)
            ),
            Ok(Read::Same)
        );
        let nested = |x: Field| field("s", DataType::Struct(vec![x].into()));
        assert_eq!(
            read_of(&nested(user("{}")), &nested(user("{\"x\":1}"))),
            Ok(Read::Cast)
        );
        let in_list = |x: Field| field("l", list(x));
        assert_eq!(
            read_of(&in_list(user("{}")), &in_list(user("{\"x\":1}"))),
            Ok(Read::Cast)
        );
    }

    #[test]
    fn test_new_fields_are_found_at_each_level_of_structs() {
        let inner = |names: &[&str]| {
            DataType::Struct(Fields::from(
                names
                    .iter()
                    .map(|x| field(x, DataType::Int32))
                    .collect::<Vec<_>>(),
            ))
        };
        let fields = |s: DataType, extra: &[&str]| {
            let mut fields = vec![field("a", DataType::Int32), field("s", s)];
            fields.extend(extra.iter().map(|x| field(x, DataType::Int32)));
            Fields::from(fields)
        };
        let stored = fields(inner(&["x", "y"]), &[]);
        let cases = [
            (fields(inner(&["x", "y"]), &[]), true, false),
            (fields(inner(&["x", "y"]), &["b"]), true, true),
            (fields(inner(&["x", "y", "z"]), &[]), true, true),
            (fields(inner(&["y", "x"]), &[]), true, false),
            (
                Fields::from(vec![
                    field("s", inner(&["x", "y"])),
                    field("a", DataType::Int32),
                ]),
                true,
                false,
            ),
            (fields(inner(&["X", "Y"]), &[]), true, true),
            (fields(inner(&["X", "Y"]), &[]), false, false),
            (
                Fields::from(vec![
                    field("A", DataType::Int32),
                    field("S", inner(&["x", "y"])),
                ]),
                false,
                false,
            ),
            // A list or a map of structs is not compared field by field.
            (
                fields(
                    DataType::List(Arc::new(field("item", inner(&["x", "y", "z"])))),
                    &[],
                ),
                true,
                false,
            ),
        ];
        for (current, case_sensitive, expected) in cases {
            assert_eq!(
                has_new_fields(&stored, &current, case_sensitive),
                expected,
                "{current:?}"
            );
        }
        // A field matches the first stored field of its name.
        let stored = Fields::from(vec![
            field("s", inner(&["x"])),
            field("s", inner(&["x", "y"])),
        ]);
        let current = Fields::from(vec![field("s", inner(&["x", "y"]))]);
        assert!(has_new_fields(&stored, &current, true));
        // Names that are not ASCII match by their lowercase letters, also the Kelvin sign.
        for (stored, current) in [("\u{e4}", "\u{c4}"), ("k", "\u{212a}")] {
            let stored = Fields::from(vec![field(stored, DataType::Int32)]);
            let current = Fields::from(vec![field(current, DataType::Int32)]);
            assert!(!has_new_fields(&stored, &current, false));
            assert!(has_new_fields(&stored, &current, true));
        }
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
            read: vec![true, false, false],
            ..reference(&table)
        };
        let current = Schema::new(vec![
            field("b", DataType::Utf8),
            field("a", DataType::Int32),
        ]);
        assert_eq!(
            positions(&reference, &current, true),
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
                positions(&reference, &current, true),
                Ok(vec![Some(0), None]),
                "{current}"
            );
        }
        // A column without nulls now is another column, which the view does not read.
        let current = Schema::new(vec![
            field("a", DataType::Int32),
            Field::new("b", DataType::Utf8, false),
        ]);
        assert_eq!(
            positions(&reference, &current, true),
            Ok(vec![Some(0), None])
        );
        assert_eq!(
            positions(
                &reference,
                &Schema::new(vec![field("b", DataType::Utf8)]),
                true
            ),
            Err(vec!["`a` Int32 has been removed".to_string()])
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
                vec!["`c`.`a` nullability has changed"],
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
        assert_eq!(
            match_columns(&stored, &current, true),
            Err(vec!["`c`.`element` nullability has changed".to_string()])
        );
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
            Err(vec!["`c`.`value` nullability has changed".to_string()])
        );
    }

    #[test]
    fn test_structs_read_whole_whose_fields_changed_are_column_changes() {
        let fields = |names: &[&str]| {
            DataType::Struct(names.iter().map(|x| field(x, DataType::Int32)).collect())
        };
        let wraps = [
            (|x: DataType| x) as fn(DataType) -> DataType,
            |x| DataType::List(Arc::new(field("item", x))),
            |x| DataType::LargeList(Arc::new(field("item", x))),
            |x| DataType::ListView(Arc::new(field("item", x))),
            |x| DataType::FixedSizeList(Arc::new(field("item", x)), 2),
            |x| DataType::Struct(vec![field("s", x)].into()),
            |x| map(Field::new("key", DataType::Utf8, false), field("value", x)),
        ];
        for wrap in wraps {
            let column = |names: &[&str]| field("c", wrap(fields(names)));
            let stored = column(&["a", "b"]);
            // A field added, the fields reordered, and a field renamed in letter case.
            for names in [&["a", "b", "d"][..], &["b", "a"], &["A", "b"]] {
                let mut fit = Fit::new(false, &type_name);
                let current = Fields::from(vec![column(names)]);
                fit.column(&stored, FieldMatch::Found(0), &current);
                assert_eq!(fit.changes.len(), 1, "{names:?}: {:?}", fit.changes);
            }
        }
    }

    type TestResult = Result<(), Box<dyn std::error::Error>>;

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

    /// Returns the positions of the columns that the plan reads of the source.
    fn read(
        plan: &LogicalPlan,
        reads: &ViewDefinitionReads,
        address: usize,
    ) -> Result<Vec<usize>, Box<dyn std::error::Error>> {
        let mut columns = read_columns(plan, reads)?
            .remove(&address)
            .unwrap_or_default()
            .into_iter()
            .collect::<Vec<_>>();
        columns.sort();
        Ok(columns)
    }

    fn get(expr: Expr, name: &str) -> Expr {
        get_field().call(vec![expr, lit(name)])
    }

    #[test]
    fn test_stored_plans_tell_whether_the_definition_read_tables_that_they_do_not_scan()
    -> TestResult {
        let mut reads = ViewDefinitionReads::default();
        let (t, _) = read_table(&mut reads, "t")?;
        let plan = LogicalPlanBuilder::from(t)
            .project(vec![col("t_a")])?
            .build()?;
        assert!(!PlanResolver::store_view_plan(plan.clone(), &reads)?.1);
        // The table of a scalar subquery that a function folded into a value.
        read_table(&mut reads, "u")?;
        assert!(PlanResolver::store_view_plan(plan, &reads)?.1);
        Ok(())
    }

    #[test]
    fn test_the_columns_that_expressions_refer_to_are_read() -> TestResult {
        let mut reads = ViewDefinitionReads::default();
        let (t, address) = read_table(&mut reads, "t")?;
        let plan = LogicalPlanBuilder::from(t.clone())
            .project(vec![col("t_a").alias("a")])?
            .build()?;
        assert_eq!(read(&plan, &reads, address)?, [0]);
        // An output that is computed and not read, and a struct field.
        let plan = LogicalPlanBuilder::from(t.clone())
            .filter(get(col("t_s"), "x").gt(lit(0)))?
            .project(vec![col("t_a"), (col("t_b") + lit(1)).alias("y")])?
            .project(vec![col("t_a")])?
            .build()?;
        assert_eq!(read(&plan, &reads, address)?, [0, 3]);
        let plan = LogicalPlanBuilder::from(t.clone())
            .project(vec![col("t_a"), (col("t_b") + lit(1)).alias("y")])?
            .build()?;
        assert_eq!(read(&plan, &reads, address)?, [0, 1]);
        // `SELECT a FROM (SELECT * FROM t)` and `SELECT a FROM (SELECT a, b FROM t) s`: the
        // projections pass the columns on, and only the column that is read is read.
        let plan = LogicalPlanBuilder::from(t.clone())
            .project(vec![col("t_a"), col("t_b"), col("t_c"), col("t_s")])?
            .project(vec![col("t_a")])?
            .build()?;
        assert_eq!(read(&plan, &reads, address)?, [0]);
        let plan = LogicalPlanBuilder::from(t)
            .project(vec![col("t_a").alias("a"), col("t_b").alias("b")])?
            .alias("s")?
            .project(vec![col("a")])?
            .build()?;
        assert_eq!(read(&plan, &reads, address)?, [0]);
        Ok(())
    }

    #[test]
    fn test_the_outputs_of_the_plan_are_read() -> TestResult {
        let mut reads = ViewDefinitionReads::default();
        let (t, address) = read_table(&mut reads, "t")?;
        let plan = LogicalPlanBuilder::from(t.clone())
            .filter(col("t_c").gt(lit(0)))?
            .build()?;
        assert_eq!(read(&plan, &reads, address)?, [0, 1, 2, 3]);
        // A scan that no projection renames, or that has filters.
        let LogicalPlan::Projection(projection) = &t else {
            return Err("expected a projection".into());
        };
        let LogicalPlan::TableScan(scan) = projection.input.as_ref().clone() else {
            return Err("expected a scan".into());
        };
        let plan = LogicalPlanBuilder::from(LogicalPlan::TableScan(scan.clone()))
            .filter(col("a").gt(lit(0)))?
            .project(vec![col("a")])?
            .build()?;
        assert_eq!(read(&plan, &reads, address)?, [0]);
        let filtered = LogicalPlan::TableScan(TableScan {
            filters: vec![col("b").gt(lit(0))],
            ..scan
        });
        let plan = LogicalPlanBuilder::from(filtered)
            .project(vec![col("a").alias("t_a")])?
            .build()?;
        assert_eq!(read(&plan, &reads, address)?, [0, 1]);
        Ok(())
    }

    #[test]
    fn test_subqueries_read_the_columns_that_they_refer_to_and_their_values() -> TestResult {
        let mut reads = ViewDefinitionReads::default();
        let (t, address) = read_table(&mut reads, "t")?;
        let (u, other) = read_table(&mut reads, "u")?;
        // `SELECT a FROM t WHERE b IN (SELECT c FROM u)`, where the values of the subquery
        // are read by their position.
        let values = LogicalPlanBuilder::from(u.clone())
            .project(vec![col("u_c").alias("v")])?
            .build()?;
        let plan = LogicalPlanBuilder::from(t.clone())
            .filter(in_subquery(col("t_b"), Arc::new(values)))?
            .project(vec![col("t_a")])?
            .build()?;
        assert_eq!(read(&plan, &reads, address)?, [0, 1]);
        assert_eq!(read(&plan, &reads, other)?, [2]);
        // A scalar subquery in an output that is not read.
        let value = LogicalPlanBuilder::from(u)
            .aggregate(Vec::<Expr>::new(), vec![sum(col("u_c")).alias("m")])?
            .build()?;
        let plan = LogicalPlanBuilder::from(t)
            .project(vec![
                col("t_a"),
                scalar_subquery(Arc::new(value)).alias("v"),
            ])?
            .project(vec![col("t_a")])?
            .build()?;
        assert_eq!(read(&plan, &reads, address)?, [0]);
        assert_eq!(read(&plan, &reads, other)?, Vec::<usize>::new());
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
        assert_eq!(read(&plan, &reads, address)?, [0, 1]);
        Ok(())
    }

    #[test]
    fn test_operators_read_the_columns_of_their_expressions() -> TestResult {
        let mut reads = ViewDefinitionReads::default();
        let (t, address) = read_table(&mut reads, "t")?;
        let plan = LogicalPlanBuilder::from(t.clone())
            .aggregate(vec![col("t_a")], vec![max(col("t_b")).alias("total")])?
            .project(vec![col("t_a")])?
            .build()?;
        assert_eq!(read(&plan, &reads, address)?, [0]);
        let plan = LogicalPlanBuilder::from(t.clone())
            .aggregate(vec![col("t_a")], vec![max(col("t_b")).alias("total")])?
            .build()?;
        assert_eq!(read(&plan, &reads, address)?, [0, 1]);
        let window = row_number()
            .partition_by(vec![col("t_b")])
            .order_by(vec![col("t_c").sort(true, false)])
            .build()?;
        let plan = LogicalPlanBuilder::from(t.clone())
            .window(vec![window.clone()])?
            .project(vec![col("t_a")])?
            .build()?;
        assert_eq!(read(&plan, &reads, address)?, [0]);
        let plan = LogicalPlanBuilder::from(t.clone())
            .window(vec![window])?
            .build()?;
        assert_eq!(read(&plan, &reads, address)?, [0, 1, 2, 3]);
        let plan = LogicalPlanBuilder::from(t.clone())
            .sort(vec![(-col("t_c")).sort(true, false)])?
            .limit(0, Some(1))?
            .project(vec![col("t_a")])?
            .build()?;
        assert_eq!(read(&plan, &reads, address)?, [0, 2]);
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
        assert_eq!(read(&plan, &reads, address)?, [0, 1]);
        let (u, other) = read_table(&mut reads, "u")?;
        let keys = (col("t_c") + lit(1)).eq(col("u_b"));
        let plan = LogicalPlanBuilder::from(t.clone())
            .join_on(u.clone(), JoinType::Inner, vec![keys])?
            .project(vec![col("t_a")])?
            .build()?;
        assert_eq!(read(&plan, &reads, address)?, [0, 2]);
        assert_eq!(read(&plan, &reads, other)?, [1]);
        // A mark join, as of an `EXISTS` subquery in a projection, returns all the rows of
        // its left input, and reads its condition for the mark.
        let mark = |columns: Vec<Expr>| {
            LogicalPlanBuilder::from(t.clone())
                .join_on(
                    u.clone(),
                    JoinType::LeftMark,
                    vec![col("t_b").eq(col("u_a"))],
                )?
                .project(columns)?
                .build()
        };
        let plan = mark(vec![col("t_a")])?;
        assert_eq!(read(&plan, &reads, address)?, [0]);
        assert_eq!(read(&plan, &reads, other)?, Vec::<usize>::new());
        let plan = mark(vec![col("t_a"), col("mark")])?;
        assert_eq!(read(&plan, &reads, address)?, [0, 1]);
        assert_eq!(read(&plan, &reads, other)?, [0]);
        Ok(())
    }

    #[test]
    fn test_operators_that_do_not_refer_to_the_columns_that_they_read_read_all_of_them()
    -> TestResult {
        let mut reads = ViewDefinitionReads::default();
        let (t, address) = read_table(&mut reads, "t")?;
        let (u, other) = read_table(&mut reads, "u")?;
        // A union reads the columns of its inputs by their positions.
        let plan = LogicalPlanBuilder::from(t.clone())
            .union(u.clone())?
            .project(vec![col("t_a")])?
            .build()?;
        assert_eq!(read(&plan, &reads, address)?, [0]);
        assert_eq!(read(&plan, &reads, other)?, [0]);
        // `DISTINCT` tells rows apart by all its columns.
        let plan = LogicalPlanBuilder::from(t.clone())
            .distinct()?
            .project(vec![col("t_a")])?
            .build()?;
        assert_eq!(read(&plan, &reads, address)?, [0, 1, 2, 3]);
        let plan = LogicalPlanBuilder::from(t.clone())
            .project(vec![col("t_a"), col("t_s")])?
            .unnest_column("t_s")?
            .project(vec![col("t_a")])?
            .build()?;
        assert_eq!(read(&plan, &reads, address)?, [0, 3]);
        let input = Arc::new(t);
        let extension = |node: Arc<dyn UserDefinedLogicalNode>| {
            LogicalPlanBuilder::from(LogicalPlan::Extension(Extension { node }))
                .project(vec![col("t_a")])?
                .build()
        };
        // An extension node reads the input columns that it needs for its outputs that are
        // read, or all of them if it does not tell which.
        let nodes = [
            (
                Arc::new(ExplicitRepartitionNode::new(
                    Arc::clone(&input),
                    Some(3),
                    ExplicitRepartitionKind::RoundRobin,
                )) as Arc<dyn UserDefinedLogicalNode>,
                vec![0],
            ),
            (
                Arc::new(SparkPartitionIdNode::try_new(
                    Arc::clone(&input),
                    "pid".to_string(),
                )?),
                vec![0],
            ),
            (
                Arc::new(BarrierNode::new(vec![], Arc::clone(&input))),
                vec![0, 1, 2, 3],
            ),
        ];
        for (node, expected) in nodes {
            assert_eq!(read(&extension(node)?, &reads, address)?, expected);
        }
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
            .filter(exists)?
            .project(vec![col("t_a")])?
            .build()?;
        assert_eq!(read(&plan, &reads, address)?, [0, 1]);
        assert_eq!(read(&plan, &reads, other)?, [0]);
        // A lateral join reads the columns of its left side in its right side.
        let right = LogicalPlanBuilder::empty(true)
            .project(vec![outer("t_c").alias("z")])?
            .build()?;
        let plan = LogicalPlanBuilder::from(t)
            .cross_join(right)?
            .project(vec![col("t_a"), col("z")])?
            .build()?;
        assert_eq!(read(&plan, &reads, address)?, [0, 2]);
        Ok(())
    }

    #[test]
    fn test_cte_definitions_and_inner_views_read_their_columns() -> TestResult {
        let mut reads = ViewDefinitionReads::default();
        let (t, address) = read_table(&mut reads, "t")?;
        let body = LogicalPlanBuilder::from(t.clone())
            .project(vec![col("t_a"), col("t_b")])?
            .build()?;
        let definition = Arc::new(CteDefinition::new(0, "c".into(), Arc::new(body), false));
        let plan = LogicalPlanBuilder::from(definition.scan()?)
            .project(vec![col("t_a")])?
            .build()?;
        assert_eq!(read(&plan, &reads, address)?, [0]);
        // A scan of a table that a view that the definition reads reads reads the columns
        // that the view reads of the table, whatever the names of the plan are.
        reads.view_reads.insert(address, [2].into_iter().collect());
        let plan = LogicalPlanBuilder::from(t)
            .project(vec![col("t_a"), col("t_b")])?
            .build()?;
        assert_eq!(read(&plan, &reads, address)?, [2]);
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
                read: vec![true, false],
                ..reference(&stored)
            };
            let reads = reference
                .fit_columns(&table, true, &type_name)
                .map_err(|e| e.join(", "))?;
            assert_eq!(reads, vec![Some((0, Read::Same)), None], "{data_type}");
            let source =
                provider_as_source(Arc::new(MemTable::try_new(Arc::new(table), vec![vec![]])?));
            let scan = LogicalPlanBuilder::scan("t", source, Some(vec![0]))?.build()?;
            let LogicalPlan::TableScan(scan) = scan else {
                return Err("expected a scan".into());
            };
            let plan = reference.with_stored_output(scan, &reads)?;
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
