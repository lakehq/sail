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
use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_schema::extension::{EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY};
use datafusion::arrow::compute::can_cast_types;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields, Schema, SchemaRef};
use datafusion::datasource::source_as_provider;
use datafusion::functions_aggregate::average::Avg;
use datafusion::functions_aggregate::count::Count;
use datafusion::functions_aggregate::sum::Sum;
use datafusion_common::nested_struct::{
    requires_nested_struct_cast, validate_data_type_compatibility,
};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, ScalarValue, TableReference};
use datafusion_expr::expr::{
    AggregateFunction, AggregateFunctionParams, BinaryExpr, Cast, InList, InSubquery, SetComparison,
};
use datafusion_expr::{
    Distinct, Expr, LogicalPlan, Projection, SubqueryAlias, TableScan, TableScanBuilder,
    TableSource, UserDefinedLogicalNode, lit, when,
};
use datafusion_functions::core::getfield::GetFieldFunc;
use datafusion_functions::core::named_struct::NamedStructFunc;
use datafusion_functions::core::{get_field, named_struct};
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
use sail_function::scalar::r#struct::struct_from_fields::StructFromFields;

use super::read::{CatalogTableRead, rename_source};
use crate::config::PlanConfig;
use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::expression::is_comparison;
use crate::resolver::state::{
    CatalogTableLookup, PlanResolverState, ViewDefinitionReads, table_source_address,
};
use crate::shared_cte::{CteReference, replace_scans, visit_nodes, visit_scans};

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
}

impl CatalogTableOrigin {
    /// Whether the read travels in time, by a clause or by a read option that Spark reads
    /// as time travel for a table of any format.
    fn travels_in_time(&self) -> bool {
        !self.temporal_options.is_empty()
            || self.options.iter().any(|(key, _)| {
                key.eq_ignore_ascii_case("versionAsOf") || key.eq_ignore_ascii_case("timestampAsOf")
            })
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
        TableScanBuilder::new(scan.table_name, reference)
            .with_filters(scan.filters)
            .with_fetch(scan.fetch)
            .build()
    }

    /// Returns, for each column that the scan read when the view was created, its position
    /// in the table and how the scan reads it, or how the columns that the stored plan reads
    /// changed. A column that the plan reads keeps its type (Spark's
    /// `SchemaUtils.validateSchemaCompatibility`) or has a type whose values convert exactly
    /// to it, or keeps only the fields that it reads for a struct, or only exists for a
    /// column that only tells rows apart or orders them, or
    /// only stays a signed integer for a column that is only an operand (see
    /// [`ColumnUse::Operand`]). A column that the plan does not read has a position only if
    /// it did not change. A column whose values changed otherwise is a change, which the
    /// view resolves its query again for, since its plan would compute on other values than
    /// the query resolved again.
    fn fit_columns(
        &self,
        table: &Schema,
        case_sensitive: bool,
        type_name: &dyn Fn(&DataType) -> String,
    ) -> Result<Vec<Option<(usize, Read)>>, Vec<String>> {
        let mut fit = Fit::new(case_sensitive, type_name);
        let matches = match_fields(self.table.fields(), table.fields(), case_sensitive);
        let mut reads = Vec::with_capacity(matches.len());
        for ((field, found), used) in self.table.fields().iter().zip(matches).zip(&self.required) {
            let read = match (used, found) {
                (Some(ColumnUse::Identity), FieldMatch::Found(position)) => {
                    Some((position, Read::Same))
                }
                (Some(ColumnUse::Operand), FieldMatch::Found(position))
                    if is_signed_integer(field) && is_signed_integer(table.field(position)) =>
                {
                    Some((position, Read::Same))
                }
                (Some(used), found) => fit.column(field, found, table.fields(), used.fields()),
                // A column that the view does not read is read only if it did not change.
                (None, found) => {
                    let mut unread = Fit::new(case_sensitive, type_name);
                    let read = unread.column(field, found, table.fields(), None);
                    read.filter(|_| unread.changes.is_empty())
                }
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
                let target = match read {
                    Read::Same => {
                        return Ok(column.alias_qualified(qualifier.clone(), stored.name()));
                    }
                    Read::Cast => cast_target(stored, current),
                    Read::Struct(_) => nullable_field(stored),
                };
                let expression = Expr::Cast(Cast::new_from_field(
                    Box::new(read_value(column, stored, current, read)?),
                    Arc::new(target.with_nullable(current.is_nullable())),
                ));
                Ok(expression.alias_qualified(qualifier.clone(), stored.name()))
            })
            .collect::<PlanResult<Vec<_>>>()?;
        Ok(LogicalPlan::Projection(Projection::try_new(
            expressions,
            Arc::new(LogicalPlan::TableScan(scan)),
        )?))
    }
}

/// Whether a field has one of the signed integer types, which the resolver plans the same way
/// for an operand (see [`ColumnUse::Operand`]). An integer of another logical type, such as a
/// user-defined type or an extension type, is not one of them.
fn is_signed_integer(field: &Field) -> bool {
    matches!(
        field.data_type(),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    ) && !has_logical_type(field)
}

/// Whether a field has the metadata of a logical type (see [`LOGICAL_TYPE_METADATA_KEYS`]).
fn has_logical_type(field: &Field) -> bool {
    LOGICAL_TYPE_METADATA_KEYS
        .iter()
        .any(|key| field.metadata().contains_key(key))
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
    /// A struct built field by field: each field that the plan reads from the current
    /// field at that position, and a null for the other fields.
    Struct(Vec<Option<(usize, Read)>>),
}

/// Finds how the columns of a table now are read as the columns that a view was created
/// with, and the changes that keep the view from reading them. The nullability of fields,
/// their metadata, and the names of the fields that Spark does not name (the elements of
/// lists and the entries of maps) are not compared. The definition of the view was
/// resolved as if every field had nulls (see [`ViewDefinitionSource`]).
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
        used: Option<&HashMap<String, ColumnUse>>,
    ) -> Option<(usize, Read)> {
        self.field(stored, found, current, &[stored.name().clone()], used)
    }

    /// Returns the position of the match of a stored field in the current fields and how
    /// the field is read. `used` are the fields that the plan reads, for a struct that it
    /// does not read whole.
    fn field(
        &mut self,
        field: &Field,
        found: FieldMatch,
        current: &Fields,
        path: &[String],
        used: Option<&HashMap<String, ColumnUse>>,
    ) -> Option<(usize, Read)> {
        match found {
            FieldMatch::Found(position) => {
                Some((position, self.value(field, &current[position], used, path)))
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
    fn value(
        &mut self,
        stored: &Field,
        current: &Field,
        used: Option<&HashMap<String, ColumnUse>>,
        path: &[String],
    ) -> Read {
        if stored == current {
            return Read::Same;
        }
        self.logical_type_changes(stored, current, path);
        let read = self.data_type(stored.data_type(), current.data_type(), used, path);
        let key = SPARK_METADATA_JSON_KEY;
        if read == Read::Same && stored.metadata().get(key) != current.metadata().get(key) {
            Read::Cast
        } else {
            read
        }
    }

    /// Returns how a value of the current type is read as the stored type.
    fn data_type(
        &mut self,
        stored: &DataType,
        current: &DataType,
        used: Option<&HashMap<String, ColumnUse>>,
        path: &[String],
    ) -> Read {
        use DataType::*;
        if stored == current {
            return Read::Same;
        }
        match (stored, current) {
            (Struct(stored), Struct(current)) => self.structs(stored, current, used, path),
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
                cast_if_changed(&[self.data_type(a, b, None, path)])
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

    /// Returns how a current struct is read as a stored struct. A struct that the plan reads
    /// only through the fields `used` keeps those fields, and the other fields can change. A
    /// struct that is read whole keeps all its fields, in their order and with their names.
    fn structs(
        &mut self,
        stored: &Fields,
        current: &Fields,
        used: Option<&HashMap<String, ColumnUse>>,
        path: &[String],
    ) -> Read {
        let matches = match_fields(stored, current, self.case_sensitive);
        let changes = self.changes.len();
        let mut reads = Vec::with_capacity(stored.len());
        let mut same = stored.len() == current.len();
        // Whether the fields are the stored fields in their order and with their names.
        let mut layout = same;
        for (i, (field, found)) in stored.iter().zip(matches).enumerate() {
            let fields = match used {
                Some(used) => match used
                    .iter()
                    .find(|(name, _)| same_name(name, field.name(), self.case_sensitive))
                {
                    Some((_, field_use)) => field_use.fields(),
                    None => {
                        same = false;
                        reads.push(None);
                        continue;
                    }
                },
                None => None,
            };
            let read = self.field(
                field,
                found,
                current,
                &child_path(path, field.name()),
                fields,
            );
            if let Some((position, read)) = &read {
                let kept = *position == i && current[*position].name() == field.name();
                layout &= kept;
                same &= kept && read == &Read::Same;
            }
            reads.push(read);
        }
        if used.is_none() {
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
        }
        match used {
            _ if same => Read::Same,
            Some(_) => Read::Struct(reads),
            None if layout => Read::Cast,
            // The plan of the view would compute on the fields as stored, such as in their
            // stored order and with their stored names.
            None => {
                if self.changes.len() == changes {
                    let change = format!("{} fields have changed", quote_names(path));
                    self.changes.push(change);
                }
                Read::Same
            }
        }
    }

    /// Returns how a value inside a list, a map or another such type is read, whole.
    fn element(&mut self, stored: &Field, current: &Field, path: &[String], name: &str) -> Read {
        self.value(stored, current, None, &child_path(path, name))
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
/// type, whose fields can have nulls now that had none, unless the type cannot be cast to.
fn cast_target(stored: &Field, current: &Field) -> Field {
    let target = nullable_field(stored).data_type().clone();
    let data_type = if stored.data_type() == current.data_type() {
        stored.data_type().clone()
    } else if casts_by_name(current.data_type(), &target) {
        target
    } else {
        current.data_type().clone()
    };
    stored.clone().with_data_type(data_type)
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

/// Returns `value`, a value of the current field, in the stored type before the cast to the
/// stored type: with the fields of the stored struct for a struct that is built (see
/// [`Read`]). A field that the view does not read is null, so every field of a struct that
/// is built can have nulls.
fn read_value(value: Expr, stored: &Field, current: &Field, read: &Read) -> PlanResult<Expr> {
    let reads = match read {
        Read::Same | Read::Cast => return Ok(value),
        Read::Struct(reads) => reads,
    };
    let (DataType::Struct(stored_fields), DataType::Struct(current_fields)) =
        (stored.data_type(), current.data_type())
    else {
        return Ok(value);
    };
    let mut args = Vec::with_capacity(stored_fields.len() * 2);
    for (field, read) in stored_fields.iter().zip(reads) {
        let value = match read {
            Some((position, read)) => {
                let current = &current_fields[*position];
                let value = get_field().call(vec![value.clone(), lit(current.name().as_str())]);
                read_value(value, field, current, read)?
            }
            None => Expr::Literal(ScalarValue::try_new_null(field.data_type())?, None),
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
    Ok(when(value.is_null(), null).otherwise(named_struct().call(args))?)
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
    /// read a persistent view, or a table that the plan does not scan, such as the table of
    /// a scalar subquery that `typeof` folded into a value.
    pub(in crate::resolver) fn store_view_plan(
        plan: LogicalPlan,
        reads: &ViewDefinitionReads,
    ) -> PlanResult<(LogicalPlan, bool)> {
        let required = required_columns(&plan, reads)?;
        let mut scanned = HashSet::new();
        visit_scans(&plan, &mut |scan| {
            scanned.insert(table_source_address(&scan.source));
            Ok(())
        })?;
        let resolves_at_each_read =
            reads.reads_persistent_view || reads.sources().any(|x| !scanned.contains(&x));
        // The stored plan has its own CTE definitions, and the stored scans have the
        // nullability of the tables, which the plan above them was resolved without.
        let plan = replace_scans(
            plan,
            &|scan| reads.table_origin(&scan.source).is_some(),
            &|scan| ViewDefinitionSource::changes_nullability(&scan.source),
            &mut |scan| {
                let Some(origin) = reads.table_origin(&scan.source) else {
                    return Ok(LogicalPlan::TableScan(scan));
                };
                let required = required.get(&table_source_address(&scan.source));
                let scan = ViewTableReference::store(scan, origin, required)?;
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
        let definition = Self::rebound_view_definition(view);
        // A view created over this view expands the star again as well.
        if definition.as_ref().is_some_and(|x| x.star_in_function) {
            state.record_star_in_function();
        }
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
        // the table changed. The nodes above the scan then compute their schemas again.
        let mut recompute_schemas = false;
        // Whether a table gained columns, which a star inside the arguments of a function
        // expands to when the query of the view is resolved again.
        let mut gained_columns = false;
        let recorded = state.view_definition_mut().map(|reads| reads.clone());
        for scan in &references {
            let address = table_source_address(&scan.source);
            if scans.contains_key(&address) {
                continue;
            }
            let bound = match self
                .bind_view_table_scan(scan, view, definition.as_deref(), state)
                .await
            {
                Ok(bound) => bound,
                // An error of the read itself, such as of a table created again in another
                // format, does not make the view resolve its query again, which would read
                // the table in another way. A request for the schema gets the stored columns.
                Err(error) if !self.schema_only => return Err(error),
                Err(_) => {
                    scans.clear();
                    (gained_columns, recompute_schemas) = (false, false);
                    break;
                }
            };
            let (bound, gained) = match bound {
                Ok(bound) => bound,
                Err(error) => {
                    // A view that the query creates does not read the scans bound so far.
                    if let (Some(reads), Some(recorded)) = (state.view_definition_mut(), recorded) {
                        *reads = recorded;
                    }
                    if let Some(definition) = &definition
                        && let Some(plan) = self
                            .resolve_view_definition_if_possible(&plan, view, definition, state)
                            .await?
                    {
                        return Ok(plan);
                    }
                    // A request for the schema gets the columns of the stored plan.
                    if !self.schema_only {
                        return Err(error);
                    }
                    scans.clear();
                    (gained_columns, recompute_schemas) = (false, false);
                    break;
                }
            };
            gained_columns |= gained;
            recompute_schemas |= bound.schema() != &scan.projected_schema;
            scans.insert(address, bound);
        }
        // The stored plan remains if the query no longer resolves.
        if gained_columns
            && let Some(definition) = &definition
            && let Ok(Some(plan)) =
                Box::pin(self.resolve_view_definition(&plan, view, definition, state)).await
        {
            return Ok(plan);
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
    fn rebound_view_definition(view: &TableStatus) -> Option<Arc<TemporaryViewDefinition>> {
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
    ) -> PlanResult<Option<LogicalPlan>> {
        let plan = Box::pin(self.resolve_view_definition(plan, view, definition, state)).await;
        Ok(plan.unwrap_or(None))
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
    /// whether the table gained columns that a star of the definition of the view expands
    /// to, or the error of the columns that changed.
    async fn bind_view_table_scan(
        &self,
        scan: &TableScan,
        view: &TableStatus,
        definition: Option<&TemporaryViewDefinition>,
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
        let gained = definition.is_some_and(|x| x.star_in_function)
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
        Ok(Ok((reference.with_stored_output(bound, &reads)?, gained)))
    }

    /// Returns the source of a read of a catalog table that a temporary view was created
    /// over, with the catalog table lookups and the table sources of the query, or the error
    /// that the table is not found or that its name is now the name of a view.
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
        .map(Ok)
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

/// Whether `current` has a field, also in a struct, that `stored` does not have.
fn has_new_fields(stored: &Fields, current: &Fields, case_sensitive: bool) -> bool {
    current.iter().any(|field| {
        let old = stored
            .iter()
            .find(|x| same_name(x.name(), field.name(), case_sensitive));
        match (old.map(|x| x.data_type()), field.data_type()) {
            (None, _) => true,
            (Some(DataType::Struct(old)), DataType::Struct(new)) => {
                has_new_fields(old, new, case_sensitive)
            }
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

/// How a stored plan reads a column.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::resolver) enum ColumnUse {
    /// The plan reads the value of the column.
    Whole,
    /// The plan reads the values of some fields of a struct.
    Fields(HashMap<String, ColumnUse>),
    /// The plan only tells rows apart by the column, as a key of `DISTINCT` or `GROUP BY`
    /// whose value is not read, or orders rows by it, as a key of `ORDER BY`. The current
    /// values of the column tell the rows apart or order them, of whatever type they are now.
    Identity,
    /// The plan only reads the column as an operand: a comparison operand, or the argument
    /// of `count`, `sum` or `avg`. The resolver plans these the same way for each signed
    /// integer type (`Int8` to `Int64`), the analyzer coerces the current column, and they
    /// return the same type for each of them, so a column of one of these types can have
    /// another one. Two things keep this exact. The operand is the column itself in the
    /// stored plan, since the resolver casts the column only for the types that it plans
    /// another way (such as an integer compared with a `FLOAT` in ANSI mode), and then the
    /// column is not an operand. And no other type is accepted: unsigned integers have
    /// other coercions and other `sum` types, and an integer of a user-defined type or an
    /// extension type is another logical type, so such a column must keep its type.
    Operand,
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
            (ColumnUse::Whole, _)
            | (ColumnUse::Identity, ColumnUse::Identity)
            | (ColumnUse::Operand, ColumnUse::Identity | ColumnUse::Operand) => false,
            (ColumnUse::Fields(fields), ColumnUse::Fields(others)) => {
                let mut changed = false;
                for (name, other) in others {
                    changed |= add_use(fields, name.clone(), other);
                }
                changed
            }
            // An operand that also tells rows apart keeps a signed integer type.
            (ColumnUse::Identity, ColumnUse::Operand) => {
                *self = ColumnUse::Operand;
                true
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

/// Returns the value that a struct field access selects from a struct that the expression
/// builds, such as `a` for `named_struct('p', a, 'q', b).p`, which does not read `b`.
fn constructed_field(expr: &Expr) -> Option<Expr> {
    let Expr::ScalarFunction(function) = expr else {
        return None;
    };
    if !function.func.inner().is::<GetFieldFunc>() {
        return None;
    }
    let [base, Expr::Literal(name, _)] = function.args.as_slice() else {
        return None;
    };
    let name = name.try_as_str().flatten()?;
    let Some(base) = constructed_field(base) else {
        return struct_argument(base, name).cloned();
    };
    match struct_argument(&base, name) {
        Some(value) => Some(value.clone()),
        None => Some(get_field().call(vec![base, lit(name)])),
    }
}

/// Returns the argument of a struct constructor that is the value of the field `name`.
fn struct_argument<'a>(expr: &'a Expr, name: &str) -> Option<&'a Expr> {
    let Expr::ScalarFunction(function) = expr else {
        return None;
    };
    if function.func.inner().is::<NamedStructFunc>() {
        let (pairs, _) = function.args.as_chunks::<2>();
        return pairs.iter().find_map(|[field, value]| match field {
            Expr::Literal(field, _) if field.try_as_str().flatten() == Some(name) => Some(value),
            _ => None,
        });
    }
    let fields = function.func.inner().downcast_ref::<StructFromFields>()?;
    let position = fields.field_names().iter().position(|x| x == name)?;
    function.args.get(position)
}

/// Adds the columns that an expression reads, and the fields of the structs that it
/// reads only through field accesses. A subquery whose values the expression compares or
/// returns reads the outputs of the subquery, while `EXISTS` reads only what the operators
/// of its subquery read.
fn add_expression_uses(
    expr: &Expr,
    uses: &mut HashMap<String, ColumnUse>,
) -> datafusion_common::Result<()> {
    expr.apply(|e| {
        if let Some((column, path)) = field_access(e) {
            add_use(uses, column.name.clone(), &ColumnUse::path(&path));
            return Ok(TreeNodeRecursion::Jump);
        }
        if let Some(value) = constructed_field(e) {
            add_expression_uses(&value, uses)?;
            return Ok(TreeNodeRecursion::Jump);
        }
        match e {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) if is_comparison(*op) => {
                add_column_uses([left.as_ref(), right.as_ref()], &ColumnUse::Operand, uses)?;
                return Ok(TreeNodeRecursion::Jump);
            }
            Expr::InList(InList { expr, list, .. }) => {
                let operands = std::iter::once(expr.as_ref()).chain(list);
                add_column_uses(operands, &ColumnUse::Operand, uses)?;
                return Ok(TreeNodeRecursion::Jump);
            }
            Expr::AggregateFunction(AggregateFunction { func, params })
                if func.inner().is::<Count>()
                    || func.inner().is::<Sum>()
                    || func.inner().is::<Avg>() =>
            {
                let AggregateFunctionParams {
                    args,
                    filter,
                    order_by,
                    ..
                } = params;
                add_column_uses(args, &ColumnUse::Operand, uses)?;
                let others = filter.as_deref().into_iter();
                for e in others.chain(order_by.iter().map(|x| &x.expr)) {
                    add_expression_uses(e, uses)?;
                }
                return Ok(TreeNodeRecursion::Jump);
            }
            Expr::Column(column) | Expr::OuterReferenceColumn(_, column) => {
                add_use(uses, column.name.clone(), &ColumnUse::Whole);
            }
            Expr::ScalarSubquery(subquery)
            | Expr::InSubquery(InSubquery { subquery, .. })
            | Expr::SetComparison(SetComparison { subquery, .. }) => {
                for field in subquery.subquery.schema().fields() {
                    add_use(uses, field.name().clone(), &ColumnUse::Whole);
                }
            }
            _ => {}
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(())
}

/// Adds the uses of expressions that read a column that is one of them as `used`, such as
/// the keys of `DISTINCT` or `GROUP BY`, which only tell rows apart. A column of an outer
/// query has the type of the column when the plan was resolved, so it is read whole.
fn add_column_uses<'a>(
    exprs: impl IntoIterator<Item = &'a Expr>,
    used: &ColumnUse,
    uses: &mut HashMap<String, ColumnUse>,
) -> datafusion_common::Result<()> {
    for expr in exprs {
        match expr {
            Expr::Column(column) => {
                add_use(uses, column.name.clone(), used);
            }
            expr => add_expression_uses(expr, uses)?,
        }
    }
    Ok(())
}

/// Adds how each output of a projection, or each value that an operator computes, reads
/// the columns of its input, for the expressions and the fields of the outputs.
fn add_derivations<'a>(
    outputs: impl Iterator<Item = (&'a Expr, &'a FieldRef)>,
    derivations: &mut HashMap<String, Vec<Derivation<String>>>,
) -> datafusion_common::Result<()> {
    for (expr, field) in outputs {
        derivations
            .entry(field.name().clone())
            .or_default()
            .push(Derivation::of(expr)?);
    }
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
            Derivation::Value(column)
                if matches!(used, ColumnUse::Identity | ColumnUse::Operand) =>
            {
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

/// Adds how the outputs of an extension node read its inputs, and returns whether the node
/// reports it. The node reports the input columns that its outputs need, apart from the
/// columns of its expressions, and DataFusion's `OptimizeProjections` prunes the inputs of
/// the node by the same report. An output that needs only the input column of its name
/// passes the column on. Any other output reads whole the inputs that it needs, and the
/// inputs that the node needs for every output are read whole.
fn add_extension_reads(
    node: &dyn UserDefinedLogicalNode,
    uses: &mut HashMap<String, ColumnUse>,
    derivations: &mut HashMap<String, Vec<Derivation<String>>>,
) -> bool {
    let inputs = node.inputs();
    let needs = |outputs: &[usize]| -> Option<HashSet<&String>> {
        let required = node.necessary_children_exprs(outputs)?;
        if required.len() != inputs.len() {
            return None;
        }
        inputs
            .iter()
            .zip(required)
            .flat_map(|(input, columns)| {
                columns
                    .into_iter()
                    .map(|i| input.schema().fields().get(i).map(|x| x.name()))
            })
            .collect()
    };
    // Nothing is added until every output is described, so that a node that does not
    // report what an output needs reads all its inputs.
    let Some(always) = needs(&[]) else {
        return false;
    };
    let mut computed = vec![];
    for (i, output) in node.schema().fields().iter().enumerate() {
        let Some(mut reads) = needs(&[i]) else {
            return false;
        };
        reads.retain(|name| !always.contains(name));
        if reads.len() == 1 && reads.contains(output.name()) {
            continue;
        }
        if !reads.is_empty() {
            let reads = reads
                .into_iter()
                .map(|name| (name.clone(), ColumnUse::Whole));
            computed.push((output.name().clone(), reads.collect()));
        }
    }
    for name in always {
        add_use(uses, name.clone(), &ColumnUse::Whole);
    }
    for (output, reads) in computed {
        derivations
            .entry(output)
            .or_default()
            .push(Derivation::Computed(reads));
    }
    true
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
/// and grouping columns tell rows apart, sort columns order them, and columns that are
/// compared, join keys, or arguments of `count`, `sum` or `avg` are only operands
/// ([`ColumnUse::Operand`]). Operators that pass their inputs on read the
/// columns of their outputs that are read, and so do extension nodes for the outputs that
/// they report to pass on. Any other operator reads all its inputs. The uses are
/// propagated once the plan, its subqueries and its CTE definitions are visited, so the
/// order of the visit does not matter.
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
                let outputs = projection.expr.iter().zip(projection.schema.fields());
                add_derivations(outputs, &mut derivations)?;
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
                add_column_uses(&on.on_expr, &ColumnUse::Identity, &mut uses)?;
                let outputs = on.select_expr.iter().zip(on.schema.fields());
                add_derivations(outputs, &mut derivations)?;
            }
            LogicalPlan::Aggregate(aggregate) => {
                // The keys decide the rows, and an aggregate value reads its arguments
                // when it is read. The values are the last outputs.
                add_column_uses(&aggregate.group_expr, &ColumnUse::Identity, &mut uses)?;
                let fields = aggregate.schema.fields().iter().rev();
                add_derivations(
                    aggregate.aggr_expr.iter().rev().zip(fields),
                    &mut derivations,
                )?;
            }
            LogicalPlan::Sort(sort) => {
                // A key that is a column orders the rows that the sort passes on.
                let keys = sort.expr.iter().map(|x| &x.expr);
                add_column_uses(keys, &ColumnUse::Identity, &mut uses)?;
            }
            LogicalPlan::Join(join) => {
                // Each pair of keys is compared, and the join passes its input columns on.
                let keys = join.on.iter().flat_map(|(left, right)| [left, right]);
                add_column_uses(keys, &ColumnUse::Operand, &mut uses)?;
                if let Some(filter) = &join.filter {
                    add_expression_uses(filter, &mut uses)?;
                }
            }
            LogicalPlan::Window(window) => {
                // The input columns are passed on, and a window value reads its arguments
                // and its keys when it is read. The values are the last outputs.
                let fields = window.schema.fields().iter().rev();
                add_derivations(
                    window.window_expr.iter().rev().zip(fields),
                    &mut derivations,
                )?;
            }
            node => {
                node.apply_expressions(|e| {
                    add_expression_uses(e, &mut uses)?;
                    Ok(TreeNodeRecursion::Continue)
                })?;
                // These operators pass their input columns on, `UNNEST` the columns that
                // it does not unnest, which its expressions read, and any other operator
                // may read all of them.
                let passes_on = match node {
                    LogicalPlan::Filter(_)
                    | LogicalPlan::Limit(_)
                    | LogicalPlan::SubqueryAlias(_)
                    | LogicalPlan::Repartition(_)
                    // The expression of a subquery reads the outputs of the subquery that
                    // it needs (see `add_expression_uses`).
                    | LogicalPlan::Subquery(_)
                    | LogicalPlan::Unnest(_) => true,
                    LogicalPlan::Extension(extension) => {
                        add_extension_reads(extension.node.as_ref(), &mut uses, &mut derivations)
                    }
                    _ => false,
                };
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
    // The scans have the nullability of the tables, which the plan above them was resolved
    // without.
    Ok(replace_scans(
        plan,
        &|scan| scan.source.downcast_ref::<ViewDefinitionSource>().is_some(),
        &|scan| ViewDefinitionSource::changes_nullability(&scan.source),
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

impl ViewDefinitionSource {
    /// Returns whether the source is a definition source whose schema allows nulls that
    /// the schema of the source that it wraps does not allow.
    fn changes_nullability(source: &Arc<dyn TableSource>) -> bool {
        source
            .downcast_ref::<Self>()
            .is_some_and(|source| source.schema != source.inner.schema())
    }
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

    use arrow_schema::extension::EXTENSION_TYPE_NAME_KEY;
    use datafusion::arrow::datatypes::{
        DataType, Field, Fields, IntervalUnit, Schema, TimeUnit, UnionFields, UnionMode,
    };
    use datafusion::datasource::{MemTable, provider_as_source};
    use datafusion::functions_aggregate::expr_fn::{avg, count, max, sum};
    use datafusion::functions_nested::expr_fn::make_array;
    use datafusion::functions_window::expr_fn::row_number;
    use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion_common::{Column, JoinType, ScalarValue};
    use datafusion_expr::expr::Exists;
    use datafusion_expr::expr_fn::{cast, exists, in_list, in_subquery, scalar_subquery};
    use datafusion_expr::{
        Expr, ExprFunctionExt, Extension, LogicalPlan, LogicalPlanBuilder, Subquery, TableScan,
        UserDefinedLogicalNode, col, lit,
    };
    use datafusion_functions::core::{get_field, named_struct};
    use sail_function::scalar::r#struct::struct_from_fields::StructFromFields;
    use sail_logical_plan::barrier::BarrierNode;
    use sail_logical_plan::monotonic_id::MonotonicIdNode;
    use sail_logical_plan::repartition::{ExplicitRepartitionKind, ExplicitRepartitionNode};
    use sail_logical_plan::spark_partition_id::SparkPartitionIdNode;

    use super::{
        ColumnUse, FieldMatch, Fit, Read, TableOrigin, ViewTableReference, required_columns,
        view_definition_source,
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

    /// Returns how a column of the field `current` is read as the field `stored`, if it is
    /// read whole and did not change.
    fn read_of(stored: &Field, current: &Field) -> Result<Read, Vec<String>> {
        let mut fit = Fit::new(false, &type_name);
        let read = fit.value(stored, current, None, &[]);
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
            None,
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
    fn test_nullability_changes_of_each_type_are_not_column_changes() {
        for data_type in data_types() {
            let required = Field::new("c", data_type.clone(), false);
            assert_eq!(
                read_of(&field("c", data_type.clone()), &required),
                Ok(Read::Same)
            );
            assert_eq!(
                read_of(&required, &field("c", data_type.clone())),
                Ok(Read::Same)
            );
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
        // A column without nulls now is read, as the same column.
        let current = Schema::new(vec![
            field("a", DataType::Int32),
            Field::new("b", DataType::Utf8, false),
        ]);
        assert_eq!(
            positions(&reference, &current, true),
            Ok(vec![Some(0), Some(1)])
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
                positions(&reference, &current, true),
                Ok(vec![Some(1), Some(0)]),
                "{current}"
            );
        }
        assert_eq!(
            positions(
                &reference,
                &Schema::new(vec![field("a", DataType::Int32)]),
                true
            ),
            Err(vec!["`b` Int32 has been removed".to_string()])
        );
        Ok(())
    }

    #[test]
    fn test_columns_that_are_only_operands_can_change_between_signed_integer_types() {
        let integers = [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
        ];
        let operand = |schema: &Schema| ViewTableReference {
            required: vec![Some(ColumnUse::Operand)],
            ..reference(schema)
        };
        for stored in &integers {
            let reference = operand(&Schema::new(vec![field("b", stored.clone())]));
            for current in &integers {
                let current = Schema::new(vec![field("b", current.clone())]);
                assert_eq!(positions(&reference, &current, true), Ok(vec![Some(0)]));
            }
            for current in [
                DataType::UInt64,
                DataType::Float32,
                DataType::Float64,
                DataType::Decimal128(20, 0),
                DataType::Utf8,
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Int64)),
            ] {
                let current = Schema::new(vec![field("b", current)]);
                assert!(positions(&reference, &current, true).is_err(), "{current}");
            }
            // An integer of an extension type is another logical type.
            let extension = |data_type: DataType| {
                field("b", data_type).with_metadata(HashMap::from([(
                    EXTENSION_TYPE_NAME_KEY.to_string(),
                    "arrow.bool8".to_string(),
                )]))
            };
            let current = Schema::new(vec![extension(DataType::Int8)]);
            assert!(positions(&reference, &current, true).is_err(), "{current}");
            let reference = operand(&Schema::new(vec![extension(stored.clone())]));
            let current = Schema::new(vec![field("b", DataType::Int64)]);
            assert!(positions(&reference, &current, true).is_err(), "{current}");
        }
    }

    #[test]
    fn test_operands_have_the_same_result_type_for_each_signed_integer_type() -> TestResult {
        // `count`, `sum` and `avg` (the functions that the resolver plans for Spark), and a
        // comparison, of each signed integer type.
        for data_type in [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
        ] {
            let schema = Arc::new(Schema::new(vec![field("b", data_type)]));
            let source = provider_as_source(Arc::new(MemTable::try_new(schema, vec![vec![]])?));
            let plan = LogicalPlanBuilder::scan("t", source, None)?
                .aggregate(
                    Vec::<Expr>::new(),
                    vec![
                        count(col("b")),
                        count(col("b")).distinct().build()?,
                        sum(col("b")),
                        avg(col("b")),
                    ],
                )?
                .build()?;
            let types = plan
                .schema()
                .fields()
                .iter()
                .map(|x| x.data_type().clone())
                .collect::<Vec<_>>();
            assert_eq!(
                types,
                [
                    DataType::Int64,
                    DataType::Int64,
                    DataType::Int64,
                    DataType::Float64
                ]
            );
        }
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
                fit.column(&stored, FieldMatch::Found(0), &current, None);
                assert_eq!(fit.changes.len(), 1, "{names:?}: {:?}", fit.changes);
            }
        }
        // A struct that is built from the fields that the plan reads reads them whole.
        let built = |x: DataType| field("c", DataType::Struct(vec![field("x", x)].into()));
        let used = HashMap::from([("x".to_string(), ColumnUse::Whole)]);
        let mut fit = Fit::new(false, &type_name);
        let current = Fields::from(vec![built(fields(&["a", "b", "d"]))]);
        fit.column(
            &built(fields(&["a", "b"])),
            FieldMatch::Found(0),
            &current,
            Some(&used),
        );
        assert_eq!(
            fit.changes,
            vec!["`c`.`x`.`d` Int32 has been added".to_string()]
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

    /// The columns that are read whole, and the column that is only an operand.
    fn operand(positions: &[usize], operand: usize) -> Uses {
        let mut uses = whole(positions).unwrap_or_default();
        uses.push((operand, ColumnUse::Operand));
        uses.sort_by_key(|(i, _)| *i);
        Some(uses)
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
        assert_eq!(uses(&plan, &reads, address)?, operand(&[0], 2));
        // The computed output is read.
        let plan = LogicalPlanBuilder::from(t)
            .filter(col("t_c").gt(lit(0)))?
            .project(vec![col("t_a"), (col("t_b") + lit(1)).alias("y")])?
            .project(vec![col("y")])?
            .build()?;
        assert_eq!(uses(&plan, &reads, address)?, operand(&[1], 2));
        Ok(())
    }

    #[test]
    fn test_values_that_are_not_read_do_not_read_their_arguments() -> TestResult {
        let mut reads = ViewDefinitionReads::default();
        let (t, address) = read_table(&mut reads, "t")?;
        // `SELECT a FROM (SELECT a, sum(b) AS total FROM t GROUP BY a)`.
        let plan = LogicalPlanBuilder::from(t.clone())
            .aggregate(vec![col("t_a")], vec![sum(col("t_b")).alias("total")])?
            .project(vec![col("t_a")])?
            .build()?;
        assert_eq!(uses(&plan, &reads, address)?, whole(&[0]));
        // `row_number() OVER (PARTITION BY b ORDER BY c)`, read or not.
        let window = row_number()
            .partition_by(vec![col("t_b")])
            .order_by(vec![col("t_c").sort(true, false)])
            .build()?;
        let windowed = LogicalPlanBuilder::from(t).window(vec![window])?.build()?;
        let value = Expr::Column(Column::from_name(windowed.schema().field(4).name()));
        let plan = LogicalPlanBuilder::from(windowed.clone())
            .project(vec![col("t_a")])?
            .build()?;
        assert_eq!(uses(&plan, &reads, address)?, whole(&[0]));
        let plan = LogicalPlanBuilder::from(windowed)
            .project(vec![col("t_a"), value])?
            .build()?;
        assert_eq!(uses(&plan, &reads, address)?, whole(&[0, 1, 2]));
        Ok(())
    }

    #[test]
    fn test_subqueries_read_the_outputs_whose_values_are_used() -> TestResult {
        let mut reads = ViewDefinitionReads::default();
        let (t, address) = read_table(&mut reads, "t")?;
        let (u, other) = read_table(&mut reads, "u")?;
        // `SELECT a FROM t WHERE b IN (SELECT c FROM u)`.
        let values = LogicalPlanBuilder::from(u.clone())
            .project(vec![col("u_c")])?
            .build()?;
        let plan = LogicalPlanBuilder::from(t.clone())
            .filter(in_subquery(col("t_b"), Arc::new(values)))?
            .project(vec![col("t_a")])?
            .build()?;
        assert_eq!(uses(&plan, &reads, address)?, whole(&[0, 1]));
        assert_eq!(uses(&plan, &reads, other)?, whole(&[2]));
        // `SELECT a FROM t WHERE EXISTS (SELECT * FROM u WHERE u.b > 0)`.
        let rows = LogicalPlanBuilder::from(u.clone())
            .filter(col("u_b").gt(lit(0)))?
            .project(["u_a", "u_b", "u_c", "u_s"].map(col).to_vec())?
            .build()?;
        let plan = LogicalPlanBuilder::from(t.clone())
            .filter(exists(Arc::new(rows)))?
            .project(vec![col("t_a")])?
            .build()?;
        assert_eq!(uses(&plan, &reads, other)?, operand(&[], 1));
        // A scalar subquery in an output that is read or not.
        let value = LogicalPlanBuilder::from(u)
            .aggregate(Vec::<Expr>::new(), vec![sum(col("u_c")).alias("m")])?
            .build()?;
        let outputs = LogicalPlanBuilder::from(t)
            .project(vec![
                col("t_a"),
                scalar_subquery(Arc::new(value)).alias("v"),
            ])?
            .build()?;
        let plan = LogicalPlanBuilder::from(outputs.clone())
            .project(vec![col("t_a")])?
            .build()?;
        assert_eq!(uses(&plan, &reads, address)?, whole(&[0]));
        assert_eq!(uses(&plan, &reads, other)?, Some(vec![]));
        let plan = LogicalPlanBuilder::from(outputs)
            .project(vec![col("v")])?
            .build()?;
        assert_eq!(uses(&plan, &reads, other)?, operand(&[], 2));
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
            .aggregate(vec![col("t_a")], vec![max(col("t_b")).alias("total")])?
            .project(vec![col("t_a"), col("total")])?
            .build()?;
        assert_eq!(uses(&plan, &reads, address)?, whole(&[0, 1]));
        let plan = LogicalPlanBuilder::from(t.clone())
            .sort(vec![(-col("t_c")).sort(true, false)])?
            .limit(0, Some(1))?
            .project(vec![col("t_a")])?
            .build()?;
        assert_eq!(uses(&plan, &reads, address)?, whole(&[0, 2]));
        let (u, other) = read_table(&mut reads, "u")?;
        let keys = (col("t_c") + lit(1)).eq(col("u_b") + lit(1));
        let plan = LogicalPlanBuilder::from(t)
            .join_on(u, JoinType::Inner, vec![keys])?
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
                (1, ColumnUse::Operand),
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
    fn test_columns_that_are_only_operands_or_order_rows_are_found() -> TestResult {
        let mut reads = ViewDefinitionReads::default();
        let (t, address) = read_table(&mut reads, "t")?;
        let (u, other) = read_table(&mut reads, "u")?;
        let filtered = |predicate: Expr| -> Result<Uses, Box<dyn std::error::Error>> {
            let plan = LogicalPlanBuilder::from(t.clone())
                .filter(predicate)?
                .project(vec![col("t_a")])?
                .build()?;
            uses(&plan, &reads, address)
        };
        // A comparison, an IN list, and `BETWEEN` as the resolver writes it.
        assert_eq!(filtered(col("t_b").gt(lit(0)))?, operand(&[0], 1));
        assert_eq!(filtered(lit(0).lt_eq(col("t_b")))?, operand(&[0], 1));
        let list = in_list(col("t_b"), vec![lit(1), col("t_c")], false);
        assert_eq!(
            filtered(list)?,
            Some(vec![
                (0, ColumnUse::Whole),
                (1, ColumnUse::Operand),
                (2, ColumnUse::Operand)
            ])
        );
        let between = col("t_b").gt_eq(lit(1)).and(col("t_b").lt_eq(lit(9)));
        assert_eq!(filtered(between)?, operand(&[0], 1));
        // An operand that the resolver casts, or that is computed, is read whole.
        assert_eq!(
            filtered(cast(col("t_b"), DataType::Float64).gt(lit(0.5)))?,
            whole(&[0, 1])
        );
        assert_eq!(filtered((col("t_b") + lit(1)).gt(lit(0)))?, whole(&[0, 1]));
        // A column that is also read whole is read whole.
        let plan = LogicalPlanBuilder::from(t.clone())
            .filter(col("t_b").gt(lit(0)))?
            .project(vec![col("t_a"), col("t_b")])?
            .build()?;
        assert_eq!(uses(&plan, &reads, address)?, whole(&[0, 1]));
        // The arguments of `count`, `sum` and `avg`, but not of other aggregate functions.
        for (value, used) in [
            (count(col("t_b")), ColumnUse::Operand),
            (count(col("t_b")).distinct().build()?, ColumnUse::Operand),
            (sum(col("t_b")), ColumnUse::Operand),
            (avg(col("t_b")), ColumnUse::Operand),
            (max(col("t_b")), ColumnUse::Whole),
            (sum(col("t_b") + lit(1)), ColumnUse::Whole),
        ] {
            let plan = LogicalPlanBuilder::from(t.clone())
                .aggregate(vec![col("t_a")], vec![value.alias("v")])?
                .project(vec![col("t_a"), col("v")])?
                .build()?;
            let expected = Some(vec![(0, ColumnUse::Whole), (1, used)]);
            assert_eq!(uses(&plan, &reads, address)?, expected);
        }
        // A sort key that is a column orders rows, and an operand that also orders rows is
        // an operand.
        let plan = LogicalPlanBuilder::from(t.clone())
            .sort(vec![
                col("t_b").sort(false, true),
                col("t_c").sort(true, false),
            ])?
            .filter(col("t_c").gt(lit(0)))?
            .limit(0, Some(1))?
            .project(vec![col("t_a")])?
            .build()?;
        assert_eq!(
            uses(&plan, &reads, address)?,
            Some(vec![
                (0, ColumnUse::Whole),
                (1, ColumnUse::Identity),
                (2, ColumnUse::Operand)
            ])
        );
        // The keys of a join, compared in pairs.
        let plan = LogicalPlanBuilder::from(t.clone())
            .join_on(u, JoinType::Inner, vec![col("t_c").eq(col("u_b"))])?
            .project(vec![col("t_a")])?
            .build()?;
        assert_eq!(uses(&plan, &reads, address)?, operand(&[0], 2));
        assert_eq!(uses(&plan, &reads, other)?, operand(&[], 1));
        // The inputs of a union keep the types of the columns that are operands.
        let union = LogicalPlanBuilder::from(t.clone())
            .project(vec![col("t_a"), col("t_b")])?
            .union(
                LogicalPlanBuilder::from(t)
                    .project(vec![col("t_c"), col("t_a")])?
                    .build()?,
            )?
            .build()?;
        let plan = LogicalPlanBuilder::from(union)
            .filter(col("t_b").gt(lit(0)))?
            .project(vec![col("t_a")])?
            .build()?;
        assert_eq!(uses(&plan, &reads, address)?, whole(&[0, 1, 2]));
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
    fn test_extension_nodes_read_the_columns_that_they_report() -> TestResult {
        let mut reads = ViewDefinitionReads::default();
        let (t, address) = read_table(&mut reads, "t")?;
        let input = Arc::new(t.clone());
        let extension = |node: Arc<dyn UserDefinedLogicalNode>| {
            LogicalPlanBuilder::from(LogicalPlan::Extension(Extension { node }))
        };
        let repartition = |kind| {
            extension(Arc::new(ExplicitRepartitionNode::new(
                Arc::clone(&input),
                Some(3),
                kind,
            )))
        };
        // A repartition passes its input columns on, and reads its keys.
        let plan = repartition(ExplicitRepartitionKind::Hash(vec![col("t_b")]))
            .project(vec![col("t_a")])?
            .build()?;
        assert_eq!(uses(&plan, &reads, address)?, whole(&[0, 1]));
        // The keys are read when no output of the node is read.
        let plan = repartition(ExplicitRepartitionKind::Hash(vec![col("t_b")]))
            .aggregate(Vec::<Expr>::new(), vec![count(lit(1))])?
            .build()?;
        assert_eq!(uses(&plan, &reads, address)?, whole(&[1]));
        let plan = repartition(ExplicitRepartitionKind::RoundRobin)
            .project(vec![get(col("t_s"), "x").alias("x")])?
            .build()?;
        let x = ColumnUse::path(&["x".to_string()]);
        assert_eq!(uses(&plan, &reads, address)?, Some(vec![(3, x)]));
        // The partition ID and a monotonically increasing ID read no input column.
        let plan = extension(Arc::new(SparkPartitionIdNode::try_new(
            Arc::clone(&input),
            "pid".to_string(),
        )?))
        .project(vec![col("t_a"), col("pid")])?
        .build()?;
        assert_eq!(uses(&plan, &reads, address)?, whole(&[0]));
        let plan = extension(Arc::new(MonotonicIdNode::try_new(
            Arc::clone(&input),
            "id".to_string(),
        )?))
        .project(vec![col("t_a"), col("id")])?
        .build()?;
        assert_eq!(uses(&plan, &reads, address)?, whole(&[0]));
        // A node that does not report what it needs reads all its inputs.
        let plan = extension(Arc::new(BarrierNode::new(vec![], Arc::clone(&input))))
            .project(vec![col("t_a")])?
            .build()?;
        assert_eq!(uses(&plan, &reads, address)?, whole(&[0, 1, 2, 3]));
        Ok(())
    }

    #[test]
    fn test_unnest_reads_the_columns_that_it_unnests_whole() -> TestResult {
        let mut reads = ViewDefinitionReads::default();
        let (t, address) = read_table(&mut reads, "t")?;
        // The columns that are not unnested are passed on.
        let plan = LogicalPlanBuilder::from(t.clone())
            .project(vec![
                col("t_a"),
                make_array(vec![col("t_b"), col("t_c")]).alias("e"),
            ])?
            .unnest_column("e")?
            .project(vec![col("t_a"), col("e")])?
            .build()?;
        assert_eq!(uses(&plan, &reads, address)?, whole(&[0, 1, 2]));
        // A field of a struct in an unnested list reads the list whole.
        let plan = LogicalPlanBuilder::from(t.clone())
            .project(vec![col("t_a"), make_array(vec![col("t_s")]).alias("e")])?
            .unnest_column("e")?
            .project(vec![get(col("e"), "x").alias("x")])?
            .build()?;
        assert_eq!(uses(&plan, &reads, address)?, whole(&[3]));
        // An unnested struct is read whole, whichever of its fields are read.
        let plan = LogicalPlanBuilder::from(t)
            .project(vec![col("t_a"), col("t_s")])?
            .unnest_column("t_s")?
            .project(vec![col("t_a")])?
            .build()?;
        assert_eq!(uses(&plan, &reads, address)?, whole(&[0, 3]));
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
        // The outer reference has the type of the column when the plan was resolved.
        assert_eq!(uses(&plan, &reads, address)?, whole(&[0, 1]));
        assert_eq!(uses(&plan, &reads, other)?, operand(&[], 0));
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
    fn test_fields_of_built_structs_read_only_their_values() -> TestResult {
        let mut reads = ViewDefinitionReads::default();
        let (t, address) = read_table(&mut reads, "t")?;
        let built = named_struct().call(vec![lit("p"), col("t_a"), lit("q"), col("t_b")]);
        let plan = LogicalPlanBuilder::from(t.clone())
            .project(vec![get_field().call(vec![built, lit("p")]).alias("p")])?
            .build()?;
        assert_eq!(uses(&plan, &reads, address)?, whole(&[0]));
        // A struct of Spark, and a field of the field that it selects.
        let fields = StructFromFields::new(vec!["p".to_string(), "q".to_string()]);
        let built = datafusion_expr::ScalarUDF::from(fields).call(vec![col("t_s"), col("t_b")]);
        let selected = get_field().call(vec![get_field().call(vec![built, lit("p")]), lit("x")]);
        let plan = LogicalPlanBuilder::from(t)
            .project(vec![selected.alias("x")])?
            .build()?;
        let x = ColumnUse::path(&["x".to_string()]);
        assert_eq!(uses(&plan, &reads, address)?, Some(vec![(3, x)]));
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
