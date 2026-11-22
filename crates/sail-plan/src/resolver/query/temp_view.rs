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
//! one version of the table.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use arrow_schema::extension::{EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY};
use datafusion::arrow::compute::can_cast_types;
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use datafusion::datasource::{provider_as_source, source_as_provider};
use datafusion_common::Column;
use datafusion_common::tree_node::{Transformed, TransformedResult};
use datafusion_expr::expr::Cast;
use datafusion_expr::{Expr, LogicalPlan, Projection, TableScan, TableScanBuilder, TableSource};
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

use super::read::CatalogTableRead;
use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::state::{PlanResolverState, table_source_address};
use crate::shared_cte::{
    inline_references, recompute_plan_schemas, renew_cte_definitions_with_scans, visit_scans,
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

    /// Returns the scan with a reference in the place of its source.
    fn store(mut scan: TableScan, origin: &Arc<TableOrigin>) -> PlanResult<TableScan> {
        let schema = scan.source.schema();
        // A source with duplicate column names is read under other names.
        let table_schema = source_as_provider(&scan.source)
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
        scan.source = Arc::new(Self {
            schema,
            table: table_schema,
            columns,
            origin: Arc::clone(origin),
        });
        scan.projection = None;
        Ok(scan)
    }

    /// Returns the positions in the table of the columns that the scan read when the view
    /// was created, or how the columns of the table changed. The rules are those of Spark
    /// for a temporary view that stores a plan (`V2ReferenceUtils.validateLoadedTableInTempView`
    /// and `SchemaUtils.validateSchemaCompatibility`): the table can have new columns and
    /// another column order, while each column that the table had when the view was
    /// created, whether the view reads it or not, keeps its type and its nullability, also
    /// for the fields of nested types.
    fn match_columns(
        &self,
        table: &Schema,
        case_sensitive: bool,
        type_name: &dyn Fn(&DataType) -> String,
    ) -> Result<Vec<usize>, Vec<String>> {
        let mut changes = vec![];
        let matches = match_fields(self.table.fields(), table.fields(), case_sensitive);
        let mut positions = Vec::with_capacity(matches.len());
        for (field, found) in self.table.fields().iter().zip(matches) {
            let path = vec![field.name().clone()];
            positions.push(field_changes(
                field,
                found,
                table.fields(),
                &path,
                case_sensitive,
                type_name,
                &mut changes,
            ));
        }
        if !changes.is_empty() {
            return Err(changes);
        }
        self.columns
            .iter()
            .map(|&i| positions[i].ok_or_else(Vec::new))
            .collect()
    }

    /// Returns the plan of a scan of the current table with the output of the stored scan:
    /// the names that the view reads, and the metadata that clients see in its schema.
    fn with_stored_output(&self, scan: TableScan) -> PlanResult<LogicalPlan> {
        let fields = scan.projected_schema.fields();
        let same = self
            .schema
            .fields()
            .iter()
            .zip(fields)
            .all(|(stored, field)| {
                stored.name() == field.name() && !user_metadata_differs(stored, field)
            });
        if same {
            return Ok(LogicalPlan::TableScan(scan));
        }
        let expressions = self
            .schema
            .fields()
            .iter()
            .zip(fields)
            .map(|(stored, field)| {
                let column = Expr::Column(Column::new(
                    Some(scan.table_name.clone()),
                    field.name().clone(),
                ));
                let expression = if user_metadata_differs(stored, field) {
                    let data_type = if can_cast_types(field.data_type(), stored.data_type()) {
                        stored.data_type().clone()
                    } else {
                        field.data_type().clone()
                    };
                    let target = stored.as_ref().clone().with_data_type(data_type);
                    Expr::Cast(Cast::new_from_field(Box::new(column), Arc::new(target)))
                } else {
                    column
                };
                expression.alias_qualified(Some(scan.table_name.clone()), stored.name())
            })
            .collect();
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

/// Returns the logical type metadata of a field that differs from that of another field.
pub(super) fn logical_type_change<'a>(
    stored: &'a Field,
    current: &'a Field,
) -> Option<(Option<&'a String>, Option<&'a String>)> {
    LOGICAL_TYPE_METADATA_KEYS.iter().find_map(|key| {
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
    })
}

/// Returns whether the metadata that clients see in a schema differs, at any level.
pub(super) fn user_metadata_differs(stored: &Field, current: &Field) -> bool {
    if stored.metadata().get(SPARK_METADATA_JSON_KEY)
        != current.metadata().get(SPARK_METADATA_JSON_KEY)
    {
        return true;
    }
    match (stored.data_type(), current.data_type()) {
        (DataType::Struct(stored), DataType::Struct(current)) => stored.iter().any(|field| {
            current
                .iter()
                .find(|x| x.name().to_lowercase() == field.name().to_lowercase())
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
    if let Some((a, b)) = logical_type_change(stored, current) {
        let describe = |field: &Field, value: Option<&String>| match value {
            Some(value) => format!("{} {value}", type_name(field.data_type())),
            None => type_name(field.data_type()),
        };
        changes.push(format!(
            "{} type has changed from {} to {}",
            quote_path(path),
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
    case_sensitive: bool,
    type_name: &dyn Fn(&DataType) -> String,
    changes: &mut Vec<String>,
) -> Option<usize> {
    match found {
        FieldMatch::Found(position) => {
            let other = &current[position];
            column_changes(
                (field.data_type(), field.is_nullable()),
                (other.data_type(), other.is_nullable()),
                path,
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
                quote_path(path),
                names.join(", ")
            ));
            None
        }
    }
}

fn same_name(a: &str, b: &str, case_sensitive: bool) -> bool {
    a == b || (!case_sensitive && a.to_lowercase() == b.to_lowercase())
}

fn quote_path(path: &[String]) -> String {
    path.iter()
        .map(|x| format!("`{}`", x.replace('`', "``")))
        .collect::<Vec<_>>()
        .join(".")
}

fn describe_field(
    path: &[String],
    field: &Field,
    type_name: &dyn Fn(&DataType) -> String,
) -> String {
    let null = if field.is_nullable() { "" } else { " NOT NULL" };
    format!(
        "{} {}{null}",
        quote_path(path),
        type_name(field.data_type())
    )
}

/// Adds the changes of a column or of a nested field from the type and the nullability
/// that a view was created with, to those of the table now. The metadata of fields and the
/// names of the fields that Spark does not name (the elements of lists and the entries
/// of maps) are not compared.
fn column_changes(
    (stored, stored_nullable): (&DataType, bool),
    (current, current_nullable): (&DataType, bool),
    path: &[String],
    case_sensitive: bool,
    type_name: &dyn Fn(&DataType) -> String,
    changes: &mut Vec<String>,
) {
    if stored_nullable && !current_nullable {
        changes.push(format!("{} is no longer nullable", quote_path(path)));
    } else if !stored_nullable && current_nullable {
        changes.push(format!("{} is nullable now", quote_path(path)));
    }
    let child = |name: &str| {
        let mut path = path.to_vec();
        path.push(name.to_string());
        path
    };
    let nested = |stored: &Field, current: &Field, name: &str, changes: &mut Vec<String>| {
        let path = child(name);
        column_changes(
            (stored.data_type(), stored.is_nullable()),
            (current.data_type(), current.is_nullable()),
            &path,
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
                field_changes(
                    field,
                    found,
                    current,
                    &child(field.name()),
                    case_sensitive,
                    type_name,
                    changes,
                );
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
        (DataType::List(stored), DataType::List(current))
        | (DataType::LargeList(stored), DataType::LargeList(current))
        | (DataType::ListView(stored), DataType::ListView(current))
        | (DataType::LargeListView(stored), DataType::LargeListView(current)) => {
            nested(stored, current, "element", changes)
        }
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
                (stored, false),
                (current, false),
                path,
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
        _ if stored != current => changes.push(format!(
            "{} type has changed from {} to {}",
            quote_path(path),
            type_name(stored),
            type_name(current)
        )),
        _ => {}
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
        state: &PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let mut found = false;
        visit_scans(&plan, &mut |scan| {
            found |= state.table_origin(&scan.source).is_some();
            Ok(())
        })?;
        if !found {
            return Ok(plan);
        }
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
                Ok(match state.table_origin(&scan.source) {
                    Some(origin) => Transformed::yes(LogicalPlan::TableScan(
                        ViewTableReference::store(scan, origin)
                            .map_err(|e| datafusion_common::DataFusionError::External(e.into()))?,
                    )),
                    None => Transformed::no(LogicalPlan::TableScan(scan)),
                })
            },
            false,
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
        if references.is_empty() {
            return if self.share_ctes {
                self.renew_shared_cte_definitions(plan, state)
            } else {
                Self::inline_shared_cte_references(plan)
            };
        }
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
            let mut definitions = vec![];
            let plan = renew_cte_definitions_with_scans(
                plan,
                &mut || state.next_cte_definition_index(),
                &mut |definition| definitions.push(definition),
                &mut bind,
                recompute_schemas,
            )?;
            for definition in definitions {
                state.register_shared_cte_definition(definition);
            }
            plan
        } else {
            let plan = inline_references(plan, &|_| true)?
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
        debug_assert!(!Self::has_view_table_reference(&plan)?);
        Ok(plan)
    }

    fn has_view_table_reference(plan: &LogicalPlan) -> PlanResult<bool> {
        let mut found = false;
        visit_scans(plan, &mut |scan| {
            found |= ViewTableReference::try_from_scan(scan).is_some();
            Ok(())
        })?;
        Ok(found)
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
            for (&position, field) in positions.iter().zip(reference.schema.fields()) {
                names[position] = field.name().clone();
            }
            let mut seen = positions
                .iter()
                .map(|&i| names[i].clone())
                .collect::<HashSet<_>>();
            for (i, name) in names.iter_mut().enumerate() {
                if !positions.contains(&i) && !seen.insert(name.clone()) {
                    *name = state.register_field_name(name.clone());
                }
            }
            let provider = source_as_provider(&source).map_err(|e| {
                PlanError::unsupported(format!(
                    "duplicate column names require DefaultTableSource-backed TableProvider: {e}"
                ))
            })?;
            provider_as_source(Arc::new(RenameTableProvider::try_new(provider, names)?))
        } else {
            source
        };
        // A scan of all the columns of the table in their order has no projection, as the
        // scan of a direct read.
        let projection = (positions.len() != table_schema.fields().len()
            || positions.iter().enumerate().any(|(i, &x)| i != x))
        .then_some(positions);
        // A view that is created over this view reads the table again as well.
        state.record_table_origin(&source, Arc::clone(&reference.origin));
        let bound = TableScanBuilder::new(scan.table_name.clone(), source)
            .with_projection(projection)
            .with_filters(scan.filters.clone())
            .with_fetch(scan.fetch)
            .build()?;
        reference.with_stored_output(bound)
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
                        Ok((status, resolved)) => {
                            let lookup = (status, Some(resolved));
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
            },
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
                reference: &origin.name,
                session_table,
                lake_relation,
                options: origin.options.clone(),
                temporal_options: origin.temporal_options.clone(),
                read_case_sensitive: origin.read_case_sensitive,
            },
            state,
        )
        .await
    }
}

/// Returns the quoted name of a temporary view.
pub(super) fn view_name(view: &TableStatus) -> String {
    let mut name = view.database.clone();
    name.push(view.name.clone());
    quote_names(&name)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{
        DataType, Field, Fields, IntervalUnit, Schema, TimeUnit, UnionFields, UnionMode,
    };

    use super::{TableOrigin, ViewTableReference, user_metadata_differs};
    use crate::resolver::query::temp_view::CatalogTableOrigin;

    fn reference(schema: &Schema) -> ViewTableReference {
        ViewTableReference {
            schema: Arc::new(schema.clone()),
            table: Arc::new(schema.clone()),
            columns: (0..schema.fields().len()).collect(),
            origin: Arc::new(TableOrigin::CatalogTable(CatalogTableOrigin {
                name: vec![],
                session_table: None,
                lake_relation: None,
                format: String::new(),
                options: vec![],
                temporal_options: vec![],
                read_case_sensitive: false,
            })),
        }
    }

    fn type_name(data_type: &DataType) -> String {
        format!("{data_type}")
    }

    fn match_columns(
        stored: &Schema,
        table: &Schema,
        case_sensitive: bool,
    ) -> Result<Vec<usize>, Vec<String>> {
        reference(stored).match_columns(table, case_sensitive, &type_name)
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

    #[test]
    fn test_columns_of_each_type_match_themselves_and_no_other_type() {
        let types = data_types();
        for (i, stored) in types.iter().enumerate() {
            for (j, current) in types.iter().enumerate() {
                let result = match_columns(
                    &Schema::new(vec![field("c", stored.clone())]),
                    &Schema::new(vec![field("c", current.clone())]),
                    true,
                );
                if i == j {
                    assert_eq!(result, Ok(vec![0]), "{stored}");
                } else {
                    assert!(result.is_err(), "{stored} and {current}");
                }
            }
        }
    }

    #[test]
    fn test_nullability_changes_of_each_type_are_column_changes() {
        for data_type in data_types() {
            let nullable = Schema::new(vec![field("c", data_type.clone())]);
            let required = Schema::new(vec![Field::new("c", data_type.clone(), false)]);
            assert_eq!(
                match_columns(&nullable, &required, true),
                Err(vec!["`c` is no longer nullable".to_string()]),
                "{data_type}"
            );
            assert_eq!(
                match_columns(&required, &nullable, true),
                Err(vec!["`c` is nullable now".to_string()]),
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
    fn test_all_the_captured_columns_are_validated() {
        let table = Schema::new(vec![
            field("a", DataType::Int32),
            field("b", DataType::Utf8),
        ]);
        let reference = ViewTableReference {
            schema: Arc::new(Schema::new(vec![field("a", DataType::Int32)])),
            columns: vec![0],
            ..reference(&table)
        };
        let current = Schema::new(vec![
            field("b", DataType::Utf8),
            field("a", DataType::Int32),
        ]);
        assert_eq!(
            reference.match_columns(&current, true, &type_name),
            Ok(vec![1])
        );
        let current = Schema::new(vec![field("a", DataType::Int32)]);
        assert_eq!(
            reference.match_columns(&current, true, &type_name),
            Err(vec!["`b` Utf8 has been removed".to_string()])
        );
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
                vec!["`c`.`a` is no longer nullable"],
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
        // The elements of a list and the values of a map.
        let stored = Schema::new(vec![field("c", list(field("item", DataType::Int32)))]);
        let current = Schema::new(vec![field(
            "c",
            list(Field::new("item", DataType::Int32, false)),
        )]);
        assert_eq!(
            match_columns(&stored, &current, true),
            Err(vec!["`c`.`element` is no longer nullable".to_string()])
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
            Err(vec!["`c`.`value` is nullable now".to_string()])
        );
    }
}
