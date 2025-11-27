use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::error::CommonError;
use crate::spec::data_type::Schema;
use crate::spec::expression::{
    CommonInlineUserDefinedFunction, CommonInlineUserDefinedTableFunction, Expr, ObjectName,
    SortOrder,
};
use crate::spec::literal::Literal;
use crate::spec::{DataType, FunctionDefinition, Identifier};

/// Unresolved logical plan node for Sail.
/// As a starting point, the definition matches the structure of the `Relation` message
/// in the Spark Connect protocol, but enum variants are added to include write commands
/// and plan analysis requests in Spark Connect. There are additional variants to
/// represent SQL constructs that are not part of the Spark Connect protocol.
///
/// For the plan nodes originating from the `Relation` message, there are a few minor differences.
///   1. We do not support raw SQL plan/expression strings as a plan node. Such SQL strings
///      should be parsed before creating the plan node.
///   2. We do not support schema strings or field metadata JSON strings. Such strings
///      should be parsed before creating the schema used in the plan node.
///   3. We use "schema" instead of "data type" in the plan node. The "schema" consists of
///      a list of named fields, each with a data type. In contrast, the Spark Connect
///      protocol does not have the concept of a schema, and instead uses a struct data type
///      whenever a schema is needed.
///   4. We do not use abbreviations (e.g. "exprs", "cols", "func", "db", "temp") in names.
///   5. We use Pascal Case for acronyms (e.g. "Na", "Df", "Udf") in enum variant names.
///   6. Some names are modified for naming consistency.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum Plan {
    Query(QueryPlan),
    Command(CommandPlan),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryPlan {
    #[serde(flatten)]
    pub node: QueryNode,
    pub plan_id: Option<i64>,
}

impl QueryPlan {
    pub fn new(node: QueryNode) -> Self {
        Self {
            node,
            plan_id: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommandPlan {
    #[serde(flatten)]
    pub node: CommandNode,
    pub plan_id: Option<i64>,
}

impl CommandPlan {
    pub fn new(node: CommandNode) -> Self {
        Self {
            node,
            plan_id: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum QueryNode {
    Read {
        #[serde(flatten)]
        read_type: ReadType,
        is_streaming: bool,
    },
    Project {
        input: Option<Box<QueryPlan>>,
        expressions: Vec<Expr>,
    },
    Filter {
        input: Box<QueryPlan>,
        condition: Expr,
    },
    Join(Join),
    SetOperation(SetOperation),
    Sort {
        input: Box<QueryPlan>,
        order: Vec<SortOrder>,
        is_global: bool,
    },
    Limit {
        input: Box<QueryPlan>,
        skip: Option<Expr>,
        limit: Option<Expr>,
    },
    Aggregate(Aggregate),
    LocalRelation {
        data: Option<Vec<u8>>,
        schema: Option<Schema>,
    },
    Sample(Sample),
    Deduplicate(Deduplicate),
    Range(Range),
    SubqueryAlias {
        input: Box<QueryPlan>,
        alias: Identifier,
        qualifier: Vec<Identifier>,
    },
    Repartition {
        input: Box<QueryPlan>,
        num_partitions: usize,
        shuffle: bool,
    },
    ToDf {
        input: Box<QueryPlan>,
        column_names: Vec<Identifier>,
    },
    WithColumnsRenamed {
        input: Box<QueryPlan>,
        rename_columns_map: Vec<(Identifier, Identifier)>,
    },
    Drop {
        input: Box<QueryPlan>,
        columns: Vec<Expr>,
        column_names: Vec<Identifier>,
    },
    Tail {
        input: Box<QueryPlan>,
        limit: Expr,
    },
    WithColumns {
        input: Box<QueryPlan>,
        aliases: Vec<Expr>,
    },
    Hint {
        input: Box<QueryPlan>,
        name: String,
        parameters: Vec<Expr>,
    },
    Pivot(Pivot),
    Unpivot(Unpivot),
    ToSchema {
        input: Box<QueryPlan>,
        schema: Schema,
    },
    RepartitionByExpression {
        input: Box<QueryPlan>,
        partition_expressions: Vec<Expr>,
        num_partitions: Option<usize>,
    },
    MapPartitions {
        input: Box<QueryPlan>,
        function: CommonInlineUserDefinedFunction,
        is_barrier: bool,
    },
    CollectMetrics {
        input: Box<QueryPlan>,
        name: String,
        metrics: Vec<Expr>,
    },
    Parse(Parse),
    GroupMap(GroupMap),
    CoGroupMap(CoGroupMap),
    WithWatermark(WithWatermark),
    ApplyInPandasWithState(ApplyInPandasWithState),
    CachedLocalRelation {
        hash: String,
    },
    CachedRemoteRelation {
        relation_id: String,
    },
    CommonInlineUserDefinedTableFunction(CommonInlineUserDefinedTableFunction),
    // NA operations
    FillNa {
        input: Box<QueryPlan>,
        columns: Vec<Identifier>,
        values: Vec<Expr>,
    },
    DropNa {
        input: Box<QueryPlan>,
        columns: Vec<Identifier>,
        min_non_nulls: Option<usize>,
    },
    Replace {
        input: Box<QueryPlan>,
        columns: Vec<Identifier>,
        replacements: Vec<Replacement>,
    },
    // stat operations
    StatSummary {
        input: Box<QueryPlan>,
        statistics: Vec<String>,
    },
    StatDescribe {
        input: Box<QueryPlan>,
        columns: Vec<Identifier>,
    },
    StatCrosstab {
        input: Box<QueryPlan>,
        left_column: Identifier,
        right_column: Identifier,
    },
    StatCov {
        input: Box<QueryPlan>,
        left_column: Identifier,
        right_column: Identifier,
    },
    StatCorr {
        input: Box<QueryPlan>,
        left_column: Identifier,
        right_column: Identifier,
        method: String,
    },
    StatApproxQuantile {
        input: Box<QueryPlan>,
        columns: Vec<Identifier>,
        probabilities: Vec<f64>,
        relative_error: f64,
    },
    StatFreqItems {
        input: Box<QueryPlan>,
        columns: Vec<Identifier>,
        support: Option<f64>,
    },
    StatSampleBy {
        input: Box<QueryPlan>,
        column: Expr,
        fractions: Vec<Fraction>,
        seed: Option<i64>,
    },
    // extensions
    Empty {
        produce_one_row: bool,
    },
    WithParameters {
        input: Box<QueryPlan>,
        positional_arguments: Vec<Expr>,
        named_arguments: Vec<(String, Expr)>,
    },
    Values(Vec<Vec<Expr>>),
    TableAlias {
        input: Box<QueryPlan>,
        name: Identifier,
        columns: Vec<Identifier>,
    },
    WithCtes {
        input: Box<QueryPlan>,
        recursive: bool,
        ctes: Vec<(Identifier, QueryPlan)>,
    },
    LateralView {
        input: Option<Box<QueryPlan>>,
        function: ObjectName,
        arguments: Vec<Expr>,
        named_arguments: Vec<(Identifier, Expr)>,
        table_alias: Option<ObjectName>,
        column_aliases: Option<Vec<Identifier>>,
        outer: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum CommandNode {
    ShowString(ShowString),
    HtmlString(HtmlString),
    // TODO: add all the "analyze" requests
    // TODO: should streaming query request be added here?
    // catalog operations
    CurrentDatabase,
    SetCurrentDatabase {
        database: ObjectName,
    },
    ListDatabases {
        qualifier: Option<ObjectName>,
        pattern: Option<String>,
    },
    ListTables {
        database: Option<ObjectName>,
        pattern: Option<String>,
    },
    ListViews {
        database: Option<ObjectName>,
        pattern: Option<String>,
    },
    ListFunctions {
        database: Option<ObjectName>,
        pattern: Option<String>,
    },
    ListColumns {
        table: ObjectName,
    },
    GetDatabase {
        database: ObjectName,
    },
    GetTable {
        table: ObjectName,
    },
    GetFunction {
        function: ObjectName,
    },
    DatabaseExists {
        database: ObjectName,
    },
    TableExists {
        table: ObjectName,
    },
    FunctionExists {
        function: ObjectName,
    },
    CreateTable {
        table: ObjectName,
        #[serde(flatten)]
        definition: TableDefinition,
    },
    CreateTableAsSelect {
        table: ObjectName,
        #[serde(flatten)]
        definition: TableDefinition,
        /// The query for `CREATE TABLE ... AS SELECT ...` (CTAS) statements.
        query: Box<QueryPlan>,
    },
    RecoverPartitions {
        table: ObjectName,
    },
    IsCached {
        table: ObjectName,
    },
    CacheTable {
        table: ObjectName,
        lazy: bool,
        storage_level: Option<StorageLevel>,
        query: Option<Box<QueryPlan>>,
    },
    UncacheTable {
        table: ObjectName,
        if_exists: bool,
    },
    ClearCache,
    RefreshTable {
        table: ObjectName,
    },
    RefreshByPath {
        path: String,
    },
    CurrentCatalog,
    SetCurrentCatalog {
        catalog: Identifier,
    },
    ListCatalogs {
        pattern: Option<String>,
    },
    CreateCatalog {
        catalog: Identifier,
        #[serde(flatten)]
        definition: CatalogDefinition,
    },
    // commands
    CreateDatabase {
        database: ObjectName,
        #[serde(flatten)]
        definition: DatabaseDefinition,
    },
    DropDatabase {
        database: ObjectName,
        if_exists: bool,
        cascade: bool,
    },
    RegisterFunction(CommonInlineUserDefinedFunction),
    RegisterTableFunction(CommonInlineUserDefinedTableFunction),
    RefreshFunction {
        function: ObjectName,
    },
    DropFunction {
        function: ObjectName,
        if_exists: bool,
        is_temporary: bool,
    },
    DropTable {
        table: ObjectName,
        if_exists: bool,
        purge: bool,
    },
    CreateView {
        view: ObjectName,
        #[serde(flatten)]
        definition: ViewDefinition,
    },
    /// Drop a view or a temporary view.
    DropView {
        view: ObjectName,
        if_exists: bool,
    },
    CreateTemporaryView {
        view: Identifier,
        is_global: bool,
        #[serde(flatten)]
        definition: TemporaryViewDefinition,
    },
    DropTemporaryView {
        view: Identifier,
        is_global: bool,
        if_exists: bool,
    },
    Write(Write),
    WriteTo(WriteTo),
    WriteStream(WriteStream),
    Explain {
        // TODO: Support stringified_plans
        mode: ExplainMode,
        input: Box<Plan>,
    },
    InsertInto {
        input: Box<QueryPlan>,
        table: ObjectName,
        mode: InsertMode,
        partition: Vec<(Identifier, Option<Expr>)>,
        if_not_exists: bool,
    },
    InsertOverwriteDirectory {
        input: Box<QueryPlan>,
        local: bool,
        location: Option<String>,
        file_format: Option<TableFileFormat>,
        row_format: Option<TableRowFormat>,
        options: Vec<(String, String)>,
    },
    MergeInto(MergeInto),
    SetVariable {
        variable: String,
        value: String,
    },
    Update {
        table: ObjectName,
        table_alias: Option<Identifier>,
        assignments: Vec<(ObjectName, Expr)>,
        condition: Option<Expr>,
    },
    Delete {
        table: ObjectName,
        table_alias: Option<Identifier>,
        condition: Option<Expr>,
    },
    AlterTable {
        table: ObjectName,
        if_exists: bool,
        operation: AlterTableOperation,
    },
    AlterView {
        view: ObjectName,
        if_exists: bool,
        operation: AlterViewOperation,
    },
    LoadData {
        local: bool,
        location: String,
        table: ObjectName,
        overwrite: bool,
        partition: Vec<(Identifier, Option<Expr>)>,
    },
    AnalyzeTable {
        table: ObjectName,
        partition: Vec<(Identifier, Option<Expr>)>,
        columns: Vec<ObjectName>,
        no_scan: bool,
    },
    AnalyzeTables {
        from: Option<ObjectName>,
        no_scan: bool,
    },
    DescribeQuery {
        query: Box<QueryPlan>,
    },
    DescribeFunction {
        function: ObjectName,
        extended: bool,
    },
    DescribeCatalog {
        catalog: ObjectName,
        extended: bool,
    },
    DescribeDatabase {
        database: ObjectName,
        extended: bool,
    },
    DescribeTable {
        table: ObjectName,
        extended: bool,
        partition: Vec<(Identifier, Option<Expr>)>,
        column: Option<ObjectName>,
    },
    CommentOnCatalog {
        catalog: ObjectName,
        value: Option<String>,
    },
    CommentOnDatabase {
        database: ObjectName,
        value: Option<String>,
    },
    CommentOnTable {
        table: ObjectName,
        value: Option<String>,
    },
    CommentOnColumn {
        column: ObjectName,
        value: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MergeInto {
    pub target: ObjectName,
    pub target_alias: Option<Identifier>,
    pub source: MergeSource,
    pub on_condition: Expr,
    pub clauses: Vec<MergeClause>,
    pub with_schema_evolution: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum MergeSource {
    Table {
        name: ObjectName,
        alias: Option<Identifier>,
    },
    Query {
        input: Box<QueryPlan>,
        alias: Option<Identifier>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum MergeClause {
    Matched(MergeMatchedClause),
    NotMatchedBySource(MergeNotMatchedBySourceClause),
    NotMatchedByTarget(MergeNotMatchedByTargetClause),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MergeMatchedClause {
    pub condition: Option<Expr>,
    pub action: MergeMatchedAction,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum MergeMatchedAction {
    Delete,
    UpdateAll,
    UpdateSet(Vec<(ObjectName, Expr)>),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MergeNotMatchedBySourceClause {
    pub condition: Option<Expr>,
    pub action: MergeNotMatchedBySourceAction,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum MergeNotMatchedBySourceAction {
    Delete,
    UpdateSet(Vec<(ObjectName, Expr)>),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MergeNotMatchedByTargetClause {
    pub condition: Option<Expr>,
    pub action: MergeNotMatchedByTargetAction,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum MergeNotMatchedByTargetAction {
    InsertAll,
    InsertColumns {
        columns: Vec<ObjectName>,
        values: Vec<Expr>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
#[allow(clippy::large_enum_variant)]
pub enum ReadType {
    // FIXME: Rust 1.87 triggers `clippy::large_enum_variant` warning
    NamedTable(ReadNamedTable),
    Udtf(ReadUdtf),
    DataSource(ReadDataSource),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadNamedTable {
    pub name: ObjectName,
    pub temporal: Option<TableTemporal>,
    pub sample: Option<TableSample>,
    pub options: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum TableTemporal {
    Version { value: Expr },
    Timestamp { value: Expr },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableSample {
    pub method: TableSampleMethod,
    pub seed: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum TableSampleMethod {
    Percent {
        value: Expr,
    },
    Rows {
        value: Expr,
    },
    Bucket {
        numerator: usize,
        denominator: usize,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadUdtf {
    pub name: ObjectName,
    pub arguments: Vec<Expr>,
    pub named_arguments: Vec<(Identifier, Expr)>,
    pub options: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadDataSource {
    pub format: Option<String>,
    pub schema: Option<Schema>,
    pub options: Vec<(String, String)>,
    pub paths: Vec<String>,
    pub predicates: Vec<Expr>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Join {
    pub left: Box<QueryPlan>,
    pub right: Box<QueryPlan>,
    pub join_type: JoinType,
    pub join_criteria: Option<JoinCriteria>,
    pub join_data_type: Option<JoinDataType>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetOperation {
    pub left: Box<QueryPlan>,
    pub right: Box<QueryPlan>,
    pub set_op_type: SetOpType,
    pub is_all: bool,
    pub by_name: bool,
    pub allow_missing_columns: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Aggregate {
    pub input: Box<QueryPlan>,
    pub grouping: Vec<Expr>,
    pub aggregate: Vec<Expr>,
    pub having: Option<Expr>,
    /// Whether the grouping expressions should be added to the projection.
    pub with_grouping_expressions: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Sample {
    pub input: Box<QueryPlan>,
    pub lower_bound: f64,
    pub upper_bound: f64,
    pub with_replacement: bool,
    pub seed: Option<i64>,
    pub deterministic_order: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Deduplicate {
    pub input: Box<QueryPlan>,
    pub column_names: Vec<Identifier>,
    pub all_columns_as_keys: bool,
    pub within_watermark: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Range {
    pub start: Option<i64>,
    pub end: i64,
    pub step: i64,
    pub num_partitions: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ShowString {
    pub input: Box<QueryPlan>,
    pub num_rows: usize,
    pub truncate: usize,
    pub vertical: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Pivot {
    pub input: Box<QueryPlan>,
    /// The group-by columns for the pivot operation (only supported in the DataFrame API).
    /// When the list is empty (for SQL statements), all the remaining columns are included.
    pub grouping: Vec<Expr>,
    pub aggregate: Vec<Expr>,
    pub columns: Vec<Expr>,
    pub values: Vec<PivotValue>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PivotValue {
    pub values: Vec<Literal>,
    pub alias: Option<Identifier>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Unpivot {
    pub input: Box<QueryPlan>,
    /// When `ids` is [None] (for SQL statements), all remaining columns are included.
    /// When `ids` is [Some] (for the DataFrame API), only the specified columns are included.
    pub ids: Option<Vec<Expr>>,
    pub values: Option<Vec<UnpivotValue>>,
    pub variable_column_name: Identifier,
    pub value_column_names: Vec<Identifier>,
    pub include_nulls: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnpivotValue {
    pub columns: Vec<Expr>,
    pub alias: Option<Identifier>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Parse {
    pub input: Box<QueryPlan>,
    pub format: ParseFormat,
    pub schema: Option<Schema>,
    pub options: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupMap {
    pub input: Box<QueryPlan>,
    pub grouping_expressions: Vec<Expr>,
    pub function: CommonInlineUserDefinedFunction,
    pub sorting_expressions: Vec<Expr>,
    pub initial_input: Option<Box<QueryPlan>>,
    pub initial_grouping_expressions: Vec<Expr>,
    pub is_map_groups_with_state: Option<bool>,
    pub output_mode: Option<String>,
    pub timeout_conf: Option<String>,
    pub state_schema: Option<Schema>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CoGroupMap {
    pub input: Box<QueryPlan>,
    pub input_grouping_expressions: Vec<Expr>,
    pub other: Box<QueryPlan>,
    pub other_grouping_expressions: Vec<Expr>,
    pub function: CommonInlineUserDefinedFunction,
    pub input_sorting_expressions: Vec<Expr>,
    pub other_sorting_expressions: Vec<Expr>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WithWatermark {
    pub input: Box<QueryPlan>,
    pub event_time: String,
    pub delay_threshold: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApplyInPandasWithState {
    pub input: Box<QueryPlan>,
    pub grouping_expressions: Vec<Expr>,
    pub function: CommonInlineUserDefinedFunction,
    pub output_schema: Schema,
    pub state_schema: Schema,
    pub output_mode: String,
    pub timeout_conf: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HtmlString {
    pub input: Box<QueryPlan>,
    pub num_rows: usize,
    pub truncate: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableDefinition {
    pub columns: Vec<TableColumnDefinition>,
    pub comment: Option<String>,
    pub constraints: Vec<TableConstraint>,
    pub location: Option<String>,
    pub file_format: Option<TableFileFormat>,
    pub row_format: Option<TableRowFormat>,
    pub partition_by: Vec<Identifier>,
    pub sort_by: Vec<SortOrder>,
    pub bucket_by: Option<SaveBucketBy>,
    pub cluster_by: Vec<ObjectName>,
    pub if_not_exists: bool,
    pub replace: bool,
    pub options: Vec<(String, String)>,
    pub properties: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableColumnDefinition {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub comment: Option<String>,
    /// An optional SQL expression string for the default value.
    pub default: Option<String>,
    /// An optional SQL expression string to calculate the generated value.
    pub generated_always_as: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseDefinition {
    pub if_not_exists: bool,
    pub comment: Option<String>,
    pub location: Option<String>,
    pub properties: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CatalogDefinition {
    pub options: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ViewDefinition {
    /// The corresponding SQL query that defines the view.
    pub definition: String,
    pub columns: Option<Vec<ViewColumnDefinition>>,
    pub if_not_exists: bool,
    pub replace: bool,
    pub comment: Option<String>,
    pub properties: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ViewColumnDefinition {
    pub name: String,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TemporaryViewDefinition {
    pub input: Box<QueryPlan>,
    pub columns: Option<Vec<ViewColumnDefinition>>,
    pub if_not_exists: bool,
    pub replace: bool,
    pub comment: Option<String>,
    pub properties: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Write {
    pub input: Box<QueryPlan>,
    pub source: Option<String>,
    pub save_type: SaveType,
    pub mode: Option<SaveMode>,
    pub sort_columns: Vec<SortOrder>,
    pub partitioning_columns: Vec<Identifier>,
    pub clustering_columns: Vec<Identifier>,
    pub bucket_by: Option<SaveBucketBy>,
    pub options: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum SaveType {
    Path(String),
    Table {
        table: ObjectName,
        save_method: TableSaveMethod,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TableSaveMethod {
    SaveAsTable,
    InsertInto,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SaveBucketBy {
    pub bucket_column_names: Vec<Identifier>,
    pub num_buckets: usize,
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SaveMode {
    Append,
    Overwrite,
    #[default]
    ErrorIfExists,
    IgnoreIfExists,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WriteTo {
    pub input: Box<QueryPlan>,
    pub provider: Option<String>,
    pub table: ObjectName,
    pub mode: WriteToMode,
    pub partitioning_columns: Vec<Expr>,
    pub clustering_columns: Vec<Identifier>,
    pub options: Vec<(String, String)>,
    pub table_properties: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum WriteToMode {
    Append,
    Create,
    CreateOrReplace,
    Overwrite { condition: Box<Expr> },
    OverwritePartitions,
    Replace,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WriteStream {
    pub input: Box<QueryPlan>,
    pub format: String,
    pub options: Vec<(String, String)>,
    pub partitioning_column_names: Vec<Identifier>,
    pub query_name: String,
    pub foreach_writer: Option<FunctionDefinition>,
    pub foreach_batch: Option<FunctionDefinition>,
    pub clustering_column_names: Vec<Identifier>,
    pub sink_destination: Option<WriteStreamSinkDestination>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum WriteStreamSinkDestination {
    Path { path: String },
    Table { table: ObjectName },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum InsertMode {
    InsertByPosition {
        overwrite: bool,
    },
    InsertByName {
        overwrite: bool,
    },
    InsertByColumns {
        columns: Vec<Identifier>,
        overwrite: bool,
    },
    Replace {
        condition: Box<Expr>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum JoinType {
    Inner,
    FullOuter,
    LeftOuter,
    RightOuter,
    LeftSemi,
    RightSemi,
    LeftAnti,
    RightAnti,
    Cross,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum JoinCriteria {
    Natural,
    On(Expr),
    Using(Vec<Identifier>),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JoinDataType {
    pub is_left_struct: bool,
    pub is_right_struct: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SetOpType {
    Intersect,
    Union,
    Except,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ParseFormat {
    Unspecified,
    Csv,
    Json,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Fraction {
    pub stratum: Literal,
    pub fraction: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Replacement {
    pub old_value: Literal,
    pub new_value: Literal,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StorageLevel {
    pub use_disk: bool,
    pub use_memory: bool,
    pub use_off_heap: bool,
    pub deserialized: bool,
    pub replication: usize,
}

impl FromStr for StorageLevel {
    type Err = CommonError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_uppercase().as_str() {
            "NONE" => Ok(Self {
                use_disk: false,
                use_memory: false,
                use_off_heap: false,
                deserialized: false,
                replication: 1,
            }),
            "DISK_ONLY" => Ok(Self {
                use_disk: true,
                use_memory: false,
                use_off_heap: false,
                deserialized: false,
                replication: 1,
            }),
            "DISK_ONLY_2" => Ok(Self {
                use_disk: true,
                use_memory: false,
                use_off_heap: false,
                deserialized: false,
                replication: 2,
            }),
            "DISK_ONLY_3" => Ok(Self {
                use_disk: true,
                use_memory: false,
                use_off_heap: false,
                deserialized: false,
                replication: 3,
            }),
            "MEMORY_ONLY" => Ok(Self {
                use_disk: false,
                use_memory: true,
                use_off_heap: false,
                deserialized: true,
                replication: 1,
            }),
            "MEMORY_ONLY_2" => Ok(Self {
                use_disk: false,
                use_memory: true,
                use_off_heap: false,
                deserialized: true,
                replication: 2,
            }),
            "MEMORY_ONLY_SER" => Ok(Self {
                use_disk: false,
                use_memory: true,
                use_off_heap: false,
                deserialized: false,
                replication: 1,
            }),
            "MEMORY_ONLY_SER_2" => Ok(Self {
                use_disk: false,
                use_memory: true,
                use_off_heap: false,
                deserialized: false,
                replication: 2,
            }),
            "MEMORY_AND_DISK" => Ok(Self {
                use_disk: true,
                use_memory: true,
                use_off_heap: false,
                deserialized: true,
                replication: 1,
            }),
            "MEMORY_AND_DISK_2" => Ok(Self {
                use_disk: true,
                use_memory: true,
                use_off_heap: false,
                deserialized: true,
                replication: 2,
            }),
            "MEMORY_AND_DISK_SER" => Ok(Self {
                use_disk: true,
                use_memory: true,
                use_off_heap: false,
                deserialized: false,
                replication: 1,
            }),
            "MEMORY_AND_DISK_SER_2" => Ok(Self {
                use_disk: true,
                use_memory: true,
                use_off_heap: false,
                deserialized: false,
                replication: 2,
            }),
            "OFF_HEAP" => Ok(Self {
                use_disk: true,
                use_memory: true,
                use_off_heap: true,
                deserialized: false,
                replication: 1,
            }),
            _ => Err(CommonError::invalid(format!("storage level: {s}"))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TableConstraint {
    Unique {
        name: Option<Identifier>,
        columns: Vec<Identifier>,
    },
    PrimaryKey {
        name: Option<Identifier>,
        columns: Vec<Identifier>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TableFileFormat {
    General {
        format: String,
    },
    Table {
        input_format: String,
        output_format: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TableRowFormat {
    Serde {
        name: String,
        properties: Vec<(String, String)>,
    },
    Delimited {
        fields_terminated_by: Option<String>,
        fields_escaped_by: Option<String>,
        collection_items_terminated_by: Option<String>,
        map_keys_terminated_by: Option<String>,
        lines_terminated_by: Option<String>,
        null_defined_as: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ExplainMode {
    Unspecified,
    Simple,
    Analyze,
    Verbose,
    Extended,
    Codegen,
    Cost,
    Formatted,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AlterTableOperation {
    Unknown,
    // TODO: add all the alter table operations
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AlterViewOperation {
    Unknown,
    // TODO: add all the alter view operations
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Delete {
    pub table: ObjectName,
    pub table_alias: Option<Identifier>,
    pub condition: Option<Expr>,
}
