use crate::spec::data_type::Schema;
use crate::spec::expression::{
    CommonInlineUserDefinedFunction, CommonInlineUserDefinedTableFunction, Expr, ObjectName,
    SortOrder,
};
use crate::spec::literal::Literal;
use crate::spec::Identifier;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Plan {
    #[serde(flatten)]
    pub node: PlanNode,
    pub plan_id: Option<i64>,
    pub source_info: Option<String>,
}

impl Plan {
    pub fn new(node: PlanNode) -> Self {
        Self {
            node,
            plan_id: None,
            source_info: None,
        }
    }
}

/// Unresolved logical plan node for the framework.
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
pub enum PlanNode {
    Read {
        #[serde(flatten)]
        read_type: ReadType,
        is_streaming: bool,
    },
    Project {
        input: Option<Box<Plan>>,
        expressions: Vec<Expr>,
    },
    Filter {
        input: Box<Plan>,
        condition: Expr,
    },
    Join {
        left: Box<Plan>,
        right: Box<Plan>,
        join_condition: Option<Expr>,
        join_type: JoinType,
        using_columns: Vec<Identifier>,
        join_data_type: Option<JoinDataType>,
    },
    SetOperation {
        left: Box<Plan>,
        right: Box<Plan>,
        set_op_type: SetOpType,
        is_all: bool,
        by_name: bool,
        allow_missing_columns: bool,
    },
    Sort {
        input: Box<Plan>,
        order: Vec<SortOrder>,
        is_global: bool,
    },
    Limit {
        input: Box<Plan>,
        limit: usize,
    },
    Aggregate {
        input: Box<Plan>,
        group_type: GroupType,
        grouping_expressions: Vec<Expr>,
        aggregate_expressions: Vec<Expr>,
        pivot: Option<Pivot>,
    },
    LocalRelation {
        data: Option<Vec<u8>>,
        schema: Option<Schema>,
    },
    Sample {
        input: Box<Plan>,
        lower_bound: f64,
        upper_bound: f64,
        with_replacement: bool,
        seed: Option<i64>,
        deterministic_order: bool,
    },
    Offset {
        input: Box<Plan>,
        offset: usize,
    },
    Deduplicate {
        input: Box<Plan>,
        column_names: Vec<Identifier>,
        all_columns_as_keys: bool,
        within_watermark: bool,
    },
    Range {
        start: Option<i64>,
        end: i64,
        step: i64,
        num_partitions: Option<usize>,
    },
    SubqueryAlias {
        input: Box<Plan>,
        alias: Identifier,
        qualifier: Vec<Identifier>,
    },
    Repartition {
        input: Box<Plan>,
        num_partitions: usize,
        shuffle: bool,
    },
    ToDf {
        input: Box<Plan>,
        column_names: Vec<Identifier>,
    },
    WithColumnsRenamed {
        input: Box<Plan>,
        rename_columns_map: HashMap<Identifier, Identifier>,
    },
    ShowString {
        input: Box<Plan>,
        num_rows: usize,
        truncate: usize,
        vertical: bool,
    },
    Drop {
        input: Box<Plan>,
        columns: Vec<Expr>,
        column_names: Vec<Identifier>,
    },
    Tail {
        input: Box<Plan>,
        limit: usize,
    },
    WithColumns {
        input: Box<Plan>,
        aliases: Vec<Expr>,
    },
    Hint {
        input: Box<Plan>,
        name: String,
        parameters: Vec<Expr>,
    },
    Unpivot {
        input: Box<Plan>,
        ids: Vec<Expr>,
        values: Vec<Expr>,
        variable_column_name: Identifier,
        value_column_name: Identifier,
    },
    ToSchema {
        input: Box<Plan>,
        schema: Schema,
    },
    RepartitionByExpression {
        input: Box<Plan>,
        partition_expressions: Vec<Expr>,
        num_partitions: Option<usize>,
    },
    MapPartitions {
        input: Box<Plan>,
        function: CommonInlineUserDefinedFunction,
        is_barrier: bool,
    },
    CollectMetrics {
        input: Box<Plan>,
        name: String,
        metrics: Vec<Expr>,
    },
    Parse {
        input: Box<Plan>,
        format: ParseFormat,
        schema: Option<Schema>,
        options: HashMap<String, String>,
    },
    GroupMap {
        input: Box<Plan>,
        grouping_expressions: Vec<Expr>,
        function: CommonInlineUserDefinedFunction,
        sorting_expressions: Vec<Expr>,
        initial_input: Option<Box<Plan>>,
        initial_grouping_expressions: Vec<Expr>,
        is_map_groups_with_state: Option<bool>, // TODO: this should probably be an enum
        output_mode: Option<String>,            // TODO: this should probably be an enum
        timeout_conf: Option<String>,           // TODO: this should probably be a struct
    },
    CoGroupMap {
        input: Box<Plan>,
        input_grouping_expressions: Vec<Expr>,
        other: Box<Plan>,
        other_grouping_expressions: Vec<Expr>,
        function: CommonInlineUserDefinedFunction,
        input_sorting_expressions: Vec<Expr>,
        other_sorting_expressions: Vec<Expr>,
    },
    WithWatermark {
        input: Box<Plan>,
        event_time: String,
        delay_threshold: String,
    },
    ApplyInPandasWithState {
        input: Box<Plan>,
        grouping_expressions: Vec<Expr>,
        function: CommonInlineUserDefinedFunction,
        output_schema: Schema,
        state_schema: Schema,
        output_mode: String,  // TODO: this should probably be an enum
        timeout_conf: String, // TODO: this should probably be a struct
    },
    HtmlString {
        input: Box<Plan>,
        num_rows: usize,
        truncate: usize,
    },
    CachedLocalRelation {
        user_id: String,
        session_id: String,
        hash: String,
    },
    CachedRemoteRelation {
        relation_id: String,
    },
    CommonInlineUserDefinedTableFunction(CommonInlineUserDefinedTableFunction),
    // NA operations
    FillNa {
        input: Box<Plan>,
        columns: Vec<Identifier>,
        values: Vec<Expr>,
    },
    DropNa {
        input: Box<Plan>,
        columns: Vec<Identifier>,
        min_non_nulls: Option<usize>,
    },
    ReplaceNa {
        input: Box<Plan>,
        columns: Vec<Identifier>,
        replacements: Vec<Replacement>,
    },
    // stat operations
    StatSummary {
        input: Box<Plan>,
        statistics: Vec<String>,
    },
    StatDescribe {
        input: Box<Plan>,
        columns: Vec<Identifier>,
    },
    StatCrosstab {
        input: Box<Plan>,
        left_column: Identifier,
        right_column: Identifier,
    },
    StatCov {
        input: Box<Plan>,
        left_column: Identifier,
        right_column: Identifier,
    },
    StatCorr {
        input: Box<Plan>,
        left_column: Identifier,
        right_column: Identifier,
        method: String,
    },
    StatApproxQuantile {
        input: Box<Plan>,
        columns: Vec<Identifier>,
        probabilities: Vec<f64>,
        relative_error: f64,
    },
    StatFreqItems {
        input: Box<Plan>,
        columns: Vec<Identifier>,
        support: Option<f64>,
    },
    StatSampleBy {
        input: Box<Plan>,
        column: Expr,
        fractions: Vec<Fraction>,
        seed: Option<i64>,
    },
    // catalog operations
    CurrentDatabase,
    SetCurrentDatabase {
        database_name: Identifier,
    },
    ListDatabases {
        catalog: Option<Identifier>,
        database_pattern: Option<String>,
    },
    ListTables {
        database: Option<ObjectName>,
        table_pattern: Option<String>,
    },
    ListFunctions {
        database: Option<ObjectName>,
        function_pattern: Option<String>,
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
        path: Option<String>,
        source: Option<String>,
        description: Option<String>,
        schema: Option<Schema>,
        options: HashMap<String, String>,
    },
    DropTemporaryView {
        view: ObjectName,
        is_global: bool,
        if_exists: bool,
    },
    RecoverPartitions {
        table: ObjectName,
    },
    IsCached {
        table: ObjectName,
    },
    CacheTable {
        table: ObjectName,
        storage_level: Option<StorageLevel>,
    },
    UncacheTable {
        table: ObjectName,
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
        catalog_name: Identifier,
    },
    ListCatalogs {
        catalog_pattern: Option<String>,
    },
    // commands
    CreateDatabase {
        database: ObjectName,
        if_not_exists: bool,
        comment: Option<String>,
        location: Option<String>,
        properties: HashMap<String, String>,
    },
    DropDatabase {
        database: ObjectName,
        if_exists: bool,
        cascade: bool,
    },
    RegisterFunction(CommonInlineUserDefinedFunction),
    RegisterTableFunction(CommonInlineUserDefinedTableFunction),
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
    CreateTemporaryView {
        view: ObjectName,
        input: Box<Plan>,
        is_global: bool,
        replace: bool,
    },
    DropView {
        view: ObjectName,
        if_exists: bool,
    },
    Write {
        input: Box<Plan>,
        source: Option<String>, // TODO: is this the same as "provider" in `WriteOperationV2`?
        save_type: SaveType,
        mode: SaveMode,
        sort_columns: Vec<Identifier>,
        partitioning_columns: Vec<Identifier>,
        bucket_by: Option<SaveBucketBy>,
        options: HashMap<String, String>,
        table_properties: HashMap<String, String>,
        overwrite_condition: Option<Expr>,
    },
    // TODO: add all the "analyze" requests
    // TODO: should streaming query request be added here?
    // extensions
    Empty {
        produce_one_row: bool,
    },
    WithParameters {
        input: Box<Plan>,
        positional_arguments: Vec<Literal>,
        named_arguments: HashMap<String, Literal>,
    },
    Values(Vec<Vec<Expr>>),
    TableAlias {
        input: Box<Plan>,
        name: Identifier,
        columns: Vec<Identifier>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum ReadType {
    NamedTable {
        identifier: ObjectName,
        options: HashMap<String, String>,
    },
    DataSource {
        format: Option<String>,
        schema: Option<Schema>,
        options: HashMap<String, String>,
        paths: Vec<String>,
        predicates: Vec<Expr>,
    },
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SaveMode {
    // TODO: understand the difference between save modes in
    //  `WriteOperation` and `WriteOperationV2`
    Append,
    Create,
    CreateOrReplace,
    ErrorIfExists,
    Ignore,
    Overwrite,
    OverwritePartitions,
    Replace,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum JoinType {
    Inner,
    FullOuter,
    LeftOuter,
    RightOuter,
    LeftAnti,
    LeftSemi,
    Cross,
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
pub enum GroupType {
    GroupBy,
    Rollup,
    Cube,
    Pivot,
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
pub struct Pivot {
    pub column: Expr,
    pub values: Vec<Literal>,
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
