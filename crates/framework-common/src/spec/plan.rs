use crate::spec::data_type::Schema;
use crate::spec::expression::{
    CommonInlineUserDefinedFunction, CommonInlineUserDefinedTableFunction, Expr, SortOrder,
};
use crate::spec::literal::Literal;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub struct Plan {
    node: PlanNode,
    plan_id: Option<i64>,
    source_info: Option<String>,
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
#[derive(Debug, Clone, PartialEq)]
pub enum PlanNode {
    Read {
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
        using_columns: Vec<String>,
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
        limit: i32,
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
        offset: i32,
    },
    Deduplicate {
        input: Box<Plan>,
        column_names: Vec<String>,
        all_columns_as_keys: bool,
        within_watermark: bool,
    },
    Range {
        start: Option<i64>,
        end: i64,
        step: i64,
        num_partitions: Option<i32>,
    },
    SubqueryAlias {
        input: Box<Plan>,
        alias: String,
        qualifier: Vec<String>,
    },
    Repartition {
        input: Box<Plan>,
        num_partitions: i32,
        shuffle: bool,
    },
    ToDf {
        input: Box<Plan>,
        column_names: Vec<String>,
    },
    WithColumnsRenamed {
        input: Box<Plan>,
        rename_columns_map: HashMap<String, String>,
    },
    ShowString {
        input: Box<Plan>,
        num_rows: i32,
        truncate: i32,
        vertical: bool,
    },
    Drop {
        input: Box<Plan>,
        columns: Vec<Expr>,
        column_names: Vec<String>,
    },
    Tail {
        input: Box<Plan>,
        limit: i32,
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
        variable_column_name: String,
        value_column_name: String,
    },
    ToSchema {
        input: Box<Plan>,
        schema: Schema,
    },
    RepartitionByExpression {
        input: Box<Plan>,
        partition_expressions: Vec<Expr>,
        num_partitions: Option<i32>,
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
        output_mode: String,                    // TODO: this should probably be an enum
        timeout_conf: String,                   // TODO: this should probably be a struct
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
        output_schema: String, // TODO: this should probably be `Schema`
        state_schema: String,  // TODO: this should probably be `Schema`
        output_mode: String,   // TODO: this should probably be an enum
        timeout_conf: String,  // TODO: this should probably be a struct
    },
    HtmlString {
        input: Box<Plan>,
        num_rows: i32,
        truncate: i32,
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
    NaFill {
        input: Box<Plan>,
        columns: Vec<String>,
        values: Vec<Expr>,
    },
    NaDrop {
        input: Box<Plan>,
        columns: Vec<String>,
        min_non_nulls: Option<i32>,
    },
    NaReplace {
        input: Box<Plan>,
        columns: Vec<String>,
        replacements: Vec<Replacement>,
    },
    // stat operations
    StatSummary {
        input: Box<Plan>,
        statistics: Vec<String>,
    },
    StatDescribe {
        input: Box<Plan>,
        columns: Vec<String>,
    },
    StatCrosstab {
        input: Box<Plan>,
        left_column: String,
        right_column: String,
    },
    StatCov {
        input: Box<Plan>,
        left_column: String,
        right_column: String,
    },
    StatCorr {
        input: Box<Plan>,
        left_column: String,
        right_column: String,
        method: String,
    },
    StatApproxQuantile {
        input: Box<Plan>,
        columns: Vec<String>,
        probabilities: Vec<f64>,
        relative_error: f64,
    },
    StatFreqItems {
        input: Box<Plan>,
        columns: Vec<String>,
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
        database_name: String,
    },
    ListDatabases {
        pattern: Option<String>,
    },
    ListTables {
        database_name: Option<String>,
        pattern: Option<String>,
    },
    ListFunctions {
        database_name: Option<String>,
        pattern: Option<String>,
    },
    ListColumns {
        table_name: String,
        database_name: Option<String>,
    },
    GetDatabase {
        database_name: String,
    },
    GetTable {
        table_name: String,
        database_name: Option<String>,
    },
    GetFunction {
        function_name: String,
        database_name: Option<String>,
    },
    DatabaseExists {
        database_name: String,
    },
    TableExists {
        table_name: String,
        database_name: Option<String>,
    },
    FunctionExists {
        function_name: String,
        database_name: Option<String>,
    },
    CreateTable {
        table_name: String,
        path: Option<String>,
        source: Option<String>,
        description: Option<String>,
        schema: Option<Schema>,
        options: HashMap<String, String>,
    },
    DropTemporaryView {
        view_name: String,
    },
    DropGlobalTemporaryView {
        view_name: String,
    },
    RecoverPartitions {
        table_name: String,
    },
    IsCached {
        table_name: String,
    },
    CacheTable {
        table_name: String,
        storage_level: Option<StorageLevel>,
    },
    UncacheTable {
        table_name: String,
    },
    ClearCache,
    RefreshTable {
        table_name: String,
    },
    RefreshByPath {
        path: String,
    },
    CurrentCatalog,
    SetCurrentCatalog {
        catalog_name: String,
    },
    ListCatalogs {
        pattern: Option<String>,
    },
    // commands
    RegisterFunction(CommonInlineUserDefinedFunction),
    RegisterTableFunction(CommonInlineUserDefinedTableFunction),
    CreateTemporaryView {
        input: Box<Plan>,
        name: String,
        is_global: bool,
        replace: bool,
    },
    Write {
        input: Box<Plan>,
        source: Option<String>, // TODO: is this the same as "provider" in `WriteOperationV2`?
        save_type: SaveType,
        mode: SaveMode,
        sort_columns: Vec<String>,
        partitioning_columns: Vec<String>,
        bucket_by: Option<SaveBucketBy>,
        options: HashMap<String, String>,
        table_properties: HashMap<String, String>,
        overwrite_condition: Option<Expr>,
    },
    // TODO: add all the "analyze" requests
    // TODO: should streaming query request be added here?
}

#[derive(Debug, Clone, PartialEq)]
pub enum ReadType {
    NamedTable {
        unparsed_identifier: String,
        options: HashMap<String, String>,
    },
    DataSource {
        format: Option<String>,
        schema: Option<Schema>,
        options: HashMap<String, String>,
        paths: Vec<String>,
        predicates: Vec<String>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum SaveType {
    Path(String),
    Table {
        table_name: String,
        save_method: TableSaveMethod,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum TableSaveMethod {
    SaveAsTable,
    InsertInto,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SaveBucketBy {
    pub bucket_column_names: Vec<String>,
    pub num_buckets: i32,
}

#[derive(Debug, Clone, PartialEq)]
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

#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    Inner,
    FullOuter,
    LeftOuter,
    RightOuter,
    LeftAnti,
    LeftSemi,
    Cross,
}

#[derive(Debug, Clone, PartialEq)]
pub struct JoinDataType {
    pub is_left_struct: bool,
    pub is_right_struct: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SetOpType {
    Intersect,
    Union,
    Except,
}

#[derive(Debug, Clone, PartialEq)]
pub enum GroupType {
    GroupBy,
    Rollup,
    Cube,
    Pivot,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ParseFormat {
    Unspecified,
    Csv,
    Json,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Fraction {
    pub stratum: Literal,
    pub fraction: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Pivot {
    pub column: Expr,
    pub values: Vec<Literal>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Replacement {
    pub old_value: Literal,
    pub new_value: Literal,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StorageLevel {
    pub use_disk: bool,
    pub use_memory: bool,
    pub use_off_heap: bool,
    pub deserialized: bool,
    pub replication: i32,
}
