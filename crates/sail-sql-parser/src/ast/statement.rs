use either::Either;
use sail_sql_macro::{TreeParser, TreeSyntax, TreeText};

use crate::ast;
use crate::ast::data_type::DataType;
use crate::ast::expression::{BooleanLiteral, Expr, OrderDirection};
use crate::ast::identifier::{table_ident, Ident, ObjectName};
use crate::ast::keywords::{
    Add, After, All, Alter, Always, Analyze, And, As, Buckets, By, Cache, Cascade, Catalog,
    Catalogs, Change, Clear, Cluster, Clustered, Codegen, Collection, Column, Columns, Comment,
    Compute, Cost, Create, Data, Database, Databases, Dbproperties, Default, Defined, Delete,
    Delimited, Desc, Describe, Directory, Distributed, Drop, Escaped, Evolution, Exists, Explain,
    Extended, External, Fields, Fileformat, First, For, Format, Formatted, From, Function,
    Functions, Generated, Global, If, In, Inpath, Inputformat, Insert, Into, Is, Items, Keys, Lazy,
    Like, Lines, Load, Local, Location, Map, Matched, Merge, Name, Noscan, Not, Null, On, Options,
    Or, Outputformat, Overwrite, Partition, Partitioned, Partitions, Properties, Purge, Recover,
    Refresh, Rename, Replace, Restrict, Row, Schema, Schemas, Serde, Serdeproperties, Set, Show,
    Sorted, Source, Statistics, Stored, Table, Tables, Target, Tblproperties, Temp, Temporary,
    Terminated, Then, Time, To, Type, Uncache, Unset, Update, Use, Using, Values, Verbose, View,
    Views, When, With, Zone,
};
use crate::ast::literal::{IntegerLiteral, NumberLiteral, StringLiteral};
use crate::ast::operator::{
    Asterisk, Colon, Comma, Equals, ExclamationMark, LeftParenthesis, Minus, Plus, RightParenthesis,
};
use crate::ast::query::{AliasClause, IdentList, Query, WhereClause};
use crate::combinator::{boxed, compose, sequence, unit};
use crate::common::Sequence;
use crate::token::TokenLabel;

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "(Statement, Query, Expr, DataType)", label = TokenLabel::Statement)]
#[allow(clippy::large_enum_variant)]
pub enum Statement {
    Query(#[parser(function = |(_, q, _, _), _| q)] Query),
    SetCatalog {
        set: Set,
        catalog: Catalog,
        name: Either<Ident, StringLiteral>,
    },
    UseDatabase {
        r#use: Use,
        database: Either<Database, Schema>,
        name: ObjectName,
    },
    UseCatalog {
        r#use: Use,
        catalog: Catalog,
        name: Ident,
    },
    CreateDatabase {
        create: Create,
        database: Either<Database, Schema>,
        name: ObjectName,
        if_not_exists: Option<(If, Not, Exists)>,
        clauses: Vec<CreateDatabaseClause>,
    },
    AlterDatabase {
        alter: Alter,
        database: Either<Database, Schema>,
        name: ObjectName,
        operation: AlterDatabaseOperation,
    },
    DropDatabase {
        drop: Drop,
        database: Either<Database, Schema>,
        if_exists: Option<(If, Exists)>,
        name: ObjectName,
        specifier: Option<Either<Restrict, Cascade>>,
    },
    ShowDatabases {
        show: Show,
        databases: Either<Databases, Schemas>,
        from: Option<(Either<From, In>, ObjectName)>,
        like: Option<(Option<Like>, StringLiteral)>,
    },
    ShowCatalogs {
        show: Show,
        catalogs: Catalogs,
        like: Option<(Option<Like>, StringLiteral)>,
    },
    CreateTable {
        create: Create,
        or_replace: Option<(Or, Replace)>,
        temporary: Option<Either<Temp, Temporary>>,
        external: Option<External>,
        table: Table,
        if_not_exists: Option<(If, Not, Exists)>,
        name: ObjectName,
        #[parser(function = |(_, _, e, d), o| compose((e, d), o))]
        columns: Option<ColumnDefinitionList>,
        like: Option<(Like, ObjectName)>,
        using: Option<(Using, Ident)>,
        #[parser(function = |(_, _, _, d), o| compose(d, o))]
        clauses: Vec<CreateTableClause>,
        #[parser(function = |(_, q, _, _), o| compose(q, o))]
        r#as: Option<AsQueryClause>,
    },
    ReplaceTable {
        replace: Replace,
        table: Table,
        name: ObjectName,
        #[parser(function = |(_, _, e, d), o| compose((e, d), o))]
        columns: Option<ColumnDefinitionList>,
        using: Option<(Using, Ident)>,
        #[parser(function = |(_, _, _, d), o| compose(d, o))]
        clauses: Vec<CreateTableClause>,
        #[parser(function = |(_, q, _, _), o| compose(q, o))]
        r#as: Option<AsQueryClause>,
    },
    RefreshTable {
        refresh: Refresh,
        table: Table,
        name: ObjectName,
    },
    AlterTable {
        alter: Alter,
        table: Table,
        name: ObjectName,
        #[parser(function = |(_, _, e, d), o| compose((e, d), o))]
        operation: AlterTableOperation,
    },
    DropTable {
        drop: Drop,
        table: Table,
        if_exists: Option<(If, Exists)>,
        name: ObjectName,
        purge: Option<Purge>,
    },
    ShowTables {
        show: Show,
        tables: Tables,
        from: Option<(Either<From, In>, ObjectName)>,
        like: Option<(Option<Like>, StringLiteral)>,
    },
    ShowCreateTable {
        show: Show,
        create: Create,
        table: Table,
        name: ObjectName,
        as_serde: Option<(As, Serde)>,
    },
    ShowColumns {
        show: Show,
        columns: Columns,
        table: (Either<From, In>, ObjectName),
        database: Option<(Either<From, In>, ObjectName)>,
    },
    CreateView {
        create: Create,
        or_replace: Option<(Or, Replace)>,
        global_temporary: Option<(Option<Global>, Either<Temp, Temporary>)>,
        view: View,
        if_not_exists: Option<(If, Not, Exists)>,
        name: ObjectName,
        columns: Option<(
            LeftParenthesis,
            Sequence<ViewColumn, Comma>,
            RightParenthesis,
        )>,
        clauses: Vec<CreateViewClause>,
        r#as: As,
        #[parser(function = |(_, q, _, _), _| q)]
        query: Query,
    },
    AlterView {
        alter: Alter,
        view: View,
        name: ObjectName,
        #[parser(function = |(_, q, e, _), o| compose((q, e), o))]
        operation: AlterViewOperation,
    },
    DropView {
        drop: Drop,
        view: View,
        if_exists: Option<(If, Exists)>,
        name: ObjectName,
    },
    ShowViews {
        show: Show,
        views: Views,
        from: Option<(Either<From, In>, ObjectName)>,
        like: Option<(Option<Like>, StringLiteral)>,
    },
    RefreshFunction {
        refresh: Refresh,
        function: Function,
        name: ObjectName,
    },
    DropFunction {
        drop: Drop,
        temporary: Option<Either<Temp, Temporary>>,
        function: Functions,
        if_exists: Option<(If, Exists)>,
        name: ObjectName,
    },
    ShowFunctions {
        show: Show,
        functions: Functions,
    },
    Explain {
        explain: Explain,
        format: Option<ExplainFormat>,
        #[parser(function = |(s, _, _, _), _| boxed(s))]
        statement: Box<Statement>,
    },
    InsertOverwriteDirectory {
        insert: Insert,
        overwrite: Overwrite,
        local: Option<Local>,
        directory: Directory,
        destination: InsertDirectoryDestination,
        #[parser(function = |(_, q, _, _), _| q)]
        query: Query,
    },
    InsertIntoAndReplace {
        insert: Insert,
        into: Into,
        table: Option<Table>,
        name: ObjectName,
        replace: Replace,
        #[parser(function = |(_, _, e, _), o| compose(e, o))]
        r#where: WhereClause,
        #[parser(function = |(_, q, _, _), _| q)]
        query: Query,
    },
    InsertInto {
        insert: Insert,
        into_or_overwrite: Either<Into, Overwrite>,
        table: Option<Table>,
        name: ObjectName,
        #[parser(function = |(_, _, e, _), o| compose(e, o))]
        partition: Option<PartitionClause>,
        if_not_exists: Option<(If, Not, Exists)>,
        columns: Option<Either<(By, Name), IdentList>>,
        #[parser(function = |(_, q, _, _), _| q)]
        query: Query,
    },
    MergeInto {
        merge: Merge,
        with_schema_evolution: Option<(With, Schema, Evolution)>,
        into: Into,
        target: ObjectName,
        alias: Option<AliasClause>,
        // FIXME: Rust 1.87 triggers `clippy::large_enum_variant` warning
        #[parser(function = |(_, q, _, _), o| unit(o).then(compose(q, o)))]
        using: (Using, MergeSource),
        #[parser(function = |(_, _, e, _), o| unit(o).then(e))]
        on: (On, Expr),
        #[parser(function = |(_, _, e, _), o| compose(e, o))]
        r#match: Vec<MergeMatchClause>,
    },
    Update {
        update: Update,
        name: ObjectName,
        alias: Option<UpdateTableAlias>,
        #[parser(function = |(_, _, e, _), o| compose(e, o))]
        set: SetClause,
        #[parser(function = |(_, _, e, _), o| compose(e, o))]
        r#where: Option<WhereClause>,
    },
    Delete {
        delete: Delete,
        from: From,
        name: ObjectName,
        alias: Option<DeleteTableAlias>,
        #[parser(function = |(_, _, e, _), o| compose(e, o))]
        r#where: Option<WhereClause>,
    },
    LoadData {
        load_data: (Load, Data),
        local: Option<Local>,
        path: (Inpath, StringLiteral),
        overwrite: Option<Overwrite>,
        into_table: (Into, Table),
        name: ObjectName,
        #[parser(function = |(_, _, e, _), o| compose(e, o))]
        partition: Option<PartitionClause>,
    },
    CacheTable {
        cache: Cache,
        lazy: Option<Lazy>,
        table: Table,
        name: ObjectName,
        options: Option<(Options, PropertyList)>,
        #[parser(function = |(_, q, _, _), o| compose(q, o))]
        r#as: Option<AsQueryClause>,
    },
    UncacheTable {
        uncache: Uncache,
        table: Table,
        if_exists: Option<(If, Exists)>,
        name: ObjectName,
    },
    ClearCache {
        clear: Clear,
        cache: Cache,
    },
    SetProperty {
        set: Set,
        property: Option<PropertyKeyValue>,
    },
    SetTimeZone {
        set: (Set, Time, Zone),
        timezone: Either<Local, StringLiteral>,
    },
    AnalyzeTable {
        analyze: (Analyze, Table),
        name: ObjectName,
        #[parser(function = |(_, _, e, _), o| compose(e, o))]
        partition: Option<PartitionClause>,
        compute: (Compute, Statistics),
        modifier: Option<AnalyzeTableModifier>,
    },
    AnalyzeTables {
        analyze: (Analyze, Tables),
        from: Option<(Either<From, In>, ObjectName)>,
        compute: (Compute, Statistics),
        no_scan: Option<Noscan>,
    },
    Describe {
        describe: Either<Desc, Describe>,
        #[parser(function = |(_, q, e, _), o| compose((q, e), o))]
        item: DescribeItem,
    },
    CommentOnCatalog {
        comment: (Comment, On, Catalog),
        name: ObjectName,
        is: Is,
        value: CommentValue,
    },
    CommentOnDatabase {
        comment: (Comment, On, Either<Database, Schema>),
        name: ObjectName,
        is: Is,
        value: CommentValue,
    },
    CommentOnTable {
        comment: (Comment, On, Table),
        name: ObjectName,
        is: Is,
        value: CommentValue,
    },
    CommentOnColumn {
        comment: (Comment, On, Column),
        name: ObjectName,
        is: Is,
        value: CommentValue,
    },
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
pub enum ExplainFormat {
    Extended(Extended),
    Codegen(Codegen),
    Cost(Cost),
    Formatted(Formatted),
    Analyze(Analyze),
    Verbose(Verbose),
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
pub struct PropertyList {
    pub left: LeftParenthesis,
    pub properties: Sequence<PropertyKeyValue, Comma>,
    pub right: RightParenthesis,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
pub struct PropertyKeyList {
    pub left: LeftParenthesis,
    pub properties: Sequence<PropertyKey, Comma>,
    pub right: RightParenthesis,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
pub struct PropertyKeyValue {
    pub key: PropertyKey,
    pub value: Option<(Option<Equals>, PropertyValue)>,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
pub enum PropertyKey {
    Name(ObjectName),
    Literal(StringLiteral),
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
pub enum PropertyValue {
    String(StringLiteral),
    Number(Option<Either<Plus, Minus>>, NumberLiteral),
    Boolean(BooleanLiteral),
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
pub enum AlterDatabaseOperation {
    SetProperties(Set, Either<Dbproperties, Properties>, PropertyList),
    SetLocation(Set, Location, StringLiteral),
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Query")]
pub struct AsQueryClause {
    pub r#as: Option<As>,
    #[parser(function = |q, _| q)]
    pub query: Query,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "(Expr, DataType)")]
pub struct ColumnDefinitionList {
    pub left: LeftParenthesis,
    #[parser(function = |(e, d), o| sequence(compose((e, d), o), unit(o)))]
    pub columns: Sequence<ColumnDefinition, Comma>,
    pub right: RightParenthesis,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "(Expr, DataType)")]
pub struct ColumnDefinition {
    pub name: Ident,
    #[parser(function = |(_, d), _| d)]
    pub data_type: DataType,
    #[parser(function = |(e, _), o| compose(e, o))]
    pub options: Vec<ColumnDefinitionOption>,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub enum ColumnDefinitionOption {
    NotNull(Not, Null),
    Default(Default, #[parser(function = |e, _| e)] Expr),
    Generated(
        Generated,
        Always,
        As,
        LeftParenthesis,
        #[parser(function = |e, _| e)] Expr,
        RightParenthesis,
    ),
    Comment(Comment, StringLiteral),
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "DataType")]
pub struct ColumnTypeDefinition {
    pub name: Ident,
    pub colon: Option<Colon>,
    #[parser(function = |d, _| d)]
    pub data_type: DataType,
    pub not_null: Option<(Not, Null)>,
    pub comment: Option<(Comment, StringLiteral)>,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "DataType")]
#[allow(clippy::large_enum_variant)]
pub enum PartitionColumn {
    // FIXME: Rust 1.87 triggers `clippy::large_enum_variant` warning
    Typed(#[parser(function = |d, o| compose(d, o))] ColumnTypeDefinition),
    Name(Ident),
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "DataType")]
pub struct PartitionColumnList {
    pub left: LeftParenthesis,
    #[parser(function = |d, o| sequence(compose(d, o), unit(o)))]
    pub columns: Sequence<PartitionColumn, Comma>,
    pub right: RightParenthesis,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub struct PartitionClause {
    pub partition: Partition,
    #[parser(function = |e, o| compose(e, o))]
    pub values: PartitionValueList,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub struct PartitionValue {
    pub column: Ident,
    #[parser(function = |e, o| unit(o).then(e).or_not())]
    pub value: Option<(Equals, Expr)>,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub struct PartitionValueList {
    pub left: LeftParenthesis,
    #[parser(function = |e, o| sequence(compose(e, o), unit(o)))]
    pub values: Sequence<PartitionValue, Comma>,
    pub right: RightParenthesis,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
pub enum CreateDatabaseClause {
    Comment(Comment, StringLiteral),
    Location(Location, StringLiteral),
    Properties(With, Either<Dbproperties, Properties>, PropertyList),
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "DataType")]
pub enum CreateTableClause {
    /// The `PARTITIONED BY` clause for table.
    PartitionedBy(
        Partitioned,
        By,
        #[parser(function = |d, o| compose(d, o))] PartitionColumnList,
    ),
    /// The `CLUSTERED BY ... SORTED BY ... INTO ... BUCKETS` clause for table.
    /// In Flink, `DISTRIBUTED BY ... INTO ... BUCKETS` seems to have a similar semantic.
    ClusteredBy(
        Either<Clustered, Distributed>,
        By,
        IdentList,
        Option<(Sorted, By, SortColumnList)>,
        Into,
        IntegerLiteral,
        Buckets,
    ),
    /// The `CLUSTER BY ...` clause introduced in Spark 4.0.
    ClusterBy(
        Cluster,
        By,
        LeftParenthesis,
        Sequence<ObjectName, Comma>,
        RightParenthesis,
    ),
    RowFormat(Row, Format, RowFormat),
    StoredAs(Stored, As, FileFormat),
    Location(Location, StringLiteral),
    Comment(Comment, StringLiteral),
    Options(Options, PropertyList),
    Properties(Tblproperties, PropertyList),
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
pub struct SortColumnList {
    pub left: LeftParenthesis,
    pub columns: Sequence<SortColumn, Comma>,
    pub right: RightParenthesis,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
pub struct SortColumn {
    pub column: Ident,
    pub direction: Option<OrderDirection>,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
pub enum RowFormat {
    Serde {
        serde: Serde,
        name: StringLiteral,
        properties: Option<(With, Serdeproperties, PropertyList)>,
    },
    Delimited {
        delimited: Delimited,
        clauses: Vec<RowFormatDelimitedClause>,
    },
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
pub enum RowFormatDelimitedClause {
    Fields(
        Fields,
        Terminated,
        By,
        StringLiteral,
        Option<(Escaped, By, StringLiteral)>,
    ),
    CollectionItems(Collection, Items, Terminated, By, StringLiteral),
    MapKeys(Map, Keys, Terminated, By, StringLiteral),
    Lines(Lines, Terminated, By, StringLiteral),
    Null(Null, Defined, As, StringLiteral),
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
pub enum FileFormat {
    Table(Inputformat, StringLiteral, Outputformat, StringLiteral),
    General(Ident),
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
pub enum CreateViewClause {
    Comment(Comment, StringLiteral),
    Properties(Tblproperties, PropertyList),
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
pub struct ViewColumn {
    pub name: Ident,
    pub comment: Option<(Comment, StringLiteral)>,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "(Expr, DataType)")]
pub enum AlterTableOperation {
    RenameTable {
        rename: Rename,
        to: To,
        name: ObjectName,
    },
    RenamePartition {
        #[parser(function = |(e, _), o| compose(e, o))]
        old: PartitionClause,
        rename: Rename,
        to: To,
        #[parser(function = |(e, _), o| compose(e, o))]
        new: PartitionClause,
    },
    AddColumns {
        add: Add,
        columns: Either<Column, Columns>,
        #[parser(function = |(e, d), o| compose((e, d), o))]
        items: ColumnAlterationList,
    },
    DropColumns {
        drop: Drop,
        columns: Either<Column, Columns>,
        if_exists: Option<(If, Exists)>,
        names: ColumnDropList,
    },
    RenameColumn {
        rename: (Rename, Column),
        old: ObjectName,
        to: To,
        new: ObjectName,
    },
    AlterColumn {
        alter: Either<Alter, Change>,
        column: Column,
        name: ObjectName,
        #[parser(function = |(e, d), o| compose((e, d), o))]
        operation: AlterColumnOperation,
    },
    ReplaceColumns {
        replace: Replace,
        columns: Either<Column, Columns>,
        #[parser(function = |(e, d), o| compose((e, d), o))]
        items: ColumnAlterationList,
    },
    AddPartitions {
        add: Add,
        if_not_exists: Option<(If, Not, Exists)>,
        #[parser(function = |(e, _), o| compose(e, o))]
        partitions: Vec<PartitionClause>,
    },
    DropPartition {
        drop: Drop,
        if_exists: Option<(If, Exists)>,
        #[parser(function = |(e, _), o| compose(e, o))]
        partition: PartitionClause,
        purge: Option<Purge>,
    },
    SetTableProperties {
        set: Set,
        table_properties: Tblproperties,
        properties: PropertyList,
    },
    UnsetTableProperties {
        unset: Unset,
        table_properties: Tblproperties,
        if_exists: Option<(If, Exists)>,
        properties: PropertyKeyList,
    },
    SetFileFormat {
        #[parser(function = |(e, _), o| compose(e, o))]
        partition: Option<PartitionClause>,
        set: Set,
        file_format: Fileformat,
        format: FileFormat,
    },
    SetLocation {
        #[parser(function = |(e, _), o| compose(e, o))]
        partition: Option<PartitionClause>,
        set: Set,
        location: Location,
        value: StringLiteral,
    },
    RecoverPartitions {
        recover: Recover,
        partitions: Partitions,
    },
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "(Query, Expr)")]
pub enum AlterViewOperation {
    RenameView {
        rename: Rename,
        to: To,
        name: ObjectName,
    },
    SetViewProperties {
        set: Set,
        table_properties: Tblproperties,
        properties: PropertyList,
    },
    UnsetViewProperties {
        unset: Unset,
        table_properties: Tblproperties,
        if_exists: Option<(If, Exists)>,
        properties: PropertyKeyList,
    },
    Query(#[parser(function = |(q, _), o| compose(q, o))] AsQueryClause),
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "(Expr, DataType)")]
pub enum AlterColumnOperation {
    Type(Type, #[parser(function = |(_, d), _| d)] DataType),
    Comment(Comment, StringLiteral),
    SetNotNull(Set, Not, Null),
    DropNotNull(Drop, Not, Null),
    Position(ColumnPosition),
    SetDefault(Set, Default, #[parser(function = |(e, _), _| e)] Expr),
    DropDefault(Drop, Default),
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "(Expr, DataType)")]
pub enum ColumnAlterationList {
    Delimited {
        left: LeftParenthesis,
        #[parser(function = |(e, d), o| sequence(compose((e, d), o), unit(o)))]
        columns: Sequence<ColumnAlteration, Comma>,
        right: RightParenthesis,
    },
    NotDelimited {
        #[parser(function = |(e, d), o| sequence(compose((e, d), o), unit(o)))]
        columns: Sequence<ColumnAlteration, Comma>,
    },
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "(Expr, DataType)")]
pub struct ColumnAlteration {
    pub name: ObjectName,
    #[parser(function = |(_, d), _| d)]
    pub data_type: DataType,
    #[parser(function = |(e, _), o| compose(e, o))]
    pub options: Vec<ColumnAlterationOption>,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub enum ColumnAlterationOption {
    NotNull(Not, Null),
    Default(Default, #[parser(function = |e, _| e)] Expr),
    Comment(Comment, StringLiteral),
    Position(ColumnPosition),
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
pub enum ColumnPosition {
    First(First),
    After(After, ObjectName),
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
pub enum ColumnDropList {
    Delimited {
        left: LeftParenthesis,
        columns: Sequence<ObjectName, Comma>,
        right: RightParenthesis,
    },
    NotDelimited {
        columns: Sequence<ObjectName, Comma>,
    },
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
pub enum InsertDirectoryDestination {
    Spark {
        path: Option<StringLiteral>,
        using: (Using, Ident),
        options: Option<(Options, PropertyList)>,
    },
    Hive {
        path: StringLiteral,
        row_format: Option<(Row, Format, RowFormat)>,
        stored_as: Option<(Stored, As, FileFormat)>,
    },
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Query")]
pub enum MergeSource {
    Table {
        name: ObjectName,
        alias: Option<AliasClause>,
    },
    Query {
        left: LeftParenthesis,
        #[parser(function = |q, _| q)]
        query: Query,
        right: RightParenthesis,
        alias: Option<AliasClause>,
    },
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub enum MergeMatchClause {
    Matched {
        when: When,
        matched: Matched,
        #[parser(function = |e, o| unit(o).then(e).or_not())]
        condition: Option<(And, Expr)>,
        then: Then,
        #[parser(function = |e, o| compose(e, o))]
        action: MergeMatchedAction,
    },
    NotMatchedBySource {
        when: When,
        not: Either<Not, ExclamationMark>,
        matched: Matched,
        by_source: (By, Source),
        #[parser(function = |e, o| unit(o).then(e).or_not())]
        condition: Option<(And, Expr)>,
        then: Then,
        #[parser(function = |e, o| compose(e, o))]
        action: MergeNotMatchedBySourceAction,
    },
    NotMatchedByTarget {
        when: When,
        not: Either<Not, ExclamationMark>,
        matched: Matched,
        by_target: Option<(By, Target)>,
        #[parser(function = |e, o| unit(o).then(e).or_not())]
        condition: Option<(And, Expr)>,
        then: Then,
        #[parser(function = |e, o| compose(e, o))]
        action: MergeNotMatchedByTargetAction,
    },
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub enum MergeMatchedAction {
    Delete(Delete),
    UpdateAll(Update, Set, Asterisk),
    Update(
        Update,
        Set,
        #[parser(function = |e, o| compose(e, o))] AssignmentList,
    ),
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub enum MergeNotMatchedBySourceAction {
    Delete(Delete),
    Update(
        Update,
        Set,
        #[parser(function = |e, o| compose(e, o))] AssignmentList,
    ),
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub enum MergeNotMatchedByTargetAction {
    InsertAll(Insert, Asterisk),
    Insert {
        insert: Insert,
        left: LeftParenthesis,
        columns: Sequence<ObjectName, Comma>,
        right: RightParenthesis,
        values: Values,
        #[parser(function = |e, o| sequence(e, unit(o)))]
        expressions: Sequence<Expr, Comma>,
    },
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
pub struct UpdateTableAlias {
    pub r#as: Option<As>,
    #[parser(function = |(), o| table_ident(o))]
    pub table: Ident,
    pub columns: Option<IdentList>,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub struct SetClause {
    pub set: Set,
    #[parser(function = |e, o| compose(e, o))]
    pub assignments: AssignmentList,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub enum AssignmentList {
    Delimited {
        left: LeftParenthesis,
        #[parser(function = |a, o| sequence(compose(a, o), unit(o)))]
        assignments: Sequence<Assignment, Comma>,
        right: RightParenthesis,
    },
    NotDelimited {
        #[parser(function = |a, o| sequence(compose(a, o), unit(o)))]
        assignments: Sequence<Assignment, Comma>,
    },
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub struct Assignment {
    pub target: ObjectName,
    pub equals: Equals,
    #[parser(function = |e, _| e)]
    pub value: Expr,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
pub struct DeleteTableAlias {
    pub r#as: Option<As>,
    #[parser(function = |(), o| table_ident(o))]
    pub table: Ident,
    pub columns: Option<IdentList>,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
pub enum AnalyzeTableModifier {
    NoScan(Noscan),
    ForAllColumns(For, All, Columns),
    ForColumns(For, Columns, Sequence<ObjectName, Comma>),
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "(Query, Expr)")]
pub enum DescribeItem {
    // We need to try `DESCRIBE QUERY` first since the `QUERY` keyword
    // is optional. We will fall back to other choices if there is
    // no valid query following the `DESCRIBE` keyword.
    Query {
        query: Option<ast::keywords::Query>,
        #[parser(function = |(q, _), _| q)]
        item: Query,
    },
    Function {
        function: Function,
        extended: Option<Extended>,
        item: Either<ObjectName, StringLiteral>,
    },
    Catalog {
        catalog: Catalog,
        extended: Option<Extended>,
        item: ObjectName,
    },
    Database {
        database: Either<Database, Schema>,
        extended: Option<Extended>,
        item: ObjectName,
    },
    Table {
        table: Option<Table>,
        extended: Option<Extended>,
        name: ObjectName,
        #[parser(function = |(_, e), o| compose(e, o))]
        partition: Option<PartitionClause>,
        column: Option<ObjectName>,
    },
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
pub enum CommentValue {
    NotNull(StringLiteral),
    Null(Null),
}
