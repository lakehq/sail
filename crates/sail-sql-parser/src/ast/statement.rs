use chumsky::prelude::choice;
use either::Either;
use sail_sql_macro::TreeParser;

use crate::ast::data_type::DataType;
use crate::ast::expression::{BooleanLiteral, Expr, OrderDirection};
use crate::ast::identifier::{Ident, ObjectName};
use crate::ast::keywords::{
    Add, After, Alter, Always, Analyze, As, Buckets, By, Cache, Cascade, Catalog, Change, Clear,
    Clustered, Codegen, Collection, Column, Columns, Comment, Cost, Create, Database, Databases,
    Dbproperties, Default, Defined, Delete, Delimited, Drop, Escaped, Exists, Explain, Extended,
    External, Fields, Fileformat, First, Format, Formatted, From, Functions, Generated, Global, If,
    In, Inputformat, Insert, Into, Items, Keys, Lazy, Like, Lines, Local, Location, Map, Not, Null,
    Options, Or, Outputformat, Overwrite, Partition, Partitioned, Partitions, Properties, Purge,
    Recover, Rename, Replace, Restrict, Row, Schema, Schemas, Serde, Serdeproperties, Set, Show,
    Sorted, Stored, Table, Tables, Tblproperties, Temp, Temporary, Terminated, Time, To, Type,
    Uncache, Unset, Update, Use, Using, Verbose, View, Views, Where, With, Zone,
};
use crate::ast::literal::{IntegerLiteral, NumberLiteral, StringLiteral};
use crate::ast::operator::{Colon, Comma, Equals, LeftParenthesis, Minus, Plus, RightParenthesis};
use crate::ast::query::{IdentList, Query, WhereClause};
use crate::combinator::{compose, sequence, unit};
use crate::common::Sequence;
use crate::token::TokenLabel;

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "(Statement, Query, Expr, DataType)", label = TokenLabel::Statement)]
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
    CreateTable {
        create: Create,
        or_replace: Option<(Or, Replace)>,
        temporary: Option<Either<Temp, Temporary>>,
        external: Option<External>,
        table: Table,
        if_not_exists: Option<(If, Not, Exists)>,
        name: ObjectName,
        #[parser(function = |(_, _, e, t), o| compose((e, t), o))]
        columns: Option<ColumnDefinitionList>,
        like: Option<(Like, ObjectName)>,
        using: Option<(Using, Ident)>,
        #[parser(function = |(_, _, _, t), o| compose(t, o))]
        clauses: Vec<CreateTableClause>,
        #[parser(function = |(_, q, _, _), o| compose(q, o))]
        r#as: Option<AsQueryClause>,
    },
    ReplaceTable {
        replace: Replace,
        table: Table,
        name: ObjectName,
        #[parser(function = |(_, _, e, t), o| compose((e, t), o))]
        columns: Option<ColumnDefinitionList>,
        using: Option<(Using, Ident)>,
        #[parser(function = |(_, _, _, t), o| compose(t, o))]
        clauses: Vec<CreateTableClause>,
        #[parser(function = |(_, q, _, _), o| compose(q, o))]
        r#as: Option<AsQueryClause>,
    },
    AlterTable {
        alter: Alter,
        table: Table,
        name: ObjectName,
        #[parser(function = |(_, _, e, t), o| compose((e, t), o))]
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
        #[parser(function = |(_, q, _, _), _| q)]
        query: Query,
    },
    Insert {
        insert: Insert,
        into_or_overwrite: Option<Either<Into, Overwrite>>,
        table: Option<Table>,
        name: ObjectName,
        #[parser(function = |(_, _, e, _), o| compose(e, o))]
        partition: Option<PartitionSpec>,
        columns: Option<IdentList>,
        #[parser(function = |(_, q, _, _), _| q)]
        query: Query,
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
}

#[derive(Debug, Clone, TreeParser)]
pub enum ExplainFormat {
    Extended(Extended),
    Codegen(Codegen),
    Cost(Cost),
    Formatted(Formatted),
    Analyze(Analyze),
    Verbose(Verbose),
}

#[derive(Debug, Clone, TreeParser)]
pub struct PropertyList {
    pub left: LeftParenthesis,
    pub properties: Sequence<PropertyKeyValue, Comma>,
    pub right: RightParenthesis,
}

#[derive(Debug, Clone, TreeParser)]
pub struct PropertyKeyList {
    pub left: LeftParenthesis,
    pub properties: Sequence<PropertyKey, Comma>,
    pub right: RightParenthesis,
}

#[derive(Debug, Clone, TreeParser)]
pub struct PropertyKeyValue {
    pub key: PropertyKey,
    pub value: Option<(Option<Equals>, PropertyValue)>,
}

#[derive(Debug, Clone, TreeParser)]
pub enum PropertyKey {
    Name(ObjectName),
    Literal(StringLiteral),
}

#[derive(Debug, Clone, TreeParser)]
pub enum PropertyValue {
    String(StringLiteral),
    Number(Option<Either<Plus, Minus>>, NumberLiteral),
    Boolean(BooleanLiteral),
}

#[derive(Debug, Clone, TreeParser)]
pub enum AlterDatabaseOperation {
    SetProperties(Set, Either<Dbproperties, Properties>, PropertyList),
    SetLocation(Set, Location, StringLiteral),
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Query")]
pub struct AsQueryClause {
    pub r#as: Option<As>,
    #[parser(function = |q, _| q)]
    pub query: Query,
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "(Expr, DataType)")]
pub struct ColumnDefinitionList {
    pub left: LeftParenthesis,
    #[parser(function = |(e, t), o| sequence(compose((e, t), o), unit(o)))]
    pub columns: Sequence<ColumnDefinition, Comma>,
    pub right: RightParenthesis,
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "(Expr, DataType)")]
pub struct ColumnDefinition {
    pub name: Ident,
    #[parser(function = |(_, t), _| t)]
    pub data_type: DataType,
    #[parser(function = |(e, _), o| compose(e, o))]
    pub options: Vec<ColumnDefinitionOption>,
}

#[derive(Debug, Clone, TreeParser)]
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

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "DataType")]
pub struct ColumnTypeDefinition {
    pub name: Ident,
    pub colon: Option<Colon>,
    #[parser(function = |x, _| x)]
    pub data_type: DataType,
    pub not_null: Option<(Not, Null)>,
    pub comment: Option<(Comment, StringLiteral)>,
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "DataType")]
pub enum PartitionColumn {
    Typed(#[parser(function = |t, o| compose(t, o))] ColumnTypeDefinition),
    Name(Ident),
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "DataType")]
pub struct PartitionColumnList {
    pub left: LeftParenthesis,
    #[parser(function = |t, o| sequence(compose(t, o), unit(o)))]
    pub columns: Sequence<PartitionColumn, Comma>,
    pub right: RightParenthesis,
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct PartitionSpec {
    pub partition: Partition,
    #[parser(function = |e, o| compose(e, o))]
    pub values: PartitionValueList,
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct PartitionValue {
    pub column: Ident,
    pub eq: Equals,
    #[parser(function = |e, _| e)]
    pub value: Expr,
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct PartitionValueList {
    pub left: LeftParenthesis,
    #[parser(function = |e, o| sequence(compose(e, o), unit(o)))]
    pub values: Sequence<PartitionValue, Comma>,
    pub right: RightParenthesis,
}

#[derive(Debug, Clone, TreeParser)]
pub enum CreateDatabaseClause {
    Comment(Comment, StringLiteral),
    Location(Location, StringLiteral),
    Properties(With, Either<Dbproperties, Properties>, PropertyList),
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "DataType")]
pub enum CreateTableClause {
    PartitionedBy(
        Partitioned,
        By,
        #[parser(function = |t, o| compose(t, o))] PartitionColumnList,
    ),
    ClusteredBy(
        Clustered,
        By,
        IdentList,
        Option<(Sorted, By, SortColumnList)>,
        Into,
        IntegerLiteral,
        Buckets,
    ),
    RowFormat(Row, Format, RowFormat),
    StoredAs(Stored, As, FileFormat),
    Location(Location, StringLiteral),
    Comment(Comment, StringLiteral),
    Options(Options, PropertyList),
    Properties(Tblproperties, PropertyList),
}

#[derive(Debug, Clone, TreeParser)]
pub struct SortColumnList {
    pub left: LeftParenthesis,
    pub columns: Sequence<SortColumn, Comma>,
    pub right: RightParenthesis,
}

#[derive(Debug, Clone, TreeParser)]
pub struct SortColumn {
    pub column: Ident,
    pub direction: Option<OrderDirection>,
}

#[derive(Debug, Clone, TreeParser)]
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

#[derive(Debug, Clone, TreeParser)]
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

#[derive(Debug, Clone, TreeParser)]
pub enum FileFormat {
    Table(Inputformat, StringLiteral, Outputformat, StringLiteral),
    General(Ident),
}

#[derive(Debug, Clone, TreeParser)]
pub enum CreateViewClause {
    Comment(Comment, StringLiteral),
    Properties(Tblproperties, PropertyList),
}

#[derive(Debug, Clone, TreeParser)]
pub struct ViewColumn {
    pub name: Ident,
    pub comment: Option<(Comment, StringLiteral)>,
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "(Expr, DataType)")]
pub enum AlterTableOperation {
    RenameTable {
        rename: Rename,
        to: To,
        name: ObjectName,
    },
    RenamePartition {
        #[parser(function = |(e, _), o| compose(e, o))]
        old: PartitionSpec,
        rename: Rename,
        to: To,
        #[parser(function = |(e, _), o| compose(e, o))]
        new: PartitionSpec,
    },
    AddColumns {
        add: Add,
        columns: Either<Column, Columns>,
        #[parser(function = |(e, t), o| compose((e, t), o))]
        items: ColumnAlterationList,
    },
    DropColumns {
        drop: Drop,
        columns: Either<Column, Columns>,
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
        #[parser(function = |(e, t), o| compose((e, t), o))]
        operation: AlterColumnOperation,
    },
    ReplaceColumns {
        replace: Replace,
        columns: Either<Column, Columns>,
        #[parser(function = |(e, t), o| compose((e, t), o))]
        items: ColumnAlterationList,
    },
    AddPartitions {
        add: Add,
        if_not_exists: Option<(If, Not, Exists)>,
        #[parser(function = |(e, _), o| compose(e, o))]
        partitions: Vec<PartitionSpec>,
    },
    DropPartition {
        drop: Drop,
        if_exists: Option<(If, Exists)>,
        #[parser(function = |(e, _), o| compose(e, o))]
        partition: PartitionSpec,
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
        partition: Option<PartitionSpec>,
        set: Set,
        file_format: Fileformat,
        format: FileFormat,
    },
    SetLocation {
        #[parser(function = |(e, _), o| compose(e, o))]
        partition: Option<PartitionSpec>,
        set: Set,
        location: Location,
        value: StringLiteral,
    },
    RecoverPartitions {
        recover: Recover,
        partitions: Partitions,
    },
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "(Expr, DataType)")]
pub enum AlterColumnOperation {
    Type(Type, #[parser(function = |(_, t), _| t)] DataType),
    Comment(Comment, StringLiteral),
    SetNotNull(Set, Not, Null),
    DropNotNull(Drop, Not, Null),
    Position(ColumnPosition),
    SetDefault(Set, Default, #[parser(function = |(e, _), _| e)] Expr),
    DropDefault(Drop, Default),
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "(Expr, DataType)")]
pub enum ColumnAlterationList {
    Delimited {
        left: LeftParenthesis,
        #[parser(function = |(e, t), o| sequence(compose((e, t), o), unit(o)))]
        columns: Sequence<ColumnAlteration, Comma>,
        right: RightParenthesis,
    },
    NotDelimited {
        #[parser(function = |(e, t), o| sequence(compose((e, t), o), unit(o)))]
        columns: Sequence<ColumnAlteration, Comma>,
    },
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "(Expr, DataType)")]
pub struct ColumnAlteration {
    pub name: ObjectName,
    #[parser(function = |(_, t), _| t)]
    pub data_type: DataType,
    #[parser(function = |(e, _), o| compose(e, o))]
    pub options: Vec<ColumnAlterationOption>,
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub enum ColumnAlterationOption {
    NotNull(Not, Null),
    Default(Default, #[parser(function = |e, _| e)] Expr),
    Comment(Comment, StringLiteral),
    Position(ColumnPosition),
}

#[derive(Debug, Clone, TreeParser)]
pub enum ColumnPosition {
    First(First),
    After(After, ObjectName),
}

#[derive(Debug, Clone, TreeParser)]
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

#[allow(unused)]
#[derive(Debug, Clone, TreeParser)]
pub struct UpdateTableAlias {
    r#as: Option<As>,
    #[parser(function = |(), o| unit(o).and_is(choice((Where::parser((), o).ignored(), Set::parser((), o).ignored())).not()))]
    table: Ident,
    columns: Option<IdentList>,
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct SetClause {
    pub set: Set,
    #[parser(function = |e, o| compose(e, o))]
    pub assignments: AssignmentList,
}

#[derive(Debug, Clone, TreeParser)]
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

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct Assignment {
    pub target: ObjectName,
    pub equals: Equals,
    #[parser(function = |e, _| e)]
    pub value: Expr,
}

#[allow(unused)]
#[derive(Debug, Clone, TreeParser)]
pub struct DeleteTableAlias {
    r#as: Option<As>,
    #[parser(function = |(), o| unit(o).and_is(Where::parser((), o).not()))]
    table: Ident,
    columns: Option<IdentList>,
}
