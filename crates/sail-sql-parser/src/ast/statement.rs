use either::Either;
use sail_sql_macro::TreeParser;

use crate::ast::data_type::DataType;
use crate::ast::expression::Expr;
use crate::ast::identifier::{Ident, ObjectName};
use crate::ast::keywords::{
    Add, Alter, Always, Analyze, As, Cache, Cascade, Catalog, Clear, Codegen, Column, Columns,
    Comment, Cost, Create, Database, Databases, Dbproperties, Default, Drop, Exists, Explain,
    Extended, External, False, Formatted, From, Generated, If, In, Lazy, Like, Local, Location,
    Not, Null, Options, Or, Properties, Purge, Rename, Replace, Restrict, Schema, Schemas, Set,
    Show, Table, Tblproperties, Temporary, Time, To, True, Uncache, Use, Using, Verbose, With,
    Zone,
};
use crate::ast::literal::StringLiteral;
use crate::ast::operator::{Comma, Equals, LeftParenthesis, RightParenthesis};
use crate::ast::query::Query;
use crate::combinator::{compose, sequence, unit};
use crate::common::Sequence;

#[allow(unused)]
#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "(Statement, Query, Expr, DataType)")]
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
        action: AlterDatabaseAction,
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
        database: Either<Databases, Schemas>,
        from: Option<(Either<From, In>, ObjectName)>,
        like: Option<(Option<Like>, StringLiteral)>,
    },
    CreateTable {
        create: Create,
        temporary: Option<Temporary>,
        external: Option<External>,
        table: Table,
        if_not_exists: Option<(If, Not, Exists)>,
        name: ObjectName,
        #[parser(function = |(_, _, e, t), o| compose((e, t), o))]
        columns: Option<TableColumns>,
        provider: Option<TableProvider>,
        clauses: Vec<CreateOrReplaceTableClause>,
        #[parser(function = |(_, q, _, _), o| unit(o).or_not().then(q).or_not())]
        r#as: Option<(Option<As>, Query)>,
    },
    ReplaceTable {
        create_or: Option<(Create, Or)>,
        replace: Replace,
        table: Table,
        name: ObjectName,
        #[parser(function = |(_, _, e, t), o| compose((e, t), o))]
        columns: Option<TableColumns>,
        #[parser(function = |(_, q, _, _), o| unit(o).or_not().then(q).or_not())]
        r#as: Option<(Option<As>, Query)>,
    },
    AlterTable {
        alter: Alter,
        table: Table,
        name: ObjectName,
        action: AlterTableAction,
    },
    DropTable {
        drop: Drop,
        table: Table,
        if_exists: Option<(If, Exists)>,
        name: ObjectName,
        purge: Option<Purge>,
    },
    Explain {
        explain: Explain,
        format: Option<ExplainFormat>,
        #[parser(function = |(_, q, _, _), _| q)]
        query: Query,
    },
    CacheTable {
        cache: Cache,
        lazy: Option<Lazy>,
        table: Table,
        name: ObjectName,
        options: Option<(Options, PropertyList)>,
        #[parser(function = |(_, q, _, _), o| unit(o).or_not().then(q).or_not())]
        r#as: Option<(Option<As>, Query)>,
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
    SetTimeZone {
        set: (Set, Time, Zone),
        timezone: Either<Local, StringLiteral>,
    },
}

#[allow(unused)]
#[derive(Debug, Clone, TreeParser)]
pub enum ExplainFormat {
    Extended(Extended),
    Codegen(Codegen),
    Cost(Cost),
    Formatted(Formatted),
    Analyze(Analyze),
    Verbose(Verbose),
}

#[allow(unused)]
#[derive(Debug, Clone, TreeParser)]
pub enum CreateDatabaseClause {
    Comment(Comment, StringLiteral),
    Location(Location, StringLiteral),
    Properties(With, Either<Dbproperties, Properties>, PropertyList),
}

#[allow(unused)]
#[derive(Debug, Clone, TreeParser)]
pub struct PropertyList {
    pub left: LeftParenthesis,
    pub properties: Sequence<PropertyKeyValue, Comma>,
    pub right: LeftParenthesis,
}

#[allow(unused)]
#[derive(Debug, Clone, TreeParser)]
pub struct PropertyKeyValue {
    pub key: Either<ObjectName, StringLiteral>,
    pub value: Option<(Option<Equals>, PropertyValue)>,
}

#[allow(unused)]
#[derive(Debug, Clone, TreeParser)]
pub enum PropertyValue {
    String(StringLiteral),
    Number(StringLiteral),
    Boolean(Either<True, False>),
}

#[allow(unused)]
#[derive(Debug, Clone, TreeParser)]
pub enum AlterDatabaseAction {
    SetProperties(Set, Either<Dbproperties, Properties>, PropertyList),
    SetLocation(Set, Location, StringLiteral),
}

#[allow(unused)]
#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "(Expr, DataType)")]
pub struct TableColumns {
    left: LeftParenthesis,
    #[parser(function = |(e, t), o| sequence(compose((e, t), o), unit(o)))]
    columns: Sequence<TableColumn, Comma>,
    right: LeftParenthesis,
}

#[allow(unused)]
#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "(Expr, DataType)")]
pub struct TableColumn {
    pub name: Ident,
    #[parser(function = |(_, t), _| t)]
    pub data_type: DataType,
    #[parser(function = |(e, _), o| compose(e, o))]
    pub options: Vec<TableColumnOption>,
}

#[allow(unused)]
#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub enum TableColumnOption {
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

#[allow(unused)]
#[derive(Debug, Clone, TreeParser)]
pub enum CreateOrReplaceTableClause {
    Options(Options),
    Location(Location, StringLiteral),
    Comment(Comment, StringLiteral),
    Properties(Tblproperties, PropertyList),
}

#[allow(unused)]
#[derive(Debug, Clone, TreeParser)]
pub struct TableProvider {
    pub using: Using,
    pub name: ObjectName,
}

#[allow(unused)]
#[derive(Debug, Clone, TreeParser)]
pub enum AlterTableAction {
    AddColumns {
        add: (Add, Either<Column, Columns>),
    },
    RenameColumn {
        rename: (Rename, Column),
        old: ObjectName,
        to: To,
        new: ObjectName,
    },
}
