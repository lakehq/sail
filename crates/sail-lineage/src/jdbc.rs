//! Input datasets for JDBC reads, named according to the OpenLineage dataset naming spec.
//!
//! See also: <https://openlineage.io/docs/spec/naming>

use std::collections::HashSet;

use log::{debug, warn};
use sail_common_datafusion::datasource::OptionLayer;
use sail_sql_analyzer::parser::parse_statements_with_options;
use sail_sql_parser::ast::expression::{AtomExpr, Expr, FunctionArgument};
use sail_sql_parser::ast::identifier::ObjectName;
use sail_sql_parser::ast::query::{
    JoinCriteria, Query, QueryBody, QuerySelect, QueryTerm, TableFactor, TableWithJoins,
};
use sail_sql_parser::ast::statement::Statement;
use sail_sql_parser::options::ParserOptions;
use url::Url;

use crate::dataset::{DatasetIdentifier, LineageDataset};

/// A database with an OpenLineage naming convention for its datasets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Dialect {
    Postgres,
    Redshift,
    MySql,
    SqlServer,
}

impl Dialect {
    fn from_jdbc_subprotocol(value: &str) -> Option<Self> {
        match value.to_ascii_lowercase().as_str() {
            "postgresql" => Some(Self::Postgres),
            "redshift" => Some(Self::Redshift),
            "mysql" => Some(Self::MySql),
            "sqlserver" => Some(Self::SqlServer),
            _ => None,
        }
    }

    fn scheme(self) -> &'static str {
        match self {
            Self::Postgres => "postgres",
            Self::Redshift => "redshift",
            Self::MySql => "mysql",
            Self::SqlServer => "mssql",
        }
    }

    fn default_port(self) -> u16 {
        match self {
            Self::Postgres => 5432,
            Self::Redshift => 5439,
            Self::MySql => 3306,
            Self::SqlServer => 1433,
        }
    }

    /// The schema of unqualified table names.
    /// MySQL has no schemas, so table names are `{database}.{table}`.
    fn default_schema(self) -> Option<&'static str> {
        match self {
            Self::Postgres | Self::Redshift => Some("public"),
            Self::SqlServer => Some("dbo"),
            Self::MySql => None,
        }
    }
}

/// The database that a JDBC URL connects to.
#[derive(Debug, PartialEq, Eq)]
struct JdbcTarget {
    dialect: Dialect,
    namespace: String,
    database: Option<String>,
}

/// Returns the input datasets of a JDBC read with the given reader options.
///
/// The namespace only contains the database scheme, host, and port, so credentials in the URL
/// (user info, query parameters, or properties) never appear in lineage events.
/// No dataset is returned if the URL is not supported or the table or query cannot be parsed.
pub fn jdbc_input_datasets(options: &[OptionLayer]) -> Vec<LineageDataset> {
    let get = |key: &str| find_option(options, key);
    let Some(target) = get("url").as_deref().and_then(parse_jdbc_url) else {
        debug!("skipped OpenLineage input for JDBC URL without a supported dialect");
        return vec![];
    };
    let sql = match (get("dbtable"), get("query")) {
        // The table option can be a table name or a subquery with an alias,
        // so it is parsed as the `FROM` clause of a query.
        (Some(table), _) => format!("SELECT * FROM {table}"),
        (None, Some(query)) => query,
        (None, None) => return vec![],
    };
    let Some(tables) = referenced_tables(&sql) else {
        warn!("skipped OpenLineage inputs for JDBC read: failed to parse the table or query");
        return vec![];
    };
    let mut seen = HashSet::new();
    tables
        .into_iter()
        .filter_map(|table| target.qualify(table))
        .filter(|name| seen.insert(name.clone()))
        .map(|name| LineageDataset {
            identifier: Some(DatasetIdentifier {
                namespace: target.namespace.clone(),
                name,
            }),
            ..Default::default()
        })
        .collect()
}

/// Returns the value of the option, where a later option layer overrides earlier ones.
fn find_option(options: &[OptionLayer], key: &str) -> Option<String> {
    options
        .iter()
        .rev()
        .filter_map(|layer| match layer {
            OptionLayer::OptionList { items } | OptionLayer::TablePropertyList { items } => {
                items.iter().find(|(k, _)| k.eq_ignore_ascii_case(key))
            }
            _ => None,
        })
        .map(|(_, v)| v.trim().to_string())
        .find(|v| !v.is_empty())
}

fn parse_jdbc_url(url: &str) -> Option<JdbcTarget> {
    let url = url.trim();
    let rest = url
        .get(..5)
        .filter(|x| x.eq_ignore_ascii_case("jdbc:"))
        .map(|_| &url[5..])?;
    let (subprotocol, address) = rest.split_once("://")?;
    let dialect = Dialect::from_jdbc_subprotocol(subprotocol)?;
    let (host, port, database) = match dialect {
        Dialect::SqlServer => parse_sqlserver_address(address)?,
        Dialect::Postgres | Dialect::Redshift | Dialect::MySql => {
            let url = Url::parse(rest).ok()?;
            let database = url
                .path_segments()
                .and_then(|mut x| x.next())
                .filter(|x| !x.is_empty())
                .map(|x| x.to_string());
            (url.host_str()?.to_string(), url.port(), database)
        }
    };
    if host.is_empty() {
        return None;
    }
    let host = match dialect {
        Dialect::Redshift => redshift_cluster(&host).unwrap_or(host),
        _ => host,
    };
    let port = port.unwrap_or(dialect.default_port());
    Some(JdbcTarget {
        dialect,
        namespace: format!("{}://{host}:{port}", dialect.scheme()),
        database,
    })
}

/// Parses the address of a SQL Server JDBC URL, which has the form
/// `host[\instance][:port][;property=value]*` instead of a URL path.
fn parse_sqlserver_address(address: &str) -> Option<(String, Option<u16>, Option<String>)> {
    let mut parts = address.split(';');
    let server = parts.next().unwrap_or_default();
    let properties = parts
        .filter_map(|x| x.split_once('='))
        .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim()))
        .collect::<Vec<_>>();
    let property = |key: &str| {
        properties
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.to_string())
            .filter(|x| !x.is_empty())
    };
    let (server, port) = match server.rsplit_once(':') {
        Some((server, port)) => (server, Some(port.parse().ok()?)),
        None => (server, None),
    };
    // The instance name is not part of the dataset namespace.
    let host = server.split('\\').next().unwrap_or_default();
    let host = match host {
        "" => property("servername").unwrap_or_default(),
        x => x.to_string(),
    };
    let port = match port {
        Some(x) => Some(x),
        None => property("portnumber")
            .or_else(|| property("port"))
            .map(|x| x.parse().ok())
            .unwrap_or(None),
    };
    let database = property("databasename").or_else(|| property("database"));
    Some((host, port, database))
}

/// Returns `{cluster_identifier}.{region}` for an AWS Redshift endpoint host such as
/// `cluster.abc123.us-west-2.redshift.amazonaws.com`.
fn redshift_cluster(host: &str) -> Option<String> {
    let labels = host.split('.').collect::<Vec<_>>();
    match labels.as_slice() {
        [
            cluster,
            _,
            region,
            "redshift" | "redshift-serverless",
            "amazonaws",
            "com",
        ] => Some(format!("{cluster}.{region}")),
        _ => None,
    }
}

impl JdbcTarget {
    /// Returns the dataset name for a table name with one or more parts.
    fn qualify(&self, table: Vec<String>) -> Option<String> {
        let database = || self.database.clone();
        let parts = match (self.dialect.default_schema(), table.as_slice()) {
            (None, [table]) => vec![database()?, table.clone()],
            (None, [_, _]) => table,
            (Some(schema), [table]) => vec![database()?, schema.to_string(), table.clone()],
            (Some(_), [schema, table]) => vec![database()?, schema.clone(), table.clone()],
            (Some(_), [_, _, _]) => table,
            _ => return None,
        };
        Some(parts.join("."))
    }
}

/// Returns the names of the tables referenced in a SQL query, or [`None`]
/// if the SQL cannot be parsed as a single query.
fn referenced_tables(sql: &str) -> Option<Vec<Vec<String>>> {
    // The SQL is sent to the database rather than Sail, so standard SQL quoting is used.
    let options = ParserOptions {
        allow_dual_quote_escape: true,
        allow_double_quote_identifier: true,
        ..Default::default()
    };
    let statements = parse_statements_with_options(sql, &options).ok()?;
    let [Statement::Query(query)] = statements.as_slice() else {
        return None;
    };
    let mut collector = TableCollector::default();
    collector.query(query);
    Some(collector.tables)
}

/// Collects table names from a query, including tables in joins, derived tables,
/// common table expressions, set operations, and subqueries in common expressions.
#[derive(Default)]
struct TableCollector {
    cte_names: HashSet<String>,
    tables: Vec<Vec<String>>,
}

impl TableCollector {
    fn query(&mut self, query: &Query) {
        if let Some(with) = &query.with {
            for cte in with.ctes.items() {
                self.cte_names.insert(cte.name.value.to_lowercase());
                self.query(&cte.query);
            }
        }
        self.body(&query.body);
    }

    fn body(&mut self, body: &QueryBody) {
        match body {
            QueryBody::Term(term) => self.term(term),
            QueryBody::SetOperation { left, right, .. } => {
                self.body(left);
                self.body(right);
            }
        }
    }

    fn term(&mut self, term: &QueryTerm) {
        match term {
            QueryTerm::Select(select) => self.select(select),
            QueryTerm::Table(_, name) => self.table(name),
            QueryTerm::Values(_) => {}
            QueryTerm::Nested(_, query, _) => self.query(query),
        }
    }

    fn select(&mut self, select: &QuerySelect) {
        for item in select.select.projection.items() {
            self.expr(&item.expr);
        }
        if let Some(from) = &select.from {
            for table in from.tables.items() {
                self.table_with_joins(table);
            }
        }
        if let Some(clause) = &select.r#where {
            self.expr(&clause.condition);
        }
        if let Some(clause) = &select.having {
            self.expr(&clause.condition);
        }
    }

    fn table_with_joins(&mut self, table: &TableWithJoins) {
        self.factor(&table.table);
        for join in &table.joins {
            self.factor(&join.other);
            if let Some(JoinCriteria::On(_, condition)) = &join.criteria {
                self.expr(condition);
            }
        }
    }

    fn factor(&mut self, factor: &TableFactor) {
        match factor {
            TableFactor::Name { name, .. } => self.table(name),
            TableFactor::Query { query, .. } => self.query(query),
            TableFactor::Nested { table, .. } => self.table_with_joins(table),
            TableFactor::Values { .. }
            | TableFactor::Identifier { .. }
            | TableFactor::TableFunction { .. } => {}
        }
    }

    fn table(&mut self, name: &ObjectName) {
        let parts = name.0.items().map(|x| x.value.clone()).collect::<Vec<_>>();
        if let [name] = parts.as_slice()
            && self.cte_names.contains(&name.to_lowercase())
        {
            return;
        }
        self.tables.push(parts);
    }

    fn expr(&mut self, expr: &Expr) {
        match expr {
            Expr::Atom(atom) => self.atom(atom),
            Expr::UnaryOperator(_, x)
            | Expr::Wildcard(x, ..)
            | Expr::Field(x, ..)
            | Expr::Cast(x, ..)
            | Expr::IsFalse(x, ..)
            | Expr::IsTrue(x, ..)
            | Expr::IsUnknown(x, ..)
            | Expr::IsNull(x, ..) => self.expr(x),
            Expr::BinaryOperator(left, _, right)
            | Expr::Subscript(left, _, right, _)
            | Expr::IsDistinctFrom(left, .., right)
            | Expr::RLike(left, .., right)
            | Expr::RegExp(left, .., right)
            | Expr::Like(left, _, _, _, right, _)
            | Expr::ILike(left, _, _, _, right, _)
            | Expr::SimilarTo(left, _, _, _, right, _) => {
                self.expr(left);
                self.expr(right);
            }
            Expr::Between(x, _, _, low, _, high) => {
                self.expr(x);
                self.expr(low);
                self.expr(high);
            }
            Expr::InList(x, _, _, _, list, _) => {
                self.expr(x);
                for item in list.items() {
                    self.expr(item);
                }
            }
            Expr::InSubquery(x, _, _, _, query, _) => {
                self.expr(x);
                self.query(query);
            }
        }
    }

    fn atom(&mut self, atom: &AtomExpr) {
        match atom {
            AtomExpr::Subquery(_, query, _) | AtomExpr::Exists(_, _, query, _) => self.query(query),
            AtomExpr::Nested(_, expr, _) => self.expr(expr),
            AtomExpr::Tuple(_, items, _) => {
                for item in items.items() {
                    self.expr(&item.expr);
                }
            }
            AtomExpr::Function(function) => {
                for argument in function.arguments.arguments.iter().flat_map(|x| x.items()) {
                    match argument {
                        FunctionArgument::Named(_, _, expr) | FunctionArgument::Unnamed(expr) => {
                            self.expr(expr)
                        }
                    }
                }
            }
            AtomExpr::Case {
                operand,
                first_condition,
                other_conditions,
                r#else,
                ..
            } => {
                if let Some(operand) = operand {
                    self.expr(operand);
                }
                for case in std::iter::once(first_condition.as_ref()).chain(other_conditions) {
                    self.expr(&case.condition);
                    self.expr(&case.result);
                }
                if let Some(r#else) = r#else {
                    self.expr(&r#else.result);
                }
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options(items: &[(&str, &str)]) -> Vec<OptionLayer> {
        vec![OptionLayer::OptionList {
            items: items
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }]
    }

    /// Returns the `(namespace, name)` pairs of the JDBC input datasets.
    fn inputs(items: &[(&str, &str)]) -> Vec<(String, String)> {
        jdbc_input_datasets(&options(items))
            .into_iter()
            .filter_map(|x| x.identifier)
            .map(|x| (x.namespace, x.name))
            .collect()
    }

    fn pairs(namespace: &str, names: &[&str]) -> Vec<(String, String)> {
        names
            .iter()
            .map(|x| (namespace.to_string(), x.to_string()))
            .collect()
    }

    #[test]
    fn test_parse_postgres_url() {
        let target = parse_jdbc_url("jdbc:postgresql://db.example.com:6543/sales?ssl=true");
        assert_eq!(
            target,
            Some(JdbcTarget {
                dialect: Dialect::Postgres,
                namespace: "postgres://db.example.com:6543".to_string(),
                database: Some("sales".to_string()),
            })
        );
        let target = parse_jdbc_url("JDBC:PostgreSQL://localhost/sales");
        assert_eq!(
            target.map(|x| x.namespace),
            Some("postgres://localhost:5432".to_string())
        );
        assert_eq!(parse_jdbc_url("postgresql://localhost/sales"), None);
        assert_eq!(parse_jdbc_url("jdbc:postgresql:sales"), None);
    }

    #[test]
    fn test_credentials_are_stripped() {
        let urls = [
            "jdbc:postgresql://admin:s3cret@localhost:5432/sales",
            "jdbc:postgresql://localhost:5432/sales?user=admin&password=s3cret",
            "jdbc:mysql://admin:s3cret@localhost/sales?password=s3cret",
            "jdbc:sqlserver://localhost;databaseName=sales;user=admin;password=s3cret",
        ];
        for url in urls {
            let datasets = jdbc_input_datasets(&options(&[
                ("url", url),
                ("dbtable", "orders"),
                ("user", "admin"),
                ("password", "s3cret"),
            ]));
            assert_eq!(datasets.len(), 1, "{url}");
            for dataset in datasets {
                let Some(input) = dataset.to_input("sail") else {
                    unreachable!("the JDBC dataset should have an identifier");
                };
                let event = serde_json::to_string(&input).unwrap_or_default();
                assert!(event.contains("localhost"), "{event}");
                assert!(!event.contains("s3cret"), "{event}");
                assert!(!event.contains("admin"), "{event}");
            }
        }
    }

    #[test]
    fn test_default_schema() {
        let url = "jdbc:postgresql://localhost:5432/sales";
        assert_eq!(
            inputs(&[("url", url), ("dbtable", "orders")]),
            pairs("postgres://localhost:5432", &["sales.public.orders"])
        );
        assert_eq!(
            inputs(&[("url", url), ("dbtable", "\"Reporting\".\"Orders\"")]),
            pairs("postgres://localhost:5432", &["sales.Reporting.Orders"])
        );
        assert_eq!(
            inputs(&[("url", url), ("dbtable", "other.public.orders")]),
            pairs("postgres://localhost:5432", &["other.public.orders"])
        );
        // An unqualified table cannot be named without the database in the URL.
        assert!(
            inputs(&[
                ("url", "jdbc:postgresql://localhost/"),
                ("dbtable", "orders")
            ])
            .is_empty()
        );
    }

    #[test]
    fn test_subqueries() {
        let url = "jdbc:postgresql://localhost:5432/sales";
        let dbtable = "(SELECT o.id, c.name FROM orders o JOIN crm.customers c ON o.customer_id = c.id \
            WHERE o.id IN (SELECT order_id FROM refunds)) AS t";
        assert_eq!(
            inputs(&[("url", url), ("dbtable", dbtable)]),
            pairs(
                "postgres://localhost:5432",
                &[
                    "sales.public.orders",
                    "sales.crm.customers",
                    "sales.public.refunds"
                ]
            )
        );
        let query = "WITH recent AS (SELECT * FROM orders WHERE created_at > now() - interval '1 day') \
            SELECT * FROM recent UNION ALL SELECT * FROM archive.orders \
            WHERE EXISTS (SELECT 1 FROM recent r WHERE r.id = archive.orders.id)";
        assert_eq!(
            inputs(&[("url", url), ("query", query)]),
            pairs(
                "postgres://localhost:5432",
                &["sales.public.orders", "sales.archive.orders"]
            )
        );
    }

    #[test]
    fn test_unparseable_sql() {
        let url = "jdbc:postgresql://localhost:5432/sales";
        assert!(inputs(&[("url", url), ("query", "SELECT * FROM (")]).is_empty());
        assert!(inputs(&[("url", url), ("query", "DELETE FROM orders")]).is_empty());
        assert!(inputs(&[("url", url)]).is_empty());
    }

    #[test]
    fn test_other_dialects() {
        assert_eq!(
            inputs(&[
                ("url", "jdbc:mysql://db.example.com/shop"),
                (
                    "query",
                    "SELECT * FROM `orders` JOIN audit.events USING (id)"
                ),
            ]),
            pairs(
                "mysql://db.example.com:3306",
                &["shop.orders", "audit.events"]
            )
        );
        assert_eq!(
            inputs(&[
                (
                    "url",
                    "jdbc:sqlserver://db.example.com\\prod:1444;databaseName=sales;encrypt=true"
                ),
                ("dbtable", "orders"),
            ]),
            pairs("mssql://db.example.com:1444", &["sales.dbo.orders"])
        );
        assert_eq!(
            inputs(&[
                (
                    "url",
                    "jdbc:redshift://analytics.abc123.us-west-2.redshift.amazonaws.com:5439/dev"
                ),
                ("dbtable", "sales.orders"),
            ]),
            pairs("redshift://analytics.us-west-2:5439", &["dev.sales.orders"])
        );
        assert!(inputs(&[("url", "jdbc:sqlite:/tmp/data.db"), ("dbtable", "orders")]).is_empty());
        assert!(
            inputs(&[
                ("url", "jdbc:oracle:thin:@//localhost:1521/service"),
                ("dbtable", "orders"),
            ])
            .is_empty()
        );
    }

    #[test]
    fn test_later_option_layer_wins() {
        let options = vec![
            OptionLayer::OptionList {
                items: vec![("url".to_string(), "jdbc:mysql://old/db".to_string())],
            },
            OptionLayer::OptionList {
                items: vec![
                    ("URL".to_string(), "jdbc:mysql://new/db".to_string()),
                    ("dbtable".to_string(), "t".to_string()),
                ],
            },
        ];
        let datasets = jdbc_input_datasets(&options);
        assert_eq!(
            datasets
                .into_iter()
                .filter_map(|x| x.identifier)
                .map(|x| x.namespace)
                .collect::<Vec<_>>(),
            vec!["mysql://new:3306".to_string()]
        );
    }
}
