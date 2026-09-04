use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::thread::LocalKey;

use chumsky::cache::{Cache, Cached};
use chumsky::error::Rich;
use chumsky::input::{Input, MappedInput};
use chumsky::span::SimpleSpan;
use chumsky::{Boxed, Parser, extra};
use sail_sql_parser::ast::data_type::DataType;
use sail_sql_parser::ast::expression::{Expr, IntervalLiteral};
use sail_sql_parser::ast::identifier::{ObjectName, QualifiedWildcard};
use sail_sql_parser::ast::query::NamedExpr;
use sail_sql_parser::ast::statement::Statement;
use sail_sql_parser::lexer::create_lexer;
use sail_sql_parser::options::ParserOptions;
use sail_sql_parser::parser::{
    create_data_type_parser, create_expression_parser, create_interval_literal_parser,
    create_named_expression_parser, create_object_name_parser, create_parser,
    create_qualified_wildcard_parser,
};
use sail_sql_parser::token::{Punctuation, Token};

use crate::error::{SqlError, SqlResult};
use crate::literal::datetime::{
    DateValue, TimeValue, TimestampValue, create_date_parser, create_time_parser,
    create_timestamp_parser,
};
use crate::literal::interval::{IntervalValue, parse_unqualified_interval_string};

fn map_parser_input<'a>(
    (t, s): &'a (Token<'a>, SimpleSpan<usize>),
) -> (&'a Token<'a>, &'a SimpleSpan<usize>) {
    (t, s)
}

type MapFn<'a> = fn(&'a (Token<'a>, SimpleSpan<usize>)) -> (&'a Token<'a>, &'a SimpleSpan<usize>);
type TokenInput<'a> =
    MappedInput<'a, Token<'a>, SimpleSpan<usize>, &'a [(Token<'a>, SimpleSpan<usize>)], MapFn<'a>>;
type TokenExtra<'a> = extra::Err<Rich<'a, Token<'a>, SimpleSpan<usize>>>;
type CharExtra<'a> = extra::Err<Rich<'a, char, SimpleSpan<usize>>>;
type Tokens<'a> = Vec<(Token<'a>, SimpleSpan<usize>)>;

/// Builds a parser once per thread instead of once per call.
///
/// The grammars behind `statement()` and `expression()` use
/// `Recursive::declare()`, whose definition holds a strong `Rc` to itself, so
/// every construction leaks. A thread keeps what it builds even after it exits,
/// so parse these only on threads that outlive the process: moving a parse onto
/// the blocking pool or a spawned thread turns a fixed cost back into a leak.
macro_rules! cached_parser {
    ($cache:ident, $output:ty, $factory:ident $(,)?) => {
        struct $cache(&'static ParserOptions);

        impl Cached for $cache {
            type Parser<'a> = Boxed<'a, 'a, TokenInput<'a>, $output, TokenExtra<'a>>;

            fn make_parser<'a>(self) -> Self::Parser<'a> {
                $factory(self.0).boxed()
            }
        }
    };
}

struct LexerCache(&'static ParserOptions);

impl Cached for LexerCache {
    type Parser<'a> = Boxed<'a, 'a, &'a str, Tokens<'a>, CharExtra<'a>>;

    fn make_parser<'a>(self) -> Self::Parser<'a> {
        create_lexer(self.0).boxed()
    }
}

cached_parser!(DataTypeCache, DataType, create_data_type_parser);
cached_parser!(ExpressionCache, Expr, create_expression_parser);
cached_parser!(StatementCache, Vec<Statement>, create_parser);
cached_parser!(ObjectNameCache, ObjectName, create_object_name_parser);
cached_parser!(
    QualifiedWildcardCache,
    QualifiedWildcard,
    create_qualified_wildcard_parser,
);
cached_parser!(NamedExprCache, NamedExpr, create_named_expression_parser);
cached_parser!(
    IntervalLiteralCache,
    IntervalLiteral,
    create_interval_literal_parser,
);

/// One parser per dialect, per thread: sessions sharing a dialect share a
/// parser, and the map holds only the dialects actually used.
type DialectCaches<C> = RefCell<HashMap<ParserOptions, Rc<Cache<C>>>>;

/// Returns the parser for a dialect, building it on first use.
///
/// `Cached::make_parser` is quantified over the parser lifetime, so the options
/// it borrows must be `'static`: every cache map leaks one `ParserOptions` per
/// thread per dialect — up to one per parser kind, since each kind misses
/// independently. The map is never borrowed across a build, which would make future
/// re-entry a panic instead of a compile error, and `try_with` keeps a parse
/// during thread-local teardown from panicking in a destructor.
fn cache_for<C: Cached>(
    caches: &'static LocalKey<DialectCaches<C>>,
    options: &ParserOptions,
    make: impl FnOnce(&'static ParserOptions) -> C,
) -> Rc<Cache<C>> {
    let cached = caches.try_with(|caches| caches.borrow().get(options).map(Rc::clone));
    if let Ok(Some(cache)) = cached {
        return cache;
    }
    let cache = Rc::new(Cache::new(make(Box::leak(Box::new(options.clone())))));
    if cached.is_ok() {
        let _ = caches.try_with(|caches| {
            caches
                .borrow_mut()
                .insert(options.clone(), Rc::clone(&cache));
        });
    }
    cache
}

thread_local! {
    static LEXER: DialectCaches<LexerCache> = RefCell::new(HashMap::new());
    static DATA_TYPE: DialectCaches<DataTypeCache> = RefCell::new(HashMap::new());
    static EXPRESSION: DialectCaches<ExpressionCache> = RefCell::new(HashMap::new());
    static STATEMENT: DialectCaches<StatementCache> = RefCell::new(HashMap::new());
    static OBJECT_NAME: DialectCaches<ObjectNameCache> = RefCell::new(HashMap::new());
    static QUALIFIED_WILDCARD: DialectCaches<QualifiedWildcardCache> = RefCell::new(HashMap::new());
    static NAMED_EXPR: DialectCaches<NamedExprCache> = RefCell::new(HashMap::new());
    static INTERVAL_LITERAL: DialectCaches<IntervalLiteralCache> = RefCell::new(HashMap::new());
}

fn tokenize<'a>(input: &'a str, options: &ParserOptions) -> SqlResult<Tokens<'a>> {
    let cache = cache_for(&LEXER, options, LexerCache);
    cache
        .get()
        .parse(input)
        .into_result()
        .map_err(SqlError::parser)
}

macro_rules! parse {
    ($input:ident, $caches:ident, $cache:ident $(,)?) => {{
        let options = ParserOptions::default();
        let length = $input.len();
        let tokens = tokenize($input, &options)?;
        let tokens = tokens
            .as_slice()
            .map((length..length).into(), map_parser_input as MapFn);
        let cache = cache_for(&$caches, &options, $cache);
        cache
            .get()
            .parse(tokens)
            .into_result()
            .map_err(SqlError::parser)
    }};
}

/// The same, for the dialect-independent parsers. They take no options, so
/// there is nothing to key on, and none of them is recursive, so none leaked:
/// they are cached so every entry point here reaches its parser the same way.
macro_rules! simple_parser {
    ($cache:ident, $output:ty, $factory:ident $(,)?) => {
        struct $cache;

        impl Cached for $cache {
            type Parser<'a> = Boxed<'a, 'a, &'a str, $output, CharExtra<'a>>;

            fn make_parser<'a>(self) -> Self::Parser<'a> {
                $factory().boxed()
            }
        }
    };
}

simple_parser!(DateCache, DateValue, create_date_parser);
simple_parser!(TimeCache, TimeValue, create_time_parser);
// `'a` is the one `Parser<'a>` introduces: a timestamp borrows its timezone.
simple_parser!(TimestampCache, TimestampValue<'a>, create_timestamp_parser);

thread_local! {
    static DATE: Cache<DateCache> = Cache::new(DateCache);
    static TIME: Cache<TimeCache> = Cache::new(TimeCache);
    static TIMESTAMP: Cache<TimestampCache> = Cache::new(TimestampCache);
}

macro_rules! parse_simple {
    ($input:ident, $caches:ident, $cache:ident $(,)?) => {
        $caches
            .try_with(|cache| cache.get().parse($input).into_result())
            .unwrap_or_else(|_| Cache::new($cache).get().parse($input).into_result())
            .map_err(SqlError::parser)
    };
}

pub fn rewrite_positional_parameter_markers(s: &str) -> SqlResult<(String, usize)> {
    let tokens = tokenize(s, &ParserOptions::default())?;

    let mut output = String::with_capacity(s.len());
    let mut last = 0;
    let mut count = 0;
    for (token, span) in tokens {
        if matches!(token, Token::Punctuation(Punctuation::QuestionMark)) {
            count += 1;
            output.push_str(&s[last..span.start]);
            output.push('$');
            output.push_str(&count.to_string());
            last = span.end;
        }
    }
    output.push_str(&s[last..]);
    Ok((output, count))
}

pub fn parse_data_type(s: &str) -> SqlResult<DataType> {
    parse!(s, DATA_TYPE, DataTypeCache)
}

pub fn parse_expression(s: &str) -> SqlResult<Expr> {
    parse!(s, EXPRESSION, ExpressionCache)
}

pub fn parse_statements(s: &str) -> SqlResult<Vec<Statement>> {
    parse!(s, STATEMENT, StatementCache)
}

/// Parses a SQL string containing exactly one statement into an AST.
pub fn parse_one_statement(s: &str) -> SqlResult<Statement> {
    let mut plan = parse_statements(s)?;
    match (plan.pop(), plan.is_empty()) {
        (Some(x), true) => Ok(x),
        _ => Err(SqlError::invalid("expected one statement")),
    }
}

pub fn parse_object_name(s: &str) -> SqlResult<ObjectName> {
    parse!(s, OBJECT_NAME, ObjectNameCache)
}

pub fn parse_qualified_wildcard(s: &str) -> SqlResult<QualifiedWildcard> {
    parse!(s, QUALIFIED_WILDCARD, QualifiedWildcardCache)
}

pub fn parse_named_expression(s: &str) -> SqlResult<NamedExpr> {
    parse!(s, NAMED_EXPR, NamedExprCache)
}

pub(crate) fn parse_interval_literal(s: &str) -> SqlResult<IntervalLiteral> {
    parse!(s, INTERVAL_LITERAL, IntervalLiteralCache)
}

pub fn parse_interval(s: &str) -> SqlResult<IntervalValue> {
    parse_unqualified_interval_string(s, false)
}

pub fn parse_date(s: &str) -> SqlResult<DateValue> {
    parse_simple!(s, DATE, DateCache)
}

pub fn parse_timestamp(s: &str) -> SqlResult<TimestampValue<'_>> {
    parse_simple!(s, TIMESTAMP, TimestampCache)
}

pub fn parse_time(s: &str) -> SqlResult<TimeValue> {
    parse_simple!(s, TIME, TimeCache)
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use sail_sql_parser::ast::query::Query;
    use sail_sql_parser::ast::statement::Statement;
    use sail_sql_parser::options::ParserOptions;
    use sail_sql_parser::tree::TreeText;

    use crate::error::SqlResult;
    use crate::parser::{
        STATEMENT, StatementCache, cache_for, parse_one_statement, parse_statements,
    };

    #[test]
    fn test_cached_parser_reuse() {
        let default = ParserOptions::default();
        let first = cache_for(&STATEMENT, &default, StatementCache);
        let second = cache_for(&STATEMENT, &default, StatementCache);
        assert!(
            Rc::ptr_eq(&first, &second),
            "equal options must reuse the parser"
        );

        let dialect = ParserOptions {
            allow_double_quote_identifier: true,
            ..Default::default()
        };
        let third = cache_for(&STATEMENT, &dialect, StatementCache);
        assert!(
            !Rc::ptr_eq(&first, &third),
            "differing options must build their own parser"
        );
        let fourth = cache_for(&STATEMENT, &dialect, StatementCache);
        assert!(
            Rc::ptr_eq(&third, &fourth),
            "the second dialect must be cached too"
        );
    }

    #[test]
    fn test_parse() -> SqlResult<()> {
        let sql = "/* */ ; SELECT 1;;; SELECT 2";
        let tree = parse_statements(sql)?;
        assert!(matches!(
            tree.as_slice(),
            [
                Statement::Query(Query { .. }),
                Statement::Query(Query { .. }),
            ]
        ));
        Ok(())
    }

    #[test]
    fn test_unparse() -> SqlResult<()> {
        assert_eq!(
            parse_one_statement("/* */ SELECT 1+1")?.text(),
            "SELECT 1 + 1 "
        );
        assert_eq!(
            parse_one_statement("Select  2*3 +(4*5)AS a, b '\\x01', $1,? -- comment")?.text(),
            "SELECT 2 * 3 + ( 4 * 5 ) AS a , b '\\x01' , $1 , ? "
        );
        assert_eq!(
            parse_one_statement("SELECT foo(0), cast(1L as decimal(10, -1)) FROM a.b")?.text(),
            "SELECT foo ( 0 ) , CAST ( 1L AS DECIMAL ( 10 , -1 ) ) FROM a . b "
        );
        assert_eq!(
            parse_one_statement("SELECT U&\"a#2014b#+002014c\"   UESCAPE '#'")?.text(),
            "SELECT U&\"a#2014b#+002014c\" UESCAPE '#' "
        );
        Ok(())
    }
}
