//! Chumsky grammar for the client generator's supported Thrift IDL subset.

use std::collections::{BTreeMap, BTreeSet};

use chumsky::prelude::*;

use super::{Definition, Field, Literal, Method, Schema, Type, invalid};
use crate::error::BuildResult;

#[derive(Debug)]
enum Declaration {
    Ignored,
    Definition(String, Definition),
    Service(String, Vec<Method>),
}

fn grammar<'a>() -> impl Parser<'a, &'a str, Vec<Declaration>, extra::Err<Rich<'a, char>>> {
    let line_comment = choice((just("//"), just("#")))
        .then(none_of('\n').repeated())
        .ignored();
    let block_comment = just("/*")
        .then(any().and_is(just("*/").not()).repeated())
        .then_ignore(just("*/"))
        .ignored();
    let padding = choice((
        text::whitespace().at_least(1).ignored(),
        line_comment,
        block_comment,
    ))
    .repeated();
    let ident = text::ascii::ident().map(str::to_owned).padded_by(padding);
    let symbol = |c| just(c).padded_by(padding);
    let keyword = |s| text::keyword(s).padded_by(padding);
    let delimiter = one_of(",;").padded_by(padding).or_not().ignored();

    let integer = just('-')
        .or_not()
        .then(text::int(10))
        .to_slice()
        .try_map(|s: &str, span| {
            s.parse::<i64>()
                .map_err(|_| Rich::custom(span, "integer out of range"))
        })
        .padded_by(padding);
    let reference = ident
        .separated_by(symbol('.'))
        .at_least(1)
        .collect::<Vec<_>>();
    let string = |quote| {
        any()
            .filter(move |c| *c != quote && *c != '\\')
            .or(just('\\').ignore_then(choice((
                just('n').to('\n'),
                just('r').to('\r'),
                just('t').to('\t'),
                one_of("\\\"'"),
            ))))
            .repeated()
            .collect::<String>()
            .delimited_by(just(quote), just(quote))
            .padded_by(padding)
    };

    let number = just('-')
        .or_not()
        .then(text::int(10))
        .then(just('.').then(text::digits(10)).or_not())
        .then(
            one_of("eE")
                .then(one_of("+-").or_not())
                .then(text::digits(10))
                .or_not(),
        )
        .to_slice()
        .try_map(|s: &str, span| {
            if s.contains(['.', 'e', 'E']) {
                s.parse::<f64>()
                    .ok()
                    .filter(|n| n.is_finite())
                    .map(Literal::Double)
                    .ok_or_else(|| Rich::custom(span, "invalid floating point value"))
            } else {
                s.parse()
                    .map(Literal::Integer)
                    .map_err(|_| Rich::custom(span, "integer out of range"))
            }
        })
        .padded_by(padding);

    let literal = choice((
        string('"').or(string('\'')).map(Literal::String),
        keyword("true").to(Literal::Bool(true)),
        keyword("false").to(Literal::Bool(false)),
        number,
        reference.map(Literal::Reference),
    ))
    .boxed();

    let ty = recursive(|ty| {
        choice((
            keyword("list")
                .ignore_then(ty.clone().delimited_by(symbol('<'), symbol('>')))
                .map(|t| Type::List(Box::new(t))),
            keyword("set")
                .ignore_then(ty.clone().delimited_by(symbol('<'), symbol('>')))
                .map(|t| Type::Set(Box::new(t))),
            keyword("map")
                .ignore_then(
                    ty.clone()
                        .then_ignore(symbol(','))
                        .then(ty)
                        .delimited_by(symbol('<'), symbol('>')),
                )
                .map(|(k, v)| Type::Map(Box::new(k), Box::new(v))),
            reference.map(|n| Type::Named(n.join("."))),
        ))
    });

    let field = integer
        .try_map(|n, span| {
            i16::try_from(n).map_err(|_| Rich::custom(span, "field ID out of range"))
        })
        .then_ignore(symbol(':'))
        .then(choice((keyword("required").to(true), keyword("optional").to(false))).or_not())
        .then(ty.clone())
        .then(ident)
        .then(symbol('=').ignore_then(literal.clone()).or_not())
        .then_ignore(delimiter)
        .map(|((((id, required), ty), name), default)| Field {
            id,
            name,
            ty,
            required: required.unwrap_or(false),
            default,
        });
    let fields = field.repeated().collect::<Vec<_>>().boxed();

    let record = choice((keyword("struct"), keyword("exception"), keyword("union")))
        .then(ident)
        .then(fields.clone().delimited_by(symbol('{'), symbol('}')))
        .map(|((kind, name), fields)| {
            let def = match kind {
                "struct" => Definition::Struct(fields),
                "exception" => Definition::Exception(fields),
                _ => Definition::Unsupported,
            };
            Declaration::Definition(name, def)
        });

    let enumeration = keyword("enum")
        .ignore_then(ident)
        .then(
            ident
                .then(symbol('=').ignore_then(integer).or_not())
                .then_ignore(delimiter)
                .repeated()
                .collect::<Vec<_>>()
                .delimited_by(symbol('{'), symbol('}')),
        )
        .try_map(|(name, variants), span| {
            let mut next = 0_i64;
            let mut names = BTreeSet::new();
            let values = variants
                .into_iter()
                .map(|(name, value)| {
                    if !names.insert(name.clone()) {
                        return Err(Rich::custom(span, "duplicate enum variant"));
                    }

                    let value = i32::try_from(value.unwrap_or(next))
                        .map_err(|_| Rich::custom(span, "enum value out of range"))?;
                    next = i64::from(value) + 1;
                    Ok((name, value))
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Declaration::Definition(name, Definition::Enum(values)))
        });

    let method = ty
        .clone()
        .then(ident)
        .then(fields.clone().delimited_by(symbol('('), symbol(')')))
        .then(
            keyword("throws")
                .ignore_then(fields.delimited_by(symbol('('), symbol(')')))
                .or_not(),
        )
        .then_ignore(delimiter)
        .map(|(((result, name), args), exceptions)| Method {
            name,
            result,
            args,
            exceptions: exceptions.unwrap_or_default(),
        });

    let service = keyword("service")
        .ignore_then(ident)
        .then(
            method
                .repeated()
                .collect::<Vec<_>>()
                .delimited_by(symbol('{'), symbol('}')),
        )
        .map(|(name, methods)| Declaration::Service(name, methods));

    let namespace = keyword("namespace")
        .ignore_then(ident.or(symbol('*').to("*".to_owned())))
        .then(reference)
        .map(|_| Declaration::Ignored);

    let constant = keyword("const")
        .ignore_then(ty)
        .then(ident)
        .then_ignore(symbol('='))
        .then(literal)
        .map(|_| Declaration::Ignored);

    choice((namespace, constant, record, enumeration, service))
        .then_ignore(delimiter)
        .repeated()
        .collect()
        .padded_by(padding)
        .then_ignore(end())
}

fn check_fields(fields: &[Field]) -> BuildResult<()> {
    let mut ids = BTreeSet::new();
    let mut names = BTreeSet::new();
    for field in fields {
        if !ids.insert(field.id) || !names.insert(&field.name) {
            return Err(invalid(format!(
                "duplicate Thrift field {}: {}",
                field.id, field.name
            )));
        }
    }

    Ok(())
}

pub(super) fn parse(source: &str) -> BuildResult<Schema> {
    let declarations = grammar().parse(source).into_result().map_err(|errors| {
        invalid(
            errors
                .into_iter()
                .map(|error| {
                    format!(
                        "Thrift IDL at bytes {}..{}: {error}",
                        error.span().start,
                        error.span().end
                    )
                })
                .collect::<Vec<_>>()
                .join("\n"),
        )
    })?;
    let mut definitions = BTreeMap::new();
    let mut service = None;
    let mut methods = BTreeMap::new();
    for declaration in declarations {
        match declaration {
            Declaration::Ignored => {}
            Declaration::Definition(name, definition) => {
                if let Definition::Struct(fields) | Definition::Exception(fields) = &definition {
                    check_fields(fields)?;
                }

                if definitions.insert(name.clone(), definition).is_some() {
                    return Err(invalid(format!("duplicate Thrift type {name}")));
                }
            }
            Declaration::Service(name, entries) => {
                if service.replace(name).is_some() {
                    return Err(invalid("expected exactly one Thrift service"));
                }

                for method in entries {
                    check_fields(&method.args)?;
                    check_fields(&method.exceptions)?;
                    if methods.insert(method.name.clone(), method).is_some() {
                        return Err(invalid("duplicate Thrift method"));
                    }
                }
            }
        }
    }

    Ok(Schema {
        definitions,
        service: service.ok_or_else(|| invalid("missing Thrift service"))?,
        methods,
    })
}

#[cfg(test)]
mod tests {
    use super::{Definition, Literal, Type, parse};
    use crate::error::BuildResult;

    #[test]
    fn parses_comments_defaults_and_nested_containers() -> BuildResult<()> {
        let schema = parse(
            r#"
            namespace * example.catalog
            # Ignored declarations still follow the grammar.
            const string LABEL = 'ignored';
            /* Multi-line
               comment */
            enum State { FIRST = -2; NEXT, LAST = 2147483647 }

            struct Request {
                -7: required list<map<string, set<i64>>> values;
                2: optional string label = 'line\n\'quoted\'';
                3: optional State state = State.NEXT // Default enum reference
            }
            service Catalog { list<string> find(1: Request request) }
        "#,
        )?;
        assert_eq!(schema.service, "Catalog");
        assert!(
            matches!(&schema.definitions["State"], Definition::Enum(values)
            if values == &[("FIRST".into(), -2), ("NEXT".into(), -1), ("LAST".into(), i32::MAX)])
        );
        let Definition::Struct(fields) = &schema.definitions["Request"] else {
            return Err(super::invalid("expected Request struct"));
        };
        assert_eq!(fields[0].id, -7);
        assert!(fields[0].required);
        assert!(matches!(&fields[0].ty, Type::List(inner) if matches!(**inner, Type::Map(_, _))));
        assert!(matches!(&fields[1].default, Some(Literal::String(s)) if s == "line\n'quoted'"));
        assert!(
            matches!(&fields[2].default, Some(Literal::Reference(path)) if path == &["State", "NEXT"])
        );
        assert_eq!(schema.methods["find"].args[0].id, 1);
        Ok(())
    }

    #[test]
    fn rejects_malformed_and_ambiguous_idl() {
        for source in [
            "/* unterminated",
            "struct X { 32768: string value } service S {}",
            "struct X { 1: string x, 1: string y } service S {}",
            "struct X { 1: string x, 2: string x } service S {}",
            "enum X { A = 9223372036854775807 } service S {}",
            "enum X { A = 2147483647, B } service S {}",
            "enum X { A, A } service S {}",
            "service S { void m() void m() }",
            "service S {} service T {}",
            "include 'unsupported.thrift' service S {}",
            "service S { void m(1: list<string arg) }",
        ] {
            assert!(parse(source).is_err(), "accepted malformed IDL: {source}");
        }

        let result = parse("service S { void m(");
        assert!(matches!(result, Err(e) if e.to_string().contains("at bytes")));
    }
}
