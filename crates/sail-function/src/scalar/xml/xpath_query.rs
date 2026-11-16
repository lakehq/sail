use chumsky::prelude::*;
use datafusion_common::{DataFusionError, Result};

use super::parser::{Extra, literal, name, whitespace};
use super::reader::{NAMESPACE_ATTRIBUTE, name_char};

/// Parse XPath 1.0 before adapting name tests and conversions to the XPath 3.1
/// engine. Literals remain opaque, and predicates retain their axis or filter
/// context, including the different position semantics of reverse axes.
pub(super) fn expression(path: &str) -> Result<String> {
    let expression = parser().parse(path).into_result().map_err(|errors| {
        DataFusionError::Execution(format!("Invalid XPath '{path}': {errors:?}"))
    })?;
    render(&expression)
}

#[derive(Clone, Debug)]
enum Expression<'a> {
    Literal(&'a str),
    Function(&'a str, Vec<Self>),
    Group(Box<Self>),
    Filter(Box<Self>, Vec<Self>),
    Path {
        root: &'a str,
        start: Option<Box<Self>>,
        steps: Vec<(&'a str, Step<'a>)>,
    },
    Negate(Box<Self>),
    Binary(Box<Self>, &'a str, Box<Self>),
}

#[derive(Clone, Debug)]
struct Step<'a> {
    axis: &'a str,
    test: NodeTest<'a>,
    predicates: Vec<Expression<'a>>,
}

#[derive(Clone, Debug)]
enum NodeTest<'a> {
    Name(&'a str),
    Kind(&'a str),
    ProcessingInstruction(Option<&'a str>),
}

fn keyword<'a>(word: &'static str) -> impl Parser<'a, &'a str, &'a str, Extra<'a>> + Clone {
    just(word)
        .then_ignore(any().filter(|c: &char| name_char(*c)).not())
        .padded_by(whitespace())
}

fn parser<'a>() -> impl Parser<'a, &'a str, Expression<'a>, Extra<'a>> {
    recursive(|expression| {
        let ncname = name(false);
        let qname = ncname
            .clone()
            .then(just(':').then(ncname.clone()).or_not())
            .to_slice();
        let name_test = ncname
            .clone()
            .then(just(':').then(ncname.clone().or(just("*"))).or_not())
            .to_slice()
            .or(just("*"));
        let predicate = expression.clone().delimited_by(
            just('[').padded_by(whitespace()),
            just(']').padded_by(whitespace()),
        );
        let kind = choice((just("node"), just("text"), just("comment")))
            .then_ignore(just('(').padded_by(whitespace()))
            .then_ignore(just(')').padded_by(whitespace()))
            .map(NodeTest::Kind);
        let instruction = just("processing-instruction")
            .ignore_then(
                literal()
                    .to_slice()
                    .padded_by(whitespace())
                    .or_not()
                    .delimited_by(
                        just('(').padded_by(whitespace()),
                        just(')').padded_by(whitespace()),
                    ),
            )
            .map(NodeTest::ProcessingInstruction);
        let test =
            choice((instruction, kind, name_test.map(NodeTest::Name))).padded_by(whitespace());
        let axis = ncname
            .clone()
            .padded_by(whitespace())
            .then_ignore(just("::").padded_by(whitespace()))
            .or(just('@').padded_by(whitespace()).to("attribute"))
            .or_not()
            .map(|axis| axis.unwrap_or("child"));
        let step = choice((
            just("..")
                .padded_by(whitespace())
                .to(("parent", NodeTest::Kind("node"))),
            just('.')
                .padded_by(whitespace())
                .to(("self", NodeTest::Kind("node"))),
            axis.then(test),
        ))
        .then(predicate.clone().repeated().collect())
        .map(|((axis, test), predicates)| Step {
            axis,
            test,
            predicates,
        });
        let separator = just("//").or(just("/")).padded_by(whitespace());
        let relative = step
            .clone()
            .then(separator.clone().then(step).repeated().collect::<Vec<_>>())
            .map(|(first, rest)| std::iter::once(("", first)).chain(rest).collect::<Vec<_>>());
        let location = choice((
            just("//")
                .padded_by(whitespace())
                .then(relative.clone())
                .map(|(root, steps)| Expression::Path {
                    root,
                    start: None,
                    steps,
                }),
            just('/')
                .padded_by(whitespace())
                .ignore_then(relative.clone().or_not())
                .map(|steps| Expression::Path {
                    root: "/",
                    start: None,
                    steps: steps.unwrap_or_default(),
                }),
            relative.clone().map(|steps| Expression::Path {
                root: "",
                start: None,
                steps,
            }),
        ));
        let digits = any().filter(char::is_ascii_digit).repeated().at_least(1);
        let number = choice((
            digits
                .then(
                    just('.')
                        .then(any().filter(char::is_ascii_digit).repeated())
                        .or_not(),
                )
                .ignored(),
            just('.')
                .then(any().filter(char::is_ascii_digit).repeated().at_least(1))
                .ignored(),
        ))
        .to_slice()
        .map(Expression::Literal);
        let function = qname
            .filter(|name: &&str| {
                !matches!(
                    *name,
                    "node" | "text" | "comment" | "processing-instruction"
                )
            })
            .padded_by(whitespace())
            .then(
                expression
                    .clone()
                    .separated_by(just(',').padded_by(whitespace()))
                    .collect()
                    .delimited_by(
                        just('(').padded_by(whitespace()),
                        just(')').padded_by(whitespace()),
                    ),
            )
            .map(|(name, args)| Expression::Function(name, args));
        let primary = choice((
            literal().to_slice().map(Expression::Literal),
            number,
            function,
            expression
                .clone()
                .delimited_by(
                    just('(').padded_by(whitespace()),
                    just(')').padded_by(whitespace()),
                )
                .map(|value| Expression::Group(Box::new(value))),
        ))
        .padded_by(whitespace());
        let filter =
            primary
                .then(predicate.repeated().collect::<Vec<_>>())
                .map(|(value, predicates)| {
                    if predicates.is_empty() {
                        value
                    } else {
                        Expression::Filter(Box::new(value), predicates)
                    }
                });
        let path = filter
            .then(separator.then(relative).or_not())
            .map(|(value, tail)| match tail {
                Some((separator, mut steps)) => {
                    if let Some((first, _)) = steps.first_mut() {
                        *first = separator;
                    }
                    Expression::Path {
                        root: "",
                        start: Some(Box::new(value)),
                        steps,
                    }
                }
                None => value,
            })
            .or(location)
            .boxed();
        let union = path.clone().foldl(
            just('|')
                .padded_by(whitespace())
                .to("|")
                .then(path)
                .repeated(),
            binary,
        );
        let unary = just('-')
            .padded_by(whitespace())
            .repeated()
            .count()
            .then(union)
            .map(|(count, value)| {
                (0..count).fold(value, |value, _| Expression::Negate(Box::new(value)))
            })
            .boxed();
        let product = unary
            .clone()
            .foldl(
                choice((
                    just('*').padded_by(whitespace()).to("*"),
                    keyword("div"),
                    keyword("mod"),
                ))
                .then(unary)
                .repeated(),
                binary,
            )
            .boxed();
        let sum = product
            .clone()
            .foldl(
                choice((just('+').to("+"), just('-').to("-")))
                    .padded_by(whitespace())
                    .then(product)
                    .repeated(),
                binary,
            )
            .boxed();
        let relation = sum
            .clone()
            .foldl(
                choice((just("<="), just(">="), just("<"), just(">")))
                    .padded_by(whitespace())
                    .then(sum)
                    .repeated(),
                binary,
            )
            .boxed();
        let equality = relation
            .clone()
            .foldl(
                just("!=")
                    .or(just("="))
                    .padded_by(whitespace())
                    .then(relation)
                    .repeated(),
                binary,
            )
            .boxed();
        let and = equality
            .clone()
            .foldl(keyword("and").then(equality).repeated(), binary)
            .boxed();
        and.clone()
            .foldl(keyword("or").then(and).repeated(), binary)
    })
    .padded_by(whitespace())
}

fn binary<'a>(
    left: Expression<'a>,
    (operator, right): (&'a str, Expression<'a>),
) -> Expression<'a> {
    Expression::Binary(Box::new(left), operator, Box::new(right))
}

fn invalid(path: &str) -> DataFusionError {
    DataFusionError::Execution(format!("Invalid XPath '{path}'"))
}

fn render(expression: &Expression<'_>) -> Result<String> {
    match expression {
        Expression::Literal(value) => Ok((*value).to_string()),
        Expression::Function(name, args) => {
            function(name, args.iter().map(render).collect::<Result<_>>()?)
        }
        Expression::Group(value) => Ok(format!("({})", render(value)?)),
        Expression::Filter(value, predicates) => {
            let mut result = format!("({})", render(value)?);
            render_predicates(&mut result, predicates)?;
            Ok(result)
        }
        Expression::Path { root, start, steps } => {
            let mut result = root.to_string();
            if let Some(start) = start {
                result.push_str(&render(start)?);
            }
            for (separator, step) in steps {
                result.push_str(separator);
                result.push_str(&render_step(step)?);
            }
            Ok(result)
        }
        Expression::Negate(value) => Ok(format!("-({})", render(value)?)),
        Expression::Binary(left, operator, right) => {
            Ok(format!("({} {operator} {})", render(left)?, render(right)?))
        }
    }
}

fn render_step(step: &Step<'_>) -> Result<String> {
    if !matches!(
        step.axis,
        "ancestor"
            | "ancestor-or-self"
            | "attribute"
            | "child"
            | "descendant"
            | "descendant-or-self"
            | "following"
            | "following-sibling"
            | "namespace"
            | "parent"
            | "preceding"
            | "preceding-sibling"
            | "self"
    ) {
        return Err(invalid(step.axis));
    }
    let mut result = if step.axis == "namespace" {
        let predicate = match &step.test {
            NodeTest::Name("*") | NodeTest::Kind("node") => {
                format!("starts-with(fn:name(), '{NAMESPACE_ATTRIBUTE}')")
            }
            NodeTest::Name(name) if !name.contains(':') => {
                format!("fn:name() = '{NAMESPACE_ATTRIBUTE}{name}'")
            }
            _ => "false()".to_string(),
        };
        format!("attribute::*[{predicate}]")
    } else {
        let test = match &step.test {
            NodeTest::Name("*") => "*".to_string(),
            NodeTest::Name(name) if name.contains(':') => "*[false()]".to_string(),
            NodeTest::Name(name) => {
                format!("*[replace(fn:local-name(), '^[^:]*:', '') = '{name}']")
            }
            NodeTest::Kind(name) => format!("{name}()"),
            NodeTest::ProcessingInstruction(target) => {
                format!("processing-instruction({})", target.unwrap_or(""))
            }
        };
        let mut result = format!("{}::{test}", step.axis);
        if step.axis == "attribute" {
            result.push_str(&format!(
                "[not(starts-with(fn:name(), '{NAMESPACE_ATTRIBUTE}'))]"
            ));
        }
        result
    };
    render_predicates(&mut result, &step.predicates)?;
    Ok(result)
}

fn render_predicates(result: &mut String, predicates: &[Expression<'_>]) -> Result<()> {
    for predicate in predicates {
        result.push_str(&format!("[{}]", render(predicate)?));
    }
    Ok(())
}
fn function(name: &str, mut args: Vec<String>) -> Result<String> {
    if !matches!(
        name,
        "last"
            | "position"
            | "count"
            | "id"
            | "local-name"
            | "namespace-uri"
            | "name"
            | "string"
            | "concat"
            | "starts-with"
            | "contains"
            | "substring-before"
            | "substring-after"
            | "substring"
            | "string-length"
            | "normalize-space"
            | "translate"
            | "boolean"
            | "not"
            | "true"
            | "false"
            | "lang"
            | "number"
            | "sum"
            | "floor"
            | "ceiling"
            | "round"
            | "node"
            | "text"
            | "comment"
            | "processing-instruction"
    ) {
        return Err(invalid(name));
    }
    Ok(match name {
        "name" | "local-name" | "namespace-uri" | "string" | "number" | "string-length"
        | "normalize-space"
            if args.len() <= 1 =>
        {
            let arg = args.pop().unwrap_or_else(|| ".".into());
            let first = format!("({arg})[1]");
            match name {
                "name" => format!("replace(fn:name({first}), '^\\{{namespace\\}}', '')"),
                "local-name" => format!(
                    "replace(replace(fn:local-name({first}), '^\\{{namespace\\}}', ''), '^[^:]*:', '')"
                ),
                "number" => number(&first),
                _ => format!("fn:{name}({first})"),
            }
        }
        "sum" if args.len() == 1 => format!("fn:sum(({}) ! ({}))", args[0], number(".")),
        "concat" | "contains" | "starts-with" | "substring-before" | "substring-after"
        | "translate" => {
            let args = args
                .iter()
                .map(|arg| format!("fn:string(({arg})[1])"))
                .collect::<Vec<_>>()
                .join(", ");
            format!("fn:{name}({args})")
        }
        "floor" | "ceiling" | "round" if args.len() == 1 => {
            format!("fn:{name}({})", number(&format!("({})[1]", args[0])))
        }
        "substring" if matches!(args.len(), 2 | 3) => {
            let text = format!("fn:string(({})[1])", args[0]);
            let numbers = args[1..]
                .iter()
                .map(|arg| number(&format!("({arg})[1]")))
                .collect::<Vec<_>>()
                .join(", ");
            format!("fn:substring({text}, {numbers})")
        }
        _ => format!("{name}({})", args.join(", ")),
    })
}

/// XPath 1.0 numbers exclude exponent notation, plus signs, and Infinity.
fn number(arg: &str) -> String {
    format!(
        "(let $v := ({arg}) return if ($v instance of xs:boolean or $v instance of xs:numeric) then fn:number($v) else if (matches(normalize-space(fn:string($v)), '^-?([0-9]+(\\.[0-9]*)?|\\.[0-9]+)$')) then fn:number($v) else xs:double('NaN'))"
    )
}
