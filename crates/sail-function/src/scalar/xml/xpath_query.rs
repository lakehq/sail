// SPDX-License-Identifier: Apache-2.0

use std::collections::HashSet;

use datafusion_common::{DataFusionError, Result};
use xee_xpath::{Documents, Query, Sequence, context};
use xee_xpath_ast::{FN_NAMESPACE, WithSpan, XPathParserContext, ast};
use xee_xpath_compiler::interpreter::Program;
use xee_xpath_lexer::Token;
use xot::xmlname::NameStrInfo;

use super::reader::NAMESPACE_ATTRIBUTE;

/// Use the engine XPath grammar and semantics, adapting only the lexical names
/// and namespace nodes of the namespace-unaware DOM used by Spark.
pub(super) fn compile(path: &str) -> Result<DomQuery> {
    let mut parser = XPathParserContext::default();
    let mut unbound_prefixes = Vec::new();
    let mut explicit_uris = HashSet::new();
    for (token, _) in xee_xpath_lexer::lexer(path) {
        let prefix = match token {
            Token::PrefixedQName(name) => name.prefix,
            Token::LocalNameWildcard(name) => name.prefix,
            Token::URIQualifiedName(name) => {
                explicit_uris.insert(name.uri);
                continue;
            }
            Token::BracedURILiteralWildcard(name) => {
                explicit_uris.insert(name.uri);
                continue;
            }
            _ => continue,
        };
        if parser.namespaces.by_prefix(prefix).is_none() {
            unbound_prefixes.push(prefix);
        }
    }

    // Bind each unknown prefix to a distinct namespace so expanded names stay
    // distinct. These namespaces also make the corresponding node tests fail.
    let mut next_id = 0;
    for prefix in unbound_prefixes {
        if parser.namespaces.by_prefix(prefix).is_some() {
            continue;
        }
        loop {
            let uri = format!("urn:sail:unbound-xpath-prefix:{next_id}");
            next_id += 1;
            if !explicit_uris.contains(uri.as_str()) {
                parser.namespaces.add(&[(prefix, &uri)]);
                break;
            }
        }
    }

    let result = (|| {
        let mut xpath = parser.parse_xpath(path).map_err(|e| format!("{e:?}"))?;
        adapt_expr(&mut xpath.0.value)?;
        xee_xpath_compiler::compile(context::StaticContext::default(), xpath)
            .map_err(|e| format!("{e:?}"))
    })();
    result
        .map(DomQuery)
        .map_err(|error| DataFusionError::Execution(format!("Invalid XPath '{path}': {error}")))
}

pub(super) struct DomQuery(Program);

impl Query<Sequence> for DomQuery {
    fn program(&self) -> &Program {
        &self.0
    }

    fn execute_with_context(
        &self,
        documents: &mut Documents,
        context: &context::DynamicContext,
    ) -> xee_xpath::error::Result<Sequence> {
        self.0.runnable(context).many(documents.xot_mut())
    }
}

type AdaptResult = std::result::Result<(), String>;

fn adapt_expr(expr: &mut ast::Expr) -> AdaptResult {
    for single in &mut expr.0 {
        adapt_single(&mut single.value)?;
    }
    Ok(())
}

fn adapt_optional(expr: &mut ast::ExprOrEmptyS) -> AdaptResult {
    if let Some(expr) = &mut expr.value {
        adapt_expr(expr)?;
    }
    Ok(())
}

fn adapt_single(expr: &mut ast::ExprSingle) -> AdaptResult {
    match expr {
        ast::ExprSingle::Path(path) => adapt_path(path),
        ast::ExprSingle::Apply(apply) => {
            adapt_path(&mut apply.path_expr)?;
            if let ast::ApplyOperator::SimpleMap(paths) = &mut apply.operator {
                for path in paths {
                    adapt_path(path)?;
                }
            }
            Ok(())
        }
        ast::ExprSingle::Binary(binary) => {
            adapt_path(&mut binary.left)?;
            adapt_path(&mut binary.right)
        }
        ast::ExprSingle::Let(binding) => {
            adapt_single(&mut binding.var_expr.value)?;
            adapt_single(&mut binding.return_expr.value)
        }
        ast::ExprSingle::For(binding) => {
            adapt_single(&mut binding.var_expr.value)?;
            adapt_single(&mut binding.return_expr.value)
        }
        ast::ExprSingle::Quantified(binding) => {
            adapt_single(&mut binding.var_expr.value)?;
            adapt_single(&mut binding.satisfies_expr.value)
        }
        ast::ExprSingle::If(condition) => {
            adapt_expr(&mut condition.condition.value)?;
            adapt_single(&mut condition.then.value)?;
            adapt_single(&mut condition.else_.value)
        }
    }
}

fn adapt_path(path: &mut ast::PathExpr) -> AdaptResult {
    for step in &mut path.steps {
        match &mut step.value {
            ast::StepExpr::PrimaryExpr(primary) => adapt_primary(&mut primary.value)?,
            ast::StepExpr::PostfixExpr { primary, postfixes } => {
                adapt_primary(&mut primary.value)?;
                for postfix in postfixes {
                    match postfix {
                        ast::Postfix::Predicate(expr) => adapt_expr(&mut expr.value)?,
                        ast::Postfix::ArgumentList(args) => {
                            for arg in args {
                                adapt_single(&mut arg.value)?;
                            }
                        }
                        ast::Postfix::Lookup(key) => adapt_key(key)?,
                    }
                }
            }
            ast::StepExpr::AxisStep(axis) => adapt_axis(axis)?,
        }
    }
    Ok(())
}

fn adapt_key(key: &mut ast::KeySpecifier) -> AdaptResult {
    if let ast::KeySpecifier::Expr(expr) = key {
        adapt_optional(expr)?;
    }
    Ok(())
}

fn adapt_primary(primary: &mut ast::PrimaryExpr) -> AdaptResult {
    match primary {
        ast::PrimaryExpr::Expr(expr) => adapt_optional(expr)?,
        ast::PrimaryExpr::FunctionCall(call) => {
            for arg in &mut call.arguments {
                adapt_single(&mut arg.value)?;
            }
            if dom_name_function(&call.name.value) {
                let local = call.name.value.local_name().to_string();
                *primary = replace(primary.clone(), "^\\{namespace\\}", "");
                if local == "local-name" {
                    *primary = replace(primary.clone(), "^[^:]*:", "");
                }
            }
        }
        ast::PrimaryExpr::NamedFunctionRef(reference)
            if dom_name_function(&reference.name.value) && reference.arity <= 1 =>
        {
            // Preserve the same DOM name behavior when the function is passed
            // as a value, including through maps and higher-order functions.
            let (param, arg) = if reference.arity == 0 {
                ("", "")
            } else {
                ("$node", "$node")
            };
            let source = format!(
                "function({param}) {{ fn:{}({arg}) }}",
                reference.name.value.local_name()
            );
            let parsed = XPathParserContext::default()
                .parse_xpath(&source)
                .map_err(|e| format!("{e:?}"))?;
            let mut expr = parsed.0;
            if reference.arity == 0 {
                let [single] = expr.value.0.as_mut_slice() else {
                    return Err("Expected a single XPath wrapper function".into());
                };
                if let ast::ExprSingle::Path(path) = &mut single.value
                    && let [step] = path.steps.as_mut_slice()
                    && let ast::StepExpr::PrimaryExpr(primary) = &mut step.value
                    && let ast::PrimaryExpr::InlineFunction(function) = &mut primary.value
                {
                    // Zero-argument name functions capture the focus at the reference.
                    function.wrapper = true;
                } else {
                    return Err("Expected an inline XPath wrapper function".into());
                }
            }
            adapt_expr(&mut expr.value)?;
            *primary = ast::PrimaryExpr::Expr(Some(expr.value).with_span(expr.span));
        }
        ast::PrimaryExpr::InlineFunction(function) => adapt_optional(&mut function.body)?,
        ast::PrimaryExpr::MapConstructor(map) => {
            for entry in &mut map.entries {
                adapt_single(&mut entry.key.value)?;
                adapt_single(&mut entry.value.value)?;
            }
        }
        ast::PrimaryExpr::ArrayConstructor(array) => match array {
            ast::ArrayConstructor::Square(expr) => adapt_expr(&mut expr.value)?,
            ast::ArrayConstructor::Curly(expr) => adapt_optional(expr)?,
        },
        ast::PrimaryExpr::UnaryLookup(key) => adapt_key(key)?,
        ast::PrimaryExpr::Literal(_)
        | ast::PrimaryExpr::VarRef(_)
        | ast::PrimaryExpr::ContextItem
        | ast::PrimaryExpr::NamedFunctionRef(_) => {}
    }
    Ok(())
}

fn dom_name_function(name: &ast::Name) -> bool {
    (name.namespace().is_empty() || name.namespace() == FN_NAMESPACE)
        && matches!(name.local_name(), "name" | "local-name")
}

fn replace(value: ast::PrimaryExpr, pattern: &str, replacement: &str) -> ast::PrimaryExpr {
    let argument = |value: ast::PrimaryExpr| {
        ast::ExprSingle::Path(ast::PathExpr {
            steps: vec![ast::StepExpr::PrimaryExpr(value.with_empty_span()).with_empty_span()],
        })
        .with_empty_span()
    };
    ast::PrimaryExpr::FunctionCall(ast::FunctionCall {
        name: ast::Name::new("replace".into(), FN_NAMESPACE.into(), "fn".into()).with_empty_span(),
        arguments: vec![
            argument(value),
            argument(ast::PrimaryExpr::Literal(ast::Literal::String(
                pattern.into(),
            ))),
            argument(ast::PrimaryExpr::Literal(ast::Literal::String(
                replacement.into(),
            ))),
        ],
    })
}

fn adapt_axis(axis: &mut ast::AxisStep) -> AdaptResult {
    for predicate in &mut axis.predicates {
        adapt_expr(&mut predicate.value)?;
    }
    let mut predicates = Vec::new();
    if axis.axis == ast::Axis::Namespace {
        let predicate = match &axis.node_test {
            ast::NodeTest::NameTest(ast::NameTest::Star)
            | ast::NodeTest::KindTest(ast::KindTest::Any) => {
                format!("starts-with(fn:name(), '{NAMESPACE_ATTRIBUTE}')")
            }
            ast::NodeTest::NameTest(ast::NameTest::Name(name))
                if name.value.namespace().is_empty() =>
            {
                format!(
                    "fn:name() = '{NAMESPACE_ATTRIBUTE}{}'",
                    name.value.local_name()
                )
            }
            _ => "false()".into(),
        };
        predicates.push(predicate);
        axis.axis = ast::Axis::Attribute;
        axis.node_test = ast::NodeTest::NameTest(ast::NameTest::Star);
    } else {
        if let ast::NodeTest::NameTest(test) = &axis.node_test {
            let predicate = match test {
                ast::NameTest::Name(name) if name.value.namespace().is_empty() => Some(format!(
                    "replace(fn:local-name(), '^[^:]*:', '') = '{}'",
                    name.value.local_name()
                )),
                ast::NameTest::LocalName(name) => Some(format!(
                    "replace(fn:local-name(), '^[^:]*:', '') = '{name}'"
                )),
                ast::NameTest::Name(_) | ast::NameTest::Namespace(_) => Some("false()".into()),
                ast::NameTest::Star => None,
            };
            if let Some(predicate) = predicate {
                predicates.push(predicate);
                axis.node_test = ast::NodeTest::NameTest(ast::NameTest::Star);
            }
        }
        if axis.axis == ast::Axis::Attribute {
            predicates.push(format!(
                "not(starts-with(fn:name(), '{NAMESPACE_ATTRIBUTE}'))"
            ));
        }
    }
    // Name filtering precedes user predicates, preserving axis position and
    // reverse-axis ordering. Generated predicates use the raw arena names.
    let mut adapted = Vec::new();
    for predicate in predicates {
        let parsed = XPathParserContext::default()
            .parse_xpath(&predicate)
            .map_err(|e| format!("{e:?}"))?;
        adapted.push(parsed.0);
    }
    adapted.append(&mut axis.predicates);
    axis.predicates = adapted;
    Ok(())
}
