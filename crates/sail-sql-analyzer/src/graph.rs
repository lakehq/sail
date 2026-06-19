use sail_common::spec;
use sail_sql_parser::ast::graph::{
    GraphEdgePattern, GraphIncomingPattern, GraphLimitClause, GraphNodePattern,
    GraphOutgoingPattern, GraphPathPattern, GraphPatternStep, GraphPropertyEntry, GraphPropertyMap,
    GraphQuery, GraphReturnClause, GraphUndirectedPattern, GraphWhereClause,
};
use sail_sql_parser::ast::query::LimitValue;

use crate::error::SqlResult;
use crate::expression::{from_ast_expression, from_ast_order_by};

pub(crate) fn from_ast_graph_query(query: GraphQuery) -> SqlResult<spec::QueryPlan> {
    let GraphQuery {
        r#match: _,
        patterns,
        r#where,
        r#return,
        order_by,
        skip,
        limit,
    } = query;

    let predicates = r#where
        .map(
            |GraphWhereClause {
                 r#where: _,
                 condition,
             }| from_ast_expression(condition),
        )
        .transpose()?
        .into_iter()
        .collect();
    let returns = from_ast_graph_return_clause(r#return)?;
    let order = order_by
        .map(|clause| {
            clause
                .items
                .into_items()
                .map(from_ast_order_by)
                .collect::<SqlResult<Vec<_>>>()
        })
        .transpose()?
        .unwrap_or_default();
    let skip = skip
        .map(|clause| from_ast_expression(*clause.value))
        .transpose()?;
    let limit = limit
        .map(from_ast_graph_limit_clause)
        .transpose()?
        .flatten();

    Ok(spec::QueryPlan::new(spec::QueryNode::Graph(
        spec::GraphQuery {
            patterns: patterns
                .into_items()
                .map(from_ast_graph_path_pattern)
                .collect::<SqlResult<_>>()?,
            predicates,
            returns,
            order,
            skip,
            limit,
        },
    )))
}

fn from_ast_graph_return_clause(clause: GraphReturnClause) -> SqlResult<Vec<spec::Expr>> {
    let GraphReturnClause { r#return: _, items } = clause;
    items.into_items().map(from_ast_expression).collect()
}

fn from_ast_graph_limit_clause(clause: GraphLimitClause) -> SqlResult<Option<spec::Expr>> {
    let GraphLimitClause { limit: _, value } = clause;
    match value {
        LimitValue::All(_) => Ok(None),
        LimitValue::Value(value) => from_ast_expression(*value).map(Some),
    }
}

fn from_ast_graph_path_pattern(pattern: GraphPathPattern) -> SqlResult<spec::GraphPathPattern> {
    let GraphPathPattern { start, steps } = pattern;
    Ok(spec::GraphPathPattern {
        start: from_ast_graph_node_pattern(start)?,
        steps: steps
            .into_iter()
            .map(from_ast_graph_pattern_step)
            .collect::<SqlResult<_>>()?,
    })
}

fn from_ast_graph_pattern_step(step: GraphPatternStep) -> SqlResult<spec::GraphPatternStep> {
    match step {
        GraphPatternStep::Outgoing(GraphOutgoingPattern {
            left: _,
            edge,
            right: _,
            target,
        }) => Ok(spec::GraphPatternStep {
            direction: spec::GraphDirection::Outgoing,
            edge: from_optional_ast_graph_edge_pattern(edge)?,
            target: from_ast_graph_node_pattern(target)?,
        }),
        GraphPatternStep::Incoming(GraphIncomingPattern {
            left: _,
            edge,
            right: _,
            target,
        }) => Ok(spec::GraphPatternStep {
            direction: spec::GraphDirection::Incoming,
            edge: from_optional_ast_graph_edge_pattern(edge)?,
            target: from_ast_graph_node_pattern(target)?,
        }),
        GraphPatternStep::Undirected(GraphUndirectedPattern {
            left: _,
            edge,
            right: _,
            target,
        }) => Ok(spec::GraphPatternStep {
            direction: spec::GraphDirection::Undirected,
            edge: from_optional_ast_graph_edge_pattern(edge)?,
            target: from_ast_graph_node_pattern(target)?,
        }),
    }
}

fn from_ast_graph_node_pattern(pattern: GraphNodePattern) -> SqlResult<spec::GraphNodePattern> {
    let GraphNodePattern {
        left: _,
        variable,
        label,
        properties,
        right: _,
    } = pattern;
    Ok(spec::GraphNodePattern {
        variable: variable.map(|x| x.value.into()),
        label: label.map(|(_, x)| x.value.into()),
        properties: from_optional_ast_graph_property_map(properties)?,
    })
}

fn from_ast_graph_edge_pattern(pattern: GraphEdgePattern) -> SqlResult<spec::GraphEdgePattern> {
    let GraphEdgePattern {
        left: _,
        variable,
        label,
        properties,
        right: _,
    } = pattern;
    Ok(spec::GraphEdgePattern {
        variable: variable.map(|x| x.value.into()),
        label: label.map(|(_, x)| x.value.into()),
        properties: from_optional_ast_graph_property_map(properties)?,
    })
}

fn from_optional_ast_graph_edge_pattern(
    pattern: Option<GraphEdgePattern>,
) -> SqlResult<spec::GraphEdgePattern> {
    pattern
        .map(from_ast_graph_edge_pattern)
        .unwrap_or_else(|| Ok(empty_graph_edge_pattern()))
}

fn empty_graph_edge_pattern() -> spec::GraphEdgePattern {
    spec::GraphEdgePattern {
        variable: None,
        label: None,
        properties: Vec::new(),
    }
}

fn from_optional_ast_graph_property_map(
    properties: Option<GraphPropertyMap>,
) -> SqlResult<Vec<spec::GraphProperty>> {
    let Some(GraphPropertyMap {
        left: _,
        entries,
        right: _,
    }) = properties
    else {
        return Ok(Vec::new());
    };
    entries
        .map(|entries| {
            entries
                .into_items()
                .map(from_ast_graph_property_entry)
                .collect()
        })
        .unwrap_or_else(|| Ok(Vec::new()))
}

fn from_ast_graph_property_entry(entry: GraphPropertyEntry) -> SqlResult<spec::GraphProperty> {
    let GraphPropertyEntry {
        key,
        colon: _,
        value,
    } = entry;
    Ok(spec::GraphProperty {
        key: key.value.into(),
        value: from_ast_expression(*value)?,
    })
}
