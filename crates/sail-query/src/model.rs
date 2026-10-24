//! Native logical query representation. Backend plans and expressions may only
//! be constructed by lowering; they are never payloads in this tree.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef};
use datafusion_common::{Result as PlanResult, ScalarValue, plan_datafusion_err};
use datafusion_expr::{
    AggregateUDF, HigherOrderUDF, JoinType, Operator, ScalarUDF, TableSource, WindowFrame,
    WindowFunctionDefinition,
};
use educe::Educe;
use sail_common::spec;

use super::deferred::{Deferred, Resolvable};
use crate::rewrite::{QueryRewriter, walk_expression};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExprId(pub u64);

/// A materialized output slot. Unlike an expression occurrence, an attribute is
/// shared by all references to the same output slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AttributeId(pub u64);

/// A plan-local allocator shared by every analysis layer and nested scope.
#[derive(Debug, Default)]
pub struct IdGenerator {
    next_expression: u64,
    next_attribute: u64,
}

struct Reserve<'a>(&'a mut IdGenerator);
impl QueryRewriter for Reserve<'_> {
    type Error = datafusion_common::DataFusionError;
    fn rewrite_attribute(&mut self, attribute: Attribute) -> PlanResult<Attribute> {
        for id in [attribute.id, attribute.origin] {
            self.0.next_attribute = self.0.next_attribute.max(
                id.0.checked_add(1)
                    .ok_or_else(|| plan_datafusion_err!("attribute identity space exhausted"))?,
            );
        }
        Ok(attribute)
    }
    fn rewrite_expression(&mut self, expression: ExprNode) -> PlanResult<ExprNode> {
        self.0.next_expression = self.0.next_expression.max(
            expression
                .id
                .0
                .checked_add(1)
                .ok_or_else(|| plan_datafusion_err!("expression identity space exhausted"))?,
        );
        if let Deferred::Resolved(ExprKind::HigherOrderFunction { arguments, .. }) =
            &expression.kind
        {
            for argument in arguments {
                if let FunctionArg::Lambda { id, .. } = argument {
                    self.0.next_expression =
                        self.0
                            .next_expression
                            .max(id.0.checked_add(1).ok_or_else(|| {
                                plan_datafusion_err!("lambda identity space exhausted")
                            })?);
                }
            }
        }
        walk_expression(self, expression)
    }
}

impl IdGenerator {
    /// Reserve identities supplied by a previous pass before allocating new nodes.
    pub fn reserve_query(&mut self, plan: &QueryNode) -> PlanResult<()> {
        Reserve(self).rewrite_query(plan.clone())?;
        Ok(())
    }

    /// Reserve identities in expressions stored outside a query tree.
    pub fn reserve_expression(&mut self, expression: &ExprNode) -> PlanResult<()> {
        Reserve(self).rewrite_expression(expression.clone())?;
        Ok(())
    }

    /// Reserve binding attributes stored outside a query tree.
    pub fn reserve_schema(&mut self, schema: &QuerySchema) -> PlanResult<()> {
        Reserve(self).rewrite_schema(schema.clone())?;
        Ok(())
    }

    pub fn expression(&mut self) -> ExprId {
        let id = ExprId(self.next_expression);
        self.next_expression += 1;
        id
    }

    pub fn attribute(&mut self) -> AttributeId {
        let id = AttributeId(self.next_attribute);
        self.next_attribute += 1;
        id
    }
}

/// The semantic result of an expression. Display names do not participate in
/// typing. Data type, nullability, and extension metadata resolve atomically.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExprType {
    pub data_type: DataType,
    pub nullable: bool,
    pub metadata: HashMap<String, String>,
}

impl ExprType {
    pub fn new(data_type: DataType, nullable: bool) -> Self {
        Self {
            data_type,
            nullable,
            metadata: HashMap::new(),
        }
    }

    pub fn from_field(field: &Field) -> Self {
        Self {
            data_type: field.data_type().clone(),
            nullable: field.is_nullable(),
            metadata: field.metadata().clone(),
        }
    }

    pub fn to_field(&self, name: impl Into<String>) -> FieldRef {
        Arc::new(
            Field::new(name, self.data_type.clone(), self.nullable)
                .with_metadata(self.metadata.clone()),
        )
    }
}

impl Resolvable for ExprType {
    type Request = Infer;
}

#[derive(Debug, Clone, Copy)]
pub struct Infer;

#[derive(Debug, Clone)]
pub struct Attribute {
    pub id: AttributeId,
    /// Identity projections preserve this origin; distinct relation instances
    /// receive fresh origins even when their display names are identical.
    pub origin: AttributeId,
    pub name: String,
    pub qualifier: Vec<String>,
    pub qualified_position: usize,
    pub plan_ids: Vec<i64>,
    pub hidden: bool,
    pub result_type: Deferred<ExprType>,
}

#[derive(Debug, Clone, Default)]
pub struct QuerySchema {
    pub attributes: Vec<Attribute>,
}

impl Resolvable for QuerySchema {
    type Request = Infer;
}

impl QuerySchema {
    pub fn arrow_schema(&self) -> PlanResult<arrow_schema::Schema> {
        let fields = self
            .attributes
            .iter()
            .map(|attribute| {
                Ok(attribute
                    .result_type
                    .resolved("attribute field")?
                    .to_field(attribute.name.clone()))
            })
            .collect::<PlanResult<Vec<_>>>()?;
        Ok(arrow_schema::Schema::new(fields))
    }

    pub fn attribute(&self, id: AttributeId) -> PlanResult<&Attribute> {
        self.attributes
            .iter()
            .find(|attribute| attribute.id == id)
            .ok_or_else(|| plan_datafusion_err!("attribute {id:?} is not in scope"))
    }
}

#[derive(Debug, Clone)]
pub struct ExprNode {
    pub id: ExprId,
    pub kind: Deferred<ExprKind>,
    pub result_type: Deferred<ExprType>,
}

impl ExprNode {
    pub fn unresolved(id: ExprId, expression: spec::Expr) -> Self {
        Self {
            id,
            kind: Deferred::unresolved(expression),
            result_type: Deferred::unresolved(Infer),
        }
    }

    pub fn literal(id: ExprId, value: ScalarValue) -> Self {
        let result_type = ExprType::new(value.data_type(), value.is_null());
        Self {
            id,
            kind: Deferred::Resolved(ExprKind::Literal(value)),
            result_type: Deferred::Resolved(result_type),
        }
    }
}

#[derive(Debug, Clone)]
pub enum ExprKind {
    Literal(ScalarValue),
    WithMetadata(Box<ExprNode>),
    TruthTest {
        expression: Box<ExprNode>,
        value: Option<bool>,
        negated: bool,
    },
    Attribute(AttributeId),
    /// Number of enclosing query scopes to cross, starting at one.
    OuterAttribute {
        depth: usize,
        attribute: AttributeId,
    },
    ScalarFunction {
        function: Arc<ScalarUDF>,
        arguments: Vec<ExprNode>,
    },
    AggregateFunction {
        function: Arc<AggregateUDF>,
        arguments: Vec<ExprNode>,
        distinct: bool,
        filter: Option<Box<ExprNode>>,
        order_by: Vec<SortKey>,
        null_treatment: Option<datafusion_expr::expr::NullTreatment>,
    },
    WindowFunction {
        function: WindowFunctionDefinition,
        arguments: Vec<ExprNode>,
        partition_by: Vec<ExprNode>,
        order_by: Vec<SortKey>,
        frame: WindowFrame,
        filter: Option<Box<ExprNode>>,
        distinct: bool,
        null_treatment: Option<datafusion_expr::expr::NullTreatment>,
    },
    HigherOrderFunction {
        function: Arc<HigherOrderUDF>,
        arguments: Vec<FunctionArg>,
    },
    LambdaVariable(AttributeId),
    Binary {
        left: Box<ExprNode>,
        operator: Operator,
        right: Box<ExprNode>,
    },
    Cast {
        expression: Box<ExprNode>,
        target: ExprType,
        safe: bool,
    },
    Not(Box<ExprNode>),
    IsNull(Box<ExprNode>),
    IsNotNull(Box<ExprNode>),
    Negative(Box<ExprNode>),
    Case {
        operand: Option<Box<ExprNode>>,
        branches: Vec<(ExprNode, ExprNode)>,
        otherwise: Option<Box<ExprNode>>,
    },
    Between {
        expression: Box<ExprNode>,
        low: Box<ExprNode>,
        high: Box<ExprNode>,
        negated: bool,
    },
    Like {
        expression: Box<ExprNode>,
        pattern: Box<ExprNode>,
        negated: bool,
        escape_char: Option<char>,
        case_insensitive: bool,
    },
    SimilarTo {
        expression: Box<ExprNode>,
        pattern: Box<ExprNode>,
        negated: bool,
        escape_char: Option<char>,
        case_insensitive: bool,
    },
    InList {
        expression: Box<ExprNode>,
        values: Vec<ExprNode>,
        negated: bool,
    },
    ScalarSubquery(Box<QueryNode>),
    Exists {
        query: Box<QueryNode>,
        negated: bool,
    },
    InSubquery {
        expression: Box<ExprNode>,
        query: Box<QueryNode>,
        negated: bool,
    },
}

impl Resolvable for ExprKind {
    type Request = spec::Expr;
}

/// A lambda has a parameter schema and a typed body; it is not represented as
/// a scalar expression with an artificial Arrow Null type.
#[derive(Debug, Clone)]
pub enum FunctionArg {
    Value(Box<ExprNode>),
    Lambda {
        id: ExprId,
        parameters: QuerySchema,
        body: Box<ExprNode>,
    },
}

#[derive(Debug, Clone)]
pub struct SortKey {
    pub expression: ExprNode,
    pub ascending: bool,
    pub nulls_first: bool,
}

#[derive(Debug, Clone)]
pub struct Projection {
    pub expression: ExprNode,
    pub attribute: Attribute,
}

#[derive(Debug, Clone)]
pub struct QueryNode {
    pub kind: Deferred<QueryKind>,
    pub schema: Deferred<QuerySchema>,
}

impl QueryNode {
    pub fn unresolved(plan: spec::QueryPlan) -> Self {
        Self {
            kind: Deferred::unresolved(plan),
            schema: Deferred::unresolved(Infer),
        }
    }
}

/// Relational operators contain native children and expressions. A scan stores
/// a source handle, not a pre-built backend plan or a resolver fallback.
#[derive(Clone, Educe)]
#[educe(Debug)]
pub enum QueryKind {
    Scope {
        input: Box<QueryNode>,
    },
    Empty {
        produce_one_row: bool,
    },
    Range {
        start: i64,
        end: i64,
        step: i64,
        partitions: usize,
    },
    Unnest {
        input: Box<QueryNode>,
        column: AttributeId,
        outer: bool,
    },
    DistinctOn {
        input: Box<QueryNode>,
        keys: Vec<ExprNode>,
    },
    RowIdentity {
        input: Box<QueryNode>,
        attribute: Attribute,
        kind: RowIdentityKind,
    },
    MapPartitions {
        input: Box<QueryNode>,
        function: Arc<dyn crate::udf::StreamUDF>,
    },
    RemoteCheckpoint {
        relation_id: String,
    },
    Repartition {
        input: Box<QueryNode>,
        partitioning: Partitioning,
    },
    Values {
        rows: Vec<Vec<ExprNode>>,
    },
    Scan {
        name: String,
        #[educe(Debug(ignore))]
        source: Arc<dyn TableSource>,
        projection: Option<Vec<usize>>,
    },
    Project {
        input: Box<QueryNode>,
        expressions: Vec<Projection>,
    },
    Filter {
        input: Box<QueryNode>,
        condition: Box<ExprNode>,
    },
    Aggregate {
        input: Box<QueryNode>,
        grouping: Vec<Projection>,
        /// Indices into `grouping`; duplicate sets remain distinct groups.
        grouping_sets: Option<Vec<Vec<usize>>>,
        grouping_id: Option<Attribute>,
        aggregates: Vec<Projection>,
    },
    Window {
        input: Box<QueryNode>,
        expressions: Vec<Projection>,
    },
    Sort {
        input: Box<QueryNode>,
        order: Vec<SortKey>,
        global: bool,
        /// Preserve an explicit upstream sort for order-sensitive consumers.
        required: bool,
    },
    Limit {
        input: Box<QueryNode>,
        skip: Option<Box<ExprNode>>,
        fetch: Option<Box<ExprNode>>,
    },
    Join {
        lateral: bool,
        left: Box<QueryNode>,
        right: Box<QueryNode>,
        kind: JoinType,
        condition: Option<Box<ExprNode>>,
    },
    Union {
        inputs: Vec<QueryNode>,
    },
    Distinct {
        input: Box<QueryNode>,
    },
}

impl Resolvable for QueryKind {
    type Request = spec::QueryPlan;
}

#[derive(Debug, Clone)]
pub enum Partitioning {
    RoundRobin(usize),
    Coalesce(usize),
    Hash(Vec<ExprNode>, usize),
    Distribute(Vec<ExprNode>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RowIdentityKind {
    Monotonic,
    Partition,
}
