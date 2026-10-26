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
    fn rewrite_expression(&mut self, expression: Expr) -> PlanResult<Expr> {
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
    pub fn reserve_query(&mut self, plan: &Query) -> PlanResult<()> {
        Reserve(self).rewrite_query(plan.clone())?;
        Ok(())
    }

    /// Reserve identities in expressions stored outside a query tree.
    pub fn reserve_expression(&mut self, expression: &Expr) -> PlanResult<()> {
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
pub struct Expr {
    pub id: ExprId,
    pub kind: Deferred<ExprKind>,
    pub result_type: Deferred<ExprType>,
}

impl Expr {
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
    WithMetadata(Box<Expr>),
    TruthTest {
        expression: Box<Expr>,
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
        arguments: Vec<Expr>,
    },
    AggregateFunction {
        function: Arc<AggregateUDF>,
        arguments: Vec<Expr>,
        distinct: bool,
        filter: Option<Box<Expr>>,
        order_by: Vec<SortKey>,
        null_treatment: Option<datafusion_expr::expr::NullTreatment>,
    },
    WindowFunction {
        function: WindowFunctionDefinition,
        arguments: Vec<Expr>,
        partition_by: Vec<Expr>,
        order_by: Vec<SortKey>,
        frame: WindowFrame,
        filter: Option<Box<Expr>>,
        distinct: bool,
        null_treatment: Option<datafusion_expr::expr::NullTreatment>,
    },
    HigherOrderFunction {
        function: Arc<HigherOrderUDF>,
        arguments: Vec<FunctionArg>,
    },
    LambdaVariable(AttributeId),
    Binary {
        left: Box<Expr>,
        operator: Operator,
        right: Box<Expr>,
    },
    Cast {
        expression: Box<Expr>,
        target: ExprType,
        safe: bool,
    },
    Not(Box<Expr>),
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    Negative(Box<Expr>),
    Case {
        operand: Option<Box<Expr>>,
        branches: Vec<(Expr, Expr)>,
        otherwise: Option<Box<Expr>>,
    },
    Between {
        expression: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
    },
    Like {
        expression: Box<Expr>,
        pattern: Box<Expr>,
        negated: bool,
        escape_char: Option<char>,
        case_insensitive: bool,
    },
    SimilarTo {
        expression: Box<Expr>,
        pattern: Box<Expr>,
        negated: bool,
        escape_char: Option<char>,
        case_insensitive: bool,
    },
    InList {
        expression: Box<Expr>,
        values: Vec<Expr>,
        negated: bool,
    },
    ScalarSubquery(Box<Query>),
    Exists {
        query: Box<Query>,
        negated: bool,
    },
    InSubquery {
        expression: Box<Expr>,
        query: Box<Query>,
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
    Value(Box<Expr>),
    Lambda {
        id: ExprId,
        parameters: QuerySchema,
        body: Box<Expr>,
    },
}

#[derive(Debug, Clone)]
pub struct SortKey {
    pub expression: Expr,
    pub ascending: bool,
    pub nulls_first: bool,
}

#[derive(Debug, Clone)]
pub struct Projection {
    pub expression: Expr,
    pub attribute: Attribute,
}

#[derive(Debug, Clone)]
pub struct Query {
    pub kind: Deferred<QueryKind>,
    pub schema: Deferred<QuerySchema>,
}

impl Query {
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
        input: Box<Query>,
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
        input: Box<Query>,
        column: AttributeId,
        outer: bool,
    },
    DistinctOn {
        input: Box<Query>,
        keys: Vec<Expr>,
    },
    RowIdentity {
        input: Box<Query>,
        attribute: Attribute,
        kind: RowIdentityKind,
    },
    MapPartitions {
        input: Box<Query>,
        function: Arc<dyn crate::udf::StreamUDF>,
    },
    RemoteCheckpoint {
        relation_id: String,
    },
    Repartition {
        input: Box<Query>,
        partitioning: Partitioning,
    },
    Values {
        rows: Vec<Vec<Expr>>,
    },
    Scan {
        name: String,
        #[educe(Debug(ignore))]
        source: Arc<dyn TableSource>,
        projection: Option<Vec<usize>>,
    },
    Project {
        input: Box<Query>,
        expressions: Vec<Projection>,
    },
    Filter {
        input: Box<Query>,
        condition: Box<Expr>,
    },
    Aggregate {
        input: Box<Query>,
        grouping: Vec<Projection>,
        /// Indices into `grouping`; duplicate sets remain distinct groups.
        grouping_sets: Option<Vec<Vec<usize>>>,
        grouping_id: Option<Attribute>,
        aggregates: Vec<Projection>,
    },
    Window {
        input: Box<Query>,
        expressions: Vec<Projection>,
    },
    Sort {
        input: Box<Query>,
        order: Vec<SortKey>,
        global: bool,
        /// Preserve an explicit upstream sort for order-sensitive consumers.
        required: bool,
    },
    Limit {
        input: Box<Query>,
        skip: Option<Box<Expr>>,
        fetch: Option<Box<Expr>>,
    },
    Join {
        lateral: bool,
        left: Box<Query>,
        right: Box<Query>,
        kind: JoinType,
        condition: Option<Box<Expr>>,
    },
    Union {
        inputs: Vec<Query>,
    },
    Distinct {
        input: Box<Query>,
    },
}

impl Resolvable for QueryKind {
    type Request = spec::QueryPlan;
}

#[derive(Debug, Clone)]
pub enum Partitioning {
    RoundRobin(usize),
    Coalesce(usize),
    Hash(Vec<Expr>, usize),
    Distribute(Vec<Expr>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RowIdentityKind {
    Monotonic,
    Partition,
}
