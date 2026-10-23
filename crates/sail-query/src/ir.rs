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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExpressionId(pub u64);

/// A materialized output slot. Unlike an expression occurrence, an attribute is
/// shared by all references to the same output slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AttributeId(pub u64);

/// A query-local allocator shared by every analysis layer and nested scope.
#[derive(Debug, Default)]
pub struct IdGenerator {
    next_expression: u64,
    next_attribute: u64,
}

impl IdGenerator {
    /// Reserve identities supplied by a previous pass before allocating new nodes.
    pub fn reserve_plan(&mut self, plan: &LogicalPlan) -> PlanResult<()> {
        use crate::rewrite::{QueryRewriter, walk_expression};
        struct Reserve<'a>(&'a mut IdGenerator);
        impl QueryRewriter for Reserve<'_> {
            type Error = datafusion_common::DataFusionError;
            fn rewrite_attribute(&mut self, attribute: Attribute) -> PlanResult<Attribute> {
                for id in [attribute.id, attribute.origin] {
                    self.0.next_attribute =
                        self.0
                            .next_attribute
                            .max(id.0.checked_add(1).ok_or_else(|| {
                                plan_datafusion_err!("attribute identity space exhausted")
                            })?);
                }
                Ok(attribute)
            }
            fn rewrite_expression(
                &mut self,
                expression: LogicalExpression,
            ) -> PlanResult<LogicalExpression> {
                self.0.next_expression =
                    self.0
                        .next_expression
                        .max(expression.id.0.checked_add(1).ok_or_else(|| {
                            plan_datafusion_err!("expression identity space exhausted")
                        })?);
                if let Deferred::Resolved(ExpressionNode::HigherOrderFunction {
                    arguments, ..
                }) = &expression.node
                {
                    for argument in arguments {
                        if let FunctionArgument::Lambda { id, .. } = argument {
                            self.0.next_expression =
                                self.0.next_expression.max(id.0.checked_add(1).ok_or_else(
                                    || plan_datafusion_err!("lambda identity space exhausted"),
                                )?);
                        }
                    }
                }
                walk_expression(self, expression)
            }
        }
        Reserve(self).rewrite_plan(plan.clone())?;
        Ok(())
    }

    pub fn expression(&mut self) -> ExpressionId {
        let id = ExpressionId(self.next_expression);
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
pub struct ExpressionType {
    pub data_type: DataType,
    pub nullable: bool,
    pub metadata: HashMap<String, String>,
}

impl ExpressionType {
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

impl Resolvable for ExpressionType {
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
    pub result_type: Deferred<ExpressionType>,
}

#[derive(Debug, Clone, Default)]
pub struct PlanSchema {
    pub attributes: Vec<Attribute>,
}

impl Resolvable for PlanSchema {
    type Request = Infer;
}

impl PlanSchema {
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
pub struct LogicalExpression {
    pub id: ExpressionId,
    pub node: Deferred<ExpressionNode>,
    pub result_type: Deferred<ExpressionType>,
}

impl LogicalExpression {
    pub fn unresolved(id: ExpressionId, expression: spec::Expr) -> Self {
        Self {
            id,
            node: Deferred::unresolved(expression),
            result_type: Deferred::unresolved(Infer),
        }
    }

    pub fn literal(id: ExpressionId, value: ScalarValue) -> Self {
        let result_type = ExpressionType::new(value.data_type(), value.is_null());
        Self {
            id,
            node: Deferred::Resolved(ExpressionNode::Literal(value)),
            result_type: Deferred::Resolved(result_type),
        }
    }
}

#[derive(Debug, Clone)]
pub enum ExpressionNode {
    Literal(ScalarValue),
    WithMetadata(Box<LogicalExpression>),
    TruthTest {
        expression: Box<LogicalExpression>,
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
        arguments: Vec<LogicalExpression>,
    },
    AggregateFunction {
        function: Arc<AggregateUDF>,
        arguments: Vec<LogicalExpression>,
        distinct: bool,
        filter: Option<Box<LogicalExpression>>,
        order_by: Vec<SortExpression>,
        null_treatment: Option<datafusion_expr::expr::NullTreatment>,
    },
    WindowFunction {
        function: WindowFunctionDefinition,
        arguments: Vec<LogicalExpression>,
        partition_by: Vec<LogicalExpression>,
        order_by: Vec<SortExpression>,
        frame: WindowFrame,
        filter: Option<Box<LogicalExpression>>,
        distinct: bool,
        null_treatment: Option<datafusion_expr::expr::NullTreatment>,
    },
    HigherOrderFunction {
        function: Arc<HigherOrderUDF>,
        arguments: Vec<FunctionArgument>,
    },
    LambdaVariable(AttributeId),
    Binary {
        left: Box<LogicalExpression>,
        operator: Operator,
        right: Box<LogicalExpression>,
    },
    Cast {
        expression: Box<LogicalExpression>,
        target: ExpressionType,
        safe: bool,
    },
    Not(Box<LogicalExpression>),
    IsNull(Box<LogicalExpression>),
    IsNotNull(Box<LogicalExpression>),
    Negative(Box<LogicalExpression>),
    Case {
        operand: Option<Box<LogicalExpression>>,
        branches: Vec<(LogicalExpression, LogicalExpression)>,
        otherwise: Option<Box<LogicalExpression>>,
    },
    Between {
        expression: Box<LogicalExpression>,
        low: Box<LogicalExpression>,
        high: Box<LogicalExpression>,
        negated: bool,
    },
    Like {
        expression: Box<LogicalExpression>,
        pattern: Box<LogicalExpression>,
        negated: bool,
        escape_char: Option<char>,
        case_insensitive: bool,
    },
    SimilarTo {
        expression: Box<LogicalExpression>,
        pattern: Box<LogicalExpression>,
        negated: bool,
        escape_char: Option<char>,
        case_insensitive: bool,
    },
    InList {
        expression: Box<LogicalExpression>,
        values: Vec<LogicalExpression>,
        negated: bool,
    },
    ScalarSubquery(Box<LogicalPlan>),
    Exists {
        query: Box<LogicalPlan>,
        negated: bool,
    },
    InSubquery {
        expression: Box<LogicalExpression>,
        query: Box<LogicalPlan>,
        negated: bool,
    },
}

impl Resolvable for ExpressionNode {
    type Request = spec::Expr;
}

/// A lambda has a parameter schema and a typed body; it is not represented as
/// a scalar expression with an artificial Arrow Null type.
#[derive(Debug, Clone)]
pub enum FunctionArgument {
    Value(Box<LogicalExpression>),
    Lambda {
        id: ExpressionId,
        parameters: PlanSchema,
        body: Box<LogicalExpression>,
    },
}

#[derive(Debug, Clone)]
pub struct SortExpression {
    pub expression: LogicalExpression,
    pub ascending: bool,
    pub nulls_first: bool,
}

#[derive(Debug, Clone)]
pub struct Projection {
    pub expression: LogicalExpression,
    pub attribute: Attribute,
}

#[derive(Debug, Clone)]
pub struct LogicalPlan {
    pub node: Deferred<PlanNode>,
    pub schema: Deferred<PlanSchema>,
}

impl LogicalPlan {
    pub fn unresolved(plan: spec::QueryPlan) -> Self {
        Self {
            node: Deferred::unresolved(plan),
            schema: Deferred::unresolved(Infer),
        }
    }
}

/// Relational operators contain native children and expressions. A scan stores
/// a source handle, not a pre-built backend plan or a resolver fallback.
#[derive(Clone, Educe)]
#[educe(Debug)]
pub enum PlanNode {
    Scope {
        input: Box<LogicalPlan>,
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
        input: Box<LogicalPlan>,
        column: AttributeId,
        outer: bool,
    },
    DistinctOn {
        input: Box<LogicalPlan>,
        keys: Vec<LogicalExpression>,
    },
    RowIdentity {
        input: Box<LogicalPlan>,
        attribute: Attribute,
        kind: RowIdentityKind,
    },
    MapPartitions {
        input: Box<LogicalPlan>,
        function: Arc<dyn crate::udf::StreamUDF>,
    },
    RemoteCheckpoint {
        relation_id: String,
    },
    Repartition {
        input: Box<LogicalPlan>,
        partitioning: Partitioning,
    },
    Values {
        rows: Vec<Vec<LogicalExpression>>,
    },
    Scan {
        name: String,
        #[educe(Debug(ignore))]
        source: Arc<dyn TableSource>,
        projection: Option<Vec<usize>>,
    },
    Project {
        input: Box<LogicalPlan>,
        expressions: Vec<Projection>,
    },
    Filter {
        input: Box<LogicalPlan>,
        condition: Box<LogicalExpression>,
    },
    Aggregate {
        input: Box<LogicalPlan>,
        grouping: Vec<Projection>,
        /// Indices into `grouping`; duplicate sets remain distinct groups.
        grouping_sets: Option<Vec<Vec<usize>>>,
        grouping_id: Option<Attribute>,
        aggregates: Vec<Projection>,
    },
    Window {
        input: Box<LogicalPlan>,
        expressions: Vec<Projection>,
    },
    Sort {
        input: Box<LogicalPlan>,
        order: Vec<SortExpression>,
        global: bool,
        /// Preserve an explicit upstream sort for order-sensitive consumers.
        required: bool,
    },
    Limit {
        input: Box<LogicalPlan>,
        skip: Option<Box<LogicalExpression>>,
        fetch: Option<Box<LogicalExpression>>,
    },
    Join {
        lateral: bool,
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        kind: JoinType,
        condition: Option<Box<LogicalExpression>>,
    },
    Union {
        inputs: Vec<LogicalPlan>,
    },
    Distinct {
        input: Box<LogicalPlan>,
    },
}

impl Resolvable for PlanNode {
    type Request = spec::QueryPlan;
}

#[derive(Debug, Clone)]
pub enum Partitioning {
    RoundRobin(usize),
    Coalesce(usize),
    Hash(Vec<LogicalExpression>, usize),
    Distribute(Vec<LogicalExpression>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RowIdentityKind {
    Monotonic,
    Partition,
}
