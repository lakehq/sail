use std::cmp::Ordering;

use datafusion::functions::core::getfield::GetFieldFunc;
use datafusion::functions::math::nans::IsNanFunc;
use datafusion::functions::string::starts_with::StartsWithFunc;
use datafusion_common::ScalarValue;
use datafusion_expr::{BinaryExpr, Expr, Operator};
use serde::{Deserialize, Serialize};

use crate::spec::manifest_list::FieldSummary;
use crate::spec::{
    DataFile, Literal, PartitionSpec, PrimitiveLiteral, PrimitiveType, Schema, Transform, Type,
};
use crate::utils::conversions::scalar_to_primitive_literal;
use crate::utils::transform::apply_transform;

/// Possible SQL outcomes for rows in a file or manifest. Missing evidence widens
/// this set; only absence of TRUE permits pruning.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Truth(u8);

impl Truth {
    const TRUE: Self = Self(1);
    const FALSE: Self = Self(2);
    const NULL: Self = Self(4);
    const UNKNOWN: Self = Self(7);

    pub(crate) fn may_match(self) -> bool {
        self.0 & 1 != 0
    }
    pub(crate) fn all_match(self) -> bool {
        self == Self::TRUE
    }

    fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
    fn intersect(self, other: Self) -> Self {
        Self(self.0 & other.0)
    }
    fn not(self) -> Self {
        Self(((self.0 & 1) << 1) | ((self.0 & 2) >> 1) | (self.0 & 4))
    }
    fn boolean(value: bool) -> Self {
        if value { Self::TRUE } else { Self::FALSE }
    }

    fn combine(self, other: Self, and: bool) -> Self {
        let mut result = Self(0);
        for a in [Self::TRUE, Self::FALSE, Self::NULL] {
            for b in [Self::TRUE, Self::FALSE, Self::NULL] {
                if self.0 & a.0 == 0 || other.0 & b.0 == 0 {
                    continue;
                }
                let value = if and {
                    if a == Self::FALSE || b == Self::FALSE {
                        Self::FALSE
                    } else if a == Self::NULL || b == Self::NULL {
                        Self::NULL
                    } else {
                        Self::TRUE
                    }
                } else if a == Self::TRUE || b == Self::TRUE {
                    Self::TRUE
                } else if a == Self::NULL || b == Self::NULL {
                    Self::NULL
                } else {
                    Self::FALSE
                };
                result = result.union(value);
            }
        }
        result
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum Predicate {
    Unknown,
    Constant(Option<bool>),
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
    Not(Box<Self>),
    IsTrue(Box<Self>),
    IsNull(Box<Self>),
    Leaf {
        id: i32,
        primitive: PrimitiveType,
        operation: Operation,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum Operation {
    Eq(#[serde(with = "literal_serde")] PrimitiveLiteral),
    Lt(#[serde(with = "literal_serde")] PrimitiveLiteral),
    LtEq(#[serde(with = "literal_serde")] PrimitiveLiteral),
    Gt(#[serde(with = "literal_serde")] PrimitiveLiteral),
    GtEq(#[serde(with = "literal_serde")] PrimitiveLiteral),
    IsNull,
    IsNan,
    StartsWith(String),
}

impl Predicate {
    pub(crate) fn conjunction(schema: &Schema, filters: &[Expr]) -> Self {
        filters
            .iter()
            .map(|expr| Self::new(schema, expr))
            .fold(Self::Constant(Some(true)), |left, right| {
                Self::And(Box::new(left), Box::new(right))
            })
    }

    pub(crate) fn new(schema: &Schema, expr: &Expr) -> Self {
        match expr {
            Expr::Alias(alias) => Self::new(schema, &alias.expr),
            Expr::Literal(ScalarValue::Boolean(value), _) => Self::Constant(*value),
            Expr::Literal(value, _) if value.is_null() => Self::Constant(None),
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::And,
                right,
            }) => Self::And(
                Box::new(Self::new(schema, left)),
                Box::new(Self::new(schema, right)),
            ),
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::Or,
                right,
            }) => Self::Or(
                Box::new(Self::new(schema, left)),
                Box::new(Self::new(schema, right)),
            ),
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                let (field, scalar, op) = if let (Some(field), Expr::Literal(value, _)) =
                    (source_field(schema, left), right.as_ref())
                {
                    (field, value, *op)
                } else if let (Expr::Literal(value, _), Some(field), Some(op)) =
                    (left.as_ref(), source_field(schema, right), op.swap())
                {
                    (field, value, op)
                } else {
                    return Self::Unknown;
                };
                let Type::Primitive(primitive) = field.field_type.as_ref() else {
                    return Self::Unknown;
                };
                if scalar.is_null() {
                    return match op {
                        Operator::IsNotDistinctFrom => {
                            Self::leaf(field.id, primitive, Operation::IsNull)
                        }
                        Operator::IsDistinctFrom => {
                            Self::leaf(field.id, primitive, Operation::IsNull).negate()
                        }
                        Operator::Eq
                        | Operator::NotEq
                        | Operator::Lt
                        | Operator::LtEq
                        | Operator::Gt
                        | Operator::GtEq => Self::Constant(None),
                        _ => Self::Unknown,
                    };
                }
                let Ok(value) = scalar_to_primitive_literal(scalar, &field.field_type) else {
                    return Self::Unknown;
                };
                let operation = match op {
                    Operator::Eq
                    | Operator::NotEq
                    | Operator::IsDistinctFrom
                    | Operator::IsNotDistinctFrom => Operation::Eq(value),
                    Operator::Lt => Operation::Lt(value),
                    Operator::LtEq => Operation::LtEq(value),
                    Operator::Gt => Operation::Gt(value),
                    Operator::GtEq => Operation::GtEq(value),
                    _ => return Self::Unknown,
                };
                let mut predicate = Self::leaf(field.id, primitive, operation);
                if matches!(op, Operator::IsDistinctFrom | Operator::IsNotDistinctFrom) {
                    predicate = Self::IsTrue(Box::new(predicate));
                }
                if matches!(op, Operator::NotEq | Operator::IsDistinctFrom) {
                    predicate = predicate.negate();
                }
                predicate
            }
            Expr::Not(value) => Self::new(schema, value).negate(),
            Expr::IsTrue(value) => Self::IsTrue(Box::new(Self::new(schema, value))),
            Expr::IsNotTrue(value) => Self::IsTrue(Box::new(Self::new(schema, value))).negate(),
            Expr::IsFalse(value) => Self::IsTrue(Box::new(Self::new(schema, value).negate())),
            Expr::IsNotFalse(value) => {
                Self::IsTrue(Box::new(Self::new(schema, value).negate())).negate()
            }
            Expr::IsNull(value) | Expr::IsNotNull(value) => {
                let predicate = if let Some(field) = source_field(schema, value) {
                    let Type::Primitive(primitive) = field.field_type.as_ref() else {
                        return Self::Unknown;
                    };
                    Self::leaf(field.id, primitive, Operation::IsNull)
                } else {
                    Self::IsNull(Box::new(Self::new(schema, value)))
                };
                if matches!(expr, Expr::IsNotNull(_)) {
                    predicate.negate()
                } else {
                    predicate
                }
            }
            Expr::InList(list) => {
                let predicate = list
                    .list
                    .iter()
                    .map(|value| Self::new(schema, &list.expr.as_ref().clone().eq(value.clone())))
                    .fold(Self::Constant(Some(false)), |left, right| {
                        Self::Or(Box::new(left), Box::new(right))
                    });
                if list.negated {
                    predicate.negate()
                } else {
                    predicate
                }
            }
            Expr::Between(between) => {
                let predicate = Self::new(
                    schema,
                    &between
                        .expr
                        .as_ref()
                        .clone()
                        .gt_eq(between.low.as_ref().clone())
                        .and(
                            between
                                .expr
                                .as_ref()
                                .clone()
                                .lt_eq(between.high.as_ref().clone()),
                        ),
                );
                if between.negated {
                    predicate.negate()
                } else {
                    predicate
                }
            }
            Expr::ScalarFunction(function) => {
                let Some(field) = function
                    .args
                    .first()
                    .and_then(|arg| source_field(schema, arg))
                else {
                    return Self::Unknown;
                };
                let Type::Primitive(primitive) = field.field_type.as_ref() else {
                    return Self::Unknown;
                };
                match function.args.as_slice() {
                    [_] if function.func.inner().is::<IsNanFunc>() && floating(primitive) => {
                        Self::leaf(field.id, primitive, Operation::IsNan)
                    }
                    [_, Expr::Literal(value, _)]
                        if function.func.inner().is::<StartsWithFunc>() =>
                    {
                        scalar_string(value)
                            .map(|prefix| {
                                Self::leaf(
                                    field.id,
                                    primitive,
                                    Operation::StartsWith(prefix.to_string()),
                                )
                            })
                            .unwrap_or(Self::Unknown)
                    }
                    _ => Self::Unknown,
                }
            }
            Expr::Like(like) if !like.case_insensitive => {
                let (Some(field), Expr::Literal(value, _)) =
                    (source_field(schema, &like.expr), like.pattern.as_ref())
                else {
                    return Self::Unknown;
                };
                let Type::Primitive(primitive @ PrimitiveType::String) = field.field_type.as_ref()
                else {
                    return Self::Unknown;
                };
                let Some(pattern) = scalar_string(value) else {
                    return Self::Unknown;
                };
                let Some(prefix) = pattern.strip_suffix('%') else {
                    return Self::Unknown;
                };
                if prefix
                    .chars()
                    .any(|c| c == '%' || c == '_' || c == like.escape_char.unwrap_or('\\'))
                {
                    return Self::Unknown;
                }
                let predicate = Self::leaf(
                    field.id,
                    primitive,
                    Operation::StartsWith(prefix.to_string()),
                );
                if like.negated {
                    predicate.negate()
                } else {
                    predicate
                }
            }
            Expr::Column(_) => {
                let Some(field) = source_field(schema, expr) else {
                    return Self::Unknown;
                };
                if field.field_type.as_ref() == &Type::Primitive(PrimitiveType::Boolean) {
                    Self::leaf(
                        field.id,
                        &PrimitiveType::Boolean,
                        Operation::Eq(PrimitiveLiteral::Boolean(true)),
                    )
                } else {
                    Self::Unknown
                }
            }
            _ => Self::Unknown,
        }
    }

    fn leaf(id: i32, primitive: &PrimitiveType, operation: Operation) -> Self {
        Self::Leaf {
            id,
            primitive: primitive.clone(),
            operation,
        }
    }
    fn negate(self) -> Self {
        Self::Not(Box::new(self))
    }

    pub(crate) fn supported(&self) -> bool {
        match self {
            Self::Unknown => false,
            Self::And(left, right) | Self::Or(left, right) => left.supported() && right.supported(),
            Self::Not(value) | Self::IsTrue(value) | Self::IsNull(value) => value.supported(),
            _ => true,
        }
    }

    fn evaluate(&self, leaf: &impl Fn(i32, &PrimitiveType, &Operation) -> Truth) -> Truth {
        match self {
            Self::Unknown => Truth::UNKNOWN,
            Self::Constant(value) => value.map(Truth::boolean).unwrap_or(Truth::NULL),
            Self::And(left, right) => left.evaluate(leaf).combine(right.evaluate(leaf), true),
            Self::Or(left, right) => left.evaluate(leaf).combine(right.evaluate(leaf), false),
            Self::Not(value) => value.evaluate(leaf).not(),
            Self::IsTrue(value) => {
                let truth = value.evaluate(leaf);
                Truth((truth.0 & 1) | if truth.0 & 6 != 0 { 2 } else { 0 })
            }
            Self::IsNull(value) => {
                let truth = value.evaluate(leaf);
                Truth(if truth.0 & 4 != 0 { 1 } else { 0 } | if truth.0 & 3 != 0 { 2 } else { 0 })
            }
            Self::Leaf {
                id,
                primitive,
                operation,
            } => leaf(*id, primitive, operation),
        }
    }

    pub(crate) fn file(&self, file: &DataFile, spec: Option<&PartitionSpec>) -> Truth {
        if file.record_count == 0 {
            return Truth(0);
        }
        self.evaluate(&|id, primitive, operation| {
            let metrics = Domain::file(file, id, primitive).evaluate(operation, primitive);
            let partition = spec
                .map(|spec| {
                    partition_truth(spec, id, primitive, operation, |index, _| {
                        file.partition
                            .get(index)
                            .map(|value| Domain::value(value.as_ref()))
                    })
                })
                .unwrap_or(Truth::UNKNOWN);
            metrics.intersect(partition)
        })
    }

    pub(crate) fn partition(&self, spec: &PartitionSpec, values: &[Option<Literal>]) -> Truth {
        self.evaluate(&|id, primitive, operation| {
            partition_truth(spec, id, primitive, operation, |index, _| {
                values.get(index).map(|value| Domain::value(value.as_ref()))
            })
        })
    }

    pub(crate) fn manifest(&self, spec: &PartitionSpec, summaries: &[FieldSummary]) -> Truth {
        self.evaluate(&|id, primitive, operation| {
            partition_truth(spec, id, primitive, operation, |index, transform| {
                let result_type = transform
                    .result_type(&Type::Primitive(primitive.clone()))
                    .ok()?;
                let Type::Primitive(result_type) = result_type else {
                    return None;
                };
                Some(Domain::summary(summaries.get(index)?, &result_type))
            })
        })
    }
}

fn source_field<'a>(
    schema: &'a Schema,
    expr: &Expr,
) -> Option<&'a crate::spec::types::NestedFieldRef> {
    match expr {
        Expr::Alias(alias) => source_field(schema, &alias.expr),
        Expr::Column(column) => schema
            .field_by_name(&column.name)
            .filter(|field| schema.field_path_by_id(field.id).is_some()),
        Expr::ScalarFunction(function) if function.func.inner().is::<GetFieldFunc>() => {
            let [parent, Expr::Literal(value, _)] = function.args.as_slice() else {
                return None;
            };
            let parent = source_field(schema, parent)?;
            let Type::Struct(children) = parent.field_type.as_ref() else {
                return None;
            };
            children.field_by_name(scalar_string(value)?)
        }
        Expr::Cast(cast) => {
            use datafusion::arrow::datatypes::DataType;
            let field = source_field(schema, &cast.expr)?;
            if crate::datasource::type_converter::iceberg_type_to_arrow(&field.field_type)
                .ok()
                .as_ref()
                == Some(cast.field.data_type())
            {
                return Some(field);
            }
            match (field.field_type.as_ref(), cast.field.data_type()) {
                (
                    Type::Primitive(PrimitiveType::String),
                    DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8,
                ) => Some(field),
                (Type::Primitive(PrimitiveType::Int), DataType::Int64)
                | (Type::Primitive(PrimitiveType::Float), DataType::Float64) => Some(field),
                _ => None,
            }
        }
        _ => None,
    }
}

fn scalar_string(value: &ScalarValue) -> Option<&str> {
    match value {
        ScalarValue::Utf8(Some(value))
        | ScalarValue::LargeUtf8(Some(value))
        | ScalarValue::Utf8View(Some(value)) => Some(value),
        _ => None,
    }
}
fn floating(primitive: &PrimitiveType) -> bool {
    matches!(primitive, PrimitiveType::Float | PrimitiveType::Double)
}
fn nan(value: &PrimitiveLiteral) -> bool {
    matches!(value, PrimitiveLiteral::Float(value) if value.is_nan())
        || matches!(value, PrimitiveLiteral::Double(value) if value.is_nan())
}

pub(crate) fn compare(left: &PrimitiveLiteral, right: &PrimitiveLiteral) -> Option<Ordering> {
    use PrimitiveLiteral::*;
    match (left, right) {
        (Int(left), Long(right)) => Some(i64::from(*left).cmp(right)),
        (Long(left), Int(right)) => Some(left.cmp(&i64::from(*right))),
        (Float(left), Double(right)) => Some(compare_float(f64::from(left.0), right.0)),
        (Double(left), Float(right)) => Some(compare_float(left.0, f64::from(right.0))),
        (Float(left), Float(right)) => Some(compare_float(f64::from(left.0), f64::from(right.0))),
        (Double(left), Double(right)) => Some(compare_float(left.0, right.0)),
        _ if std::mem::discriminant(left) == std::mem::discriminant(right) => Some(left.cmp(right)),
        _ => None,
    }
}

fn compare_float(left: f64, right: f64) -> Ordering {
    if left == 0.0 && right == 0.0 {
        Ordering::Equal
    } else {
        left.total_cmp(&right)
    }
}

struct Domain {
    lower: Option<PrimitiveLiteral>,
    upper: Option<PrimitiveLiteral>,
    null: bool,
    nan: bool,
    ordinary: bool,
}

impl Domain {
    fn file(file: &DataFile, id: i32, primitive: &PrimitiveType) -> Self {
        let nulls = file.null_value_counts.get(&id).copied();
        let nans = if floating(primitive) {
            file.nan_value_counts.get(&id).copied()
        } else {
            Some(0)
        };
        // Row-level primitive fields have at most one value per row, including
        // nulls under optional structs. Repeated list/map leaves are never bound.
        let ordinary = nulls
            .zip(nans)
            .is_none_or(|(nulls, nans)| nulls.saturating_add(nans) < file.record_count);
        Self {
            lower: file
                .lower_bounds
                .get(&id)
                .map(|d| d.literal.clone())
                .filter(|v| !nan(v)),
            upper: file
                .upper_bounds
                .get(&id)
                .map(|d| d.literal.clone())
                .filter(|v| !nan(v)),
            null: nulls != Some(0),
            nan: nans != Some(0),
            ordinary,
        }
    }
    fn value(value: Option<&Literal>) -> Self {
        match value {
            None => Self {
                lower: None,
                upper: None,
                null: true,
                nan: false,
                ordinary: false,
            },
            Some(Literal::Primitive(value)) => Self {
                lower: Some(value.clone()),
                upper: Some(value.clone()),
                null: false,
                nan: nan(value),
                ordinary: !nan(value),
            },
            _ => Self {
                lower: None,
                upper: None,
                null: true,
                nan: true,
                ordinary: true,
            },
        }
    }
    fn summary(summary: &FieldSummary, primitive: &PrimitiveType) -> Self {
        Self {
            lower: summary
                .lower_bound_bytes
                .as_ref()
                .and_then(|bytes| primitive.literal_from_bytes(bytes).ok()),
            upper: summary
                .upper_bound_bytes
                .as_ref()
                .and_then(|bytes| primitive.literal_from_bytes(bytes).ok()),
            null: summary.contains_null,
            nan: floating(primitive) && summary.contains_nan != Some(false),
            ordinary: summary.lower_bound_bytes.is_some()
                || summary.upper_bound_bytes.is_some()
                || !summary.contains_null
                || (floating(primitive) && summary.contains_nan != Some(false)),
        }
    }
    fn evaluate(&self, operation: &Operation, _primitive: &PrimitiveType) -> Truth {
        let mut result = Truth(0);
        if self.null {
            result = result.union(match operation {
                Operation::IsNull => Truth::TRUE,
                _ => Truth::NULL,
            });
        }
        if self.nan {
            // Counts and partition equality do not preserve NaN sign/payload.
            // Row comparisons use totalOrder, so neither comparison outcome
            // can be excluded for this part of the domain.
            result = result.union(match operation {
                Operation::IsNull => Truth::FALSE,
                Operation::IsNan => Truth::TRUE,
                _ => Truth::TRUE.union(Truth::FALSE),
            });
        }
        if self.ordinary {
            result = result.union(self.ordinary_truth(
                operation,
                self.lower.as_ref(),
                self.upper.as_ref(),
            ));
        }
        result
    }
    fn ordinary_truth(
        &self,
        operation: &Operation,
        lower: Option<&PrimitiveLiteral>,
        upper: Option<&PrimitiveLiteral>,
    ) -> Truth {
        use Operation::*;
        let (yes, no) = match operation {
            IsNull => (false, true),
            IsNan => (lower.is_some_and(nan), !lower.is_some_and(nan)),
            Eq(value) => (
                lower
                    .and_then(|bound| compare(bound, value))
                    .is_none_or(|c| c != Ordering::Greater)
                    && upper
                        .and_then(|bound| compare(bound, value))
                        .is_none_or(|c| c != Ordering::Less),
                !(lower.and_then(|bound| compare(bound, value)) == Some(Ordering::Equal)
                    && upper.and_then(|bound| compare(bound, value)) == Some(Ordering::Equal)),
            ),
            Lt(value) => (
                lower
                    .and_then(|bound| compare(bound, value))
                    .is_none_or(|c| c.is_lt()),
                upper
                    .and_then(|bound| compare(bound, value))
                    .is_none_or(|c| !c.is_lt()),
            ),
            LtEq(value) => (
                lower
                    .and_then(|bound| compare(bound, value))
                    .is_none_or(|c| !c.is_gt()),
                upper
                    .and_then(|bound| compare(bound, value))
                    .is_none_or(|c| c.is_gt()),
            ),
            Gt(value) => (
                upper
                    .and_then(|bound| compare(bound, value))
                    .is_none_or(|c| c.is_gt()),
                lower
                    .and_then(|bound| compare(bound, value))
                    .is_none_or(|c| !c.is_gt()),
            ),
            GtEq(value) => (
                upper
                    .and_then(|bound| compare(bound, value))
                    .is_none_or(|c| !c.is_lt()),
                lower
                    .and_then(|bound| compare(bound, value))
                    .is_none_or(|c| c.is_lt()),
            ),
            StartsWith(prefix) => {
                let lower = match lower {
                    Some(PrimitiveLiteral::String(value)) => Some(value.as_str()),
                    _ => None,
                };
                let upper = match upper {
                    Some(PrimitiveLiteral::String(value)) => Some(value.as_str()),
                    _ => None,
                };
                let end = prefix_end(prefix);
                (
                    upper.is_none_or(|value| value >= prefix.as_str())
                        && lower
                            .zip(end.as_deref())
                            .is_none_or(|(value, end)| value < end),
                    !(lower.is_some_and(|value| value.starts_with(prefix))
                        && upper.is_some_and(|value| value.starts_with(prefix))),
                )
            }
        };
        Truth(if yes { 1 } else { 0 } | if no { 2 } else { 0 })
    }
}

fn prefix_end(prefix: &str) -> Option<String> {
    let mut chars = prefix.chars().collect::<Vec<_>>();
    while let Some(last) = chars.pop() {
        let next = if last as u32 + 1 == 0xd800 {
            0xe000
        } else {
            last as u32 + 1
        };
        if let Some(next) = char::from_u32(next) {
            chars.push(next);
            return Some(chars.into_iter().collect());
        }
    }
    None
}

fn partition_truth(
    spec: &PartitionSpec,
    id: i32,
    primitive: &PrimitiveType,
    operation: &Operation,
    domain: impl Fn(usize, Transform) -> Option<Domain>,
) -> Truth {
    spec.fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| {
            field.source_id == id
                && !matches!(field.transform, Transform::Void | Transform::Unknown)
        })
        .fold(Truth::UNKNOWN, |truth, (index, field)| {
            let Some(domain) = domain(index, field.transform) else {
                return truth;
            };
            if field.transform == Transform::Identity {
                return truth.intersect(domain.evaluate(operation, primitive));
            }
            if matches!(operation, Operation::IsNull) {
                return truth.intersect(domain.evaluate(operation, primitive));
            }
            let null = if domain.null { Truth::NULL } else { Truth(0) };
            let non_null = if domain.ordinary || domain.nan {
                let yes =
                    projected_may_match(operation, false, field.transform, primitive, &domain);
                let no = projected_may_match(operation, true, field.transform, primitive, &domain);
                Truth(if yes { 1 } else { 0 } | if no { 2 } else { 0 })
            } else {
                Truth(0)
            };
            truth.intersect(null.union(non_null))
        })
}

fn projected_may_match(
    operation: &Operation,
    negated: bool,
    transform: Transform,
    primitive: &PrimitiveType,
    domain: &Domain,
) -> bool {
    use Operation::*;
    // Project TRUE and FALSE independently; complementing an inclusive
    // projection would discard rows in transform collisions.
    let operation = match (operation, negated) {
        (Lt(value), true) => GtEq(value.clone()),
        (LtEq(value), true) => Gt(value.clone()),
        (Gt(value), true) => LtEq(value.clone()),
        (GtEq(value), true) => Lt(value.clone()),
        (Eq(value), true) => {
            if let Transform::Truncate(width) = transform
                && matches!(value, PrimitiveLiteral::String(text) if text.chars().count() < width as usize)
            {
                return !domain.evaluate(&Eq(value.clone()), primitive).all_match();
            }
            return true;
        }
        (StartsWith(prefix), _) => {
            let Transform::Truncate(width) = transform else {
                return true;
            };
            if width as usize >= prefix.chars().count() {
                let truth = domain.evaluate(operation, primitive);
                return if negated {
                    truth.not().may_match()
                } else {
                    truth.may_match()
                };
            }
            if negated {
                return true;
            }
            let prefix = PrimitiveLiteral::String(prefix.chars().take(width as usize).collect());
            return domain.evaluate(&Eq(prefix), primitive).may_match();
        }
        (IsNan, _) => return true,
        _ => operation.clone(),
    };
    let (literal, increment) = match &operation {
        Eq(value) | LtEq(value) | GtEq(value) => (value.clone(), None),
        Lt(value) => (value.clone(), Some(false)),
        Gt(value) => (value.clone(), Some(true)),
        _ => return true,
    };
    if !matches!(operation, Eq(_)) && !transform.preserves_order() {
        return true;
    }
    if !matches!(operation, Eq(_))
        && let Transform::Truncate(width) = transform
    {
        let wrapped = match primitive {
            PrimitiveType::Int => i32::try_from(width)
                .ok()
                .filter(|width| *width > 0)
                .and_then(|width| {
                    let remainder = i32::MIN.rem_euclid(width);
                    (remainder != 0)
                        .then(|| PrimitiveLiteral::Int(i32::MIN.wrapping_sub(remainder)))
                }),
            PrimitiveType::Long if width > 0 => {
                let remainder = i64::MIN.rem_euclid(i64::from(width));
                (remainder != 0).then(|| PrimitiveLiteral::Long(i64::MIN.wrapping_sub(remainder)))
            }
            _ => None,
        };
        if wrapped.is_some_and(|value| domain.evaluate(&Eq(value), primitive).may_match()) {
            return true;
        }
        if *primitive == PrimitiveType::Long
            && let Ok(width) = i32::try_from(width)
            && width > 0
        {
            let remainder = i32::MIN.rem_euclid(width);
            if remainder != 0
                && domain
                    .evaluate(
                        &Eq(PrimitiveLiteral::Int(i32::MIN.wrapping_sub(remainder))),
                        primitive,
                    )
                    .may_match()
            {
                return true;
            }
        }
    }

    let shifted = increment.and_then(|increment| shift(&literal, increment));
    let adjusted = shifted.as_ref().unwrap_or(&literal).clone();
    if let Transform::Truncate(width) = transform
        && *primitive == PrimitiveType::Long
        && let PrimitiveLiteral::Long(value) = adjusted
        && let (Ok(value), Ok(width)) = (i32::try_from(value), i32::try_from(width))
        && width > 0
        && value.checked_sub(value.rem_euclid(width)).is_none()
    {
        // A promoted INT column can retain the original wrapped INT partition.
        if !matches!(operation, Eq(_))
            || domain
                .evaluate(
                    &Eq(PrimitiveLiteral::Int(
                        value.wrapping_sub(value.rem_euclid(width)),
                    )),
                    primitive,
                )
                .may_match()
        {
            return true;
        }
    }
    if transform == Transform::Hour
        && matches!(
            primitive,
            PrimitiveType::Timestamp | PrimitiveType::Timestamptz
        )
        && !matches!(operation, Eq(_))
    {
        let lower_wrap = (i64::MAX.div_euclid(3_600_000_000) as i32).wrapping_add(1);
        let upper_wrap = (i64::MIN.div_euclid(3_600_000_000) as i32).wrapping_sub(1);
        if domain
            .evaluate(
                &LtEq(PrimitiveLiteral::Int(lower_wrap)),
                &PrimitiveType::Int,
            )
            .may_match()
            || domain
                .evaluate(
                    &GtEq(PrimitiveLiteral::Int(upper_wrap)),
                    &PrimitiveType::Int,
                )
                .may_match()
            || matches!(&adjusted, PrimitiveLiteral::Long(value) if i32::try_from(value.div_euclid(3_600_000_000)).is_err())
        {
            return true;
        }
    }
    if !matches!(operation, Eq(_))
        && let Transform::Truncate(width) = transform
    {
        let overflow = match &adjusted {
            PrimitiveLiteral::Int(value) => i32::try_from(width)
                .ok()
                .filter(|width| *width > 0)
                .is_none_or(|width| value.checked_sub(value.rem_euclid(width)).is_none()),
            PrimitiveLiteral::Long(value) if width > 0 => value
                .checked_sub(value.rem_euclid(i64::from(width)))
                .is_none(),
            _ => false,
        };
        if overflow {
            return true;
        }
    }

    let negative_time = matches!(&adjusted, PrimitiveLiteral::Int(value) if *value < 0)
        || matches!(&adjusted, PrimitiveLiteral::Long(value) if *value < 0);
    let Some(Literal::Primitive(mut value)) = apply_transform(
        transform,
        &Type::Primitive(primitive.clone()),
        Some(Literal::Primitive(adjusted)),
    ) else {
        return true;
    };
    let legacy = matches!(
        transform,
        Transform::Year | Transform::Month | Transform::Day | Transform::Hour
    ) && !matches!(
        (primitive, transform),
        (PrimitiveType::Date, Transform::Day)
    );
    if legacy
        && let PrimitiveLiteral::Int(partition) = value
        && negative_time
    {
        match operation {
            Eq(_) => {
                return domain
                    .evaluate(&Eq(PrimitiveLiteral::Int(partition)), &PrimitiveType::Int)
                    .may_match()
                    || domain
                        .evaluate(
                            &Eq(PrimitiveLiteral::Int(partition.wrapping_add(1))),
                            &PrimitiveType::Int,
                        )
                        .may_match();
            }
            Lt(_) | LtEq(_) => value = PrimitiveLiteral::Int(partition.wrapping_add(1)),
            _ => {}
        }
    }
    let short_truncate = matches!(transform, Transform::Truncate(width) if matches!(&literal, PrimitiveLiteral::String(text) if text.chars().count() < width as usize) || matches!(&literal, PrimitiveLiteral::Binary(bytes) if bytes.len() < width as usize));
    let projected = match operation {
        Eq(_) => Eq(value),
        Lt(_) if short_truncate => Lt(value),
        Gt(_) if short_truncate => Gt(value),
        Lt(_) | LtEq(_) => LtEq(value),
        Gt(_) | GtEq(_) => GtEq(value),
        _ => return true,
    };
    let result_type = transform
        .result_type(&Type::Primitive(primitive.clone()))
        .ok();
    let Some(Type::Primitive(result_type)) = result_type else {
        return true;
    };
    domain.evaluate(&projected, &result_type).may_match()
}

fn shift(value: &PrimitiveLiteral, increment: bool) -> Option<PrimitiveLiteral> {
    let offset = if increment { 1 } else { -1 };
    match value {
        PrimitiveLiteral::Int(value) => value.checked_add(offset).map(PrimitiveLiteral::Int),
        PrimitiveLiteral::Long(value) => value
            .checked_add(i64::from(offset))
            .map(PrimitiveLiteral::Long),
        PrimitiveLiteral::Int128(value) => value
            .checked_add(i128::from(offset))
            .map(PrimitiveLiteral::Int128),
        _ => None,
    }
}

mod literal_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use super::PrimitiveLiteral;

    #[derive(Serialize, Deserialize)]
    enum LiteralBits {
        Boolean(bool),
        Int(i32),
        Long(i64),
        Float(u32),
        Double(u64),
        Decimal(String),
        String(String),
        Uuid(String),
        Binary(Vec<u8>),
    }
    pub(super) fn serialize<S: Serializer>(
        value: &PrimitiveLiteral,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let bits = match value {
            PrimitiveLiteral::Boolean(v) => LiteralBits::Boolean(*v),
            PrimitiveLiteral::Int(v) => LiteralBits::Int(*v),
            PrimitiveLiteral::Long(v) => LiteralBits::Long(*v),
            PrimitiveLiteral::Float(v) => LiteralBits::Float(v.0.to_bits()),
            PrimitiveLiteral::Double(v) => LiteralBits::Double(v.0.to_bits()),
            PrimitiveLiteral::Int128(v) => LiteralBits::Decimal(v.to_string()),
            PrimitiveLiteral::String(v) => LiteralBits::String(v.clone()),
            PrimitiveLiteral::UInt128(v) => LiteralBits::Uuid(v.to_string()),
            PrimitiveLiteral::Binary(v) => LiteralBits::Binary(v.clone()),
        };
        bits.serialize(serializer)
    }
    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<PrimitiveLiteral, D::Error> {
        Ok(match LiteralBits::deserialize(deserializer)? {
            LiteralBits::Boolean(v) => PrimitiveLiteral::Boolean(v),
            LiteralBits::Int(v) => PrimitiveLiteral::Int(v),
            LiteralBits::Long(v) => PrimitiveLiteral::Long(v),
            LiteralBits::Float(v) => PrimitiveLiteral::Float(f32::from_bits(v).into()),
            LiteralBits::Double(v) => PrimitiveLiteral::Double(f64::from_bits(v).into()),
            LiteralBits::Decimal(v) => {
                PrimitiveLiteral::Int128(v.parse().map_err(serde::de::Error::custom)?)
            }
            LiteralBits::String(v) => PrimitiveLiteral::String(v),
            LiteralBits::Uuid(v) => {
                PrimitiveLiteral::UInt128(v.parse().map_err(serde::de::Error::custom)?)
            }
            LiteralBits::Binary(v) => PrimitiveLiteral::Binary(v),
        })
    }
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use datafusion_expr::{col, lit};

    use super::*;
    use crate::spec::NestedField;

    #[test]
    fn time_projection_includes_historical_offsets_and_never_overclaims() {
        let primitive = PrimitiveType::Timestamp;
        let schema = Schema::builder()
            .with_fields([
                NestedField::optional(1, "ts", Type::Primitive(primitive.clone())).into(),
            ])
            .build()
            .expect("schema");
        for transform in [
            Transform::Year,
            Transform::Month,
            Transform::Day,
            Transform::Hour,
        ] {
            let spec = PartitionSpec::builder()
                .add_field_with_id(1, 1000, "period", transform)
                .build();
            for value in [
                -31_536_000_000_001i64,
                -86_400_000_000,
                -3_600_000_001,
                -1,
                0,
                1,
                86_400_000_000,
            ] {
                let literal = PrimitiveLiteral::Long(value);
                let partition = apply_transform(
                    transform,
                    &Type::Primitive(primitive.clone()),
                    Some(Literal::Primitive(literal.clone())),
                )
                .expect("partition");
                let mut partitions = vec![partition.clone()];
                if let Literal::Primitive(PrimitiveLiteral::Int(value)) = partition
                    && value < 0
                {
                    partitions.push(Literal::Primitive(PrimitiveLiteral::Int(value + 1)));
                }
                for bound in [-86_400_000_000i64, -1, 0, 1, 86_400_000_000] {
                    for operation in [
                        Operation::Eq(PrimitiveLiteral::Long(bound)),
                        Operation::Lt(PrimitiveLiteral::Long(bound)),
                        Operation::LtEq(PrimitiveLiteral::Long(bound)),
                        Operation::Gt(PrimitiveLiteral::Long(bound)),
                        Operation::GtEq(PrimitiveLiteral::Long(bound)),
                    ] {
                        let actual = Domain::value(Some(&Literal::Primitive(literal.clone())))
                            .evaluate(&operation, &primitive);
                        let predicate = Predicate::leaf(1, &primitive, operation);
                        for partition in &partitions {
                            let evidence = predicate.partition(&spec, &[Some(partition.clone())]);
                            assert_eq!(
                                actual.intersect(evidence),
                                actual,
                                "{transform:?} {value} {bound} {partition:?}"
                            );
                        }
                    }
                }
            }
        }
        assert!(
            Predicate::new(
                &schema,
                &col("ts").lt(lit(ScalarValue::TimestampMicrosecond(Some(0), None)))
            )
            .supported()
        );
    }

    #[test]
    fn serialized_predicates_preserve_literal_types_and_nonfinite_bits() {
        for (primitive, value) in [
            (
                PrimitiveType::Double,
                PrimitiveLiteral::Double(f64::NAN.into()),
            ),
            (
                PrimitiveType::Float,
                PrimitiveLiteral::Float(f32::INFINITY.into()),
            ),
            (PrimitiveType::Long, PrimitiveLiteral::Long(i64::MIN)),
            (
                PrimitiveType::Decimal {
                    precision: 38,
                    scale: 2,
                },
                PrimitiveLiteral::Int128(12_345),
            ),
        ] {
            let predicate = Predicate::leaf(1, &primitive, Operation::Eq(value.clone()));
            let bytes = serde_json::to_vec(&predicate).expect("encode");
            let decoded: Predicate = serde_json::from_slice(&bytes).expect("decode");
            let value = Literal::Primitive(value);
            assert!(
                decoded
                    .evaluate(&|_, primitive, operation| Domain::value(Some(&value))
                        .evaluate(operation, primitive))
                    .may_match()
            );
            assert_eq!(serde_json::to_vec(&decoded).expect("re-encode"), bytes);
        }
    }

    #[test]
    fn nan_null_and_finite_bounds_have_distinct_outcomes() {
        let domain = Domain {
            lower: Some(PrimitiveLiteral::Double(1.0.into())),
            upper: Some(PrimitiveLiteral::Double(1.0.into())),
            null: true,
            nan: true,
            ordinary: true,
        };
        assert_eq!(
            domain.evaluate(
                &Operation::Eq(PrimitiveLiteral::Double(f64::NAN.into())),
                &PrimitiveType::Double
            ),
            Truth::UNKNOWN
        );
        assert_eq!(
            domain.evaluate(
                &Operation::Gt(PrimitiveLiteral::Double(1.0.into())),
                &PrimitiveType::Double
            ),
            Truth::UNKNOWN
        );
        assert_eq!(
            domain.evaluate(&Operation::IsNan, &PrimitiveType::Double),
            Truth::UNKNOWN
        );
        assert_eq!(Truth::NULL.not(), Truth::NULL);
        assert_eq!(Truth::NULL.combine(Truth::FALSE, true), Truth::FALSE);
        assert_eq!(Truth::NULL.combine(Truth::TRUE, false), Truth::TRUE);
    }

    #[test]
    fn integer_truncate_wrapping_partition_is_not_a_range_proof() {
        let primitive = PrimitiveType::Int;
        let transform = Transform::Truncate(10);
        let spec = PartitionSpec::builder()
            .add_field_with_id(1, 1000, "truncated", transform)
            .build();
        let value = Literal::Primitive(PrimitiveLiteral::Int(i32::MIN));
        let partition =
            apply_transform(transform, &Type::Primitive(primitive.clone()), Some(value))
                .expect("partition");
        for source in [i32::MIN, i32::MIN + 1, -1, 0, 1, i32::MAX] {
            let value = Literal::Primitive(PrimitiveLiteral::Int(source));
            let partition = apply_transform(
                transform,
                &Type::Primitive(primitive.clone()),
                Some(value.clone()),
            )
            .expect("partition");
            for bound in [i32::MIN, i32::MIN + 1, -1, 0, i32::MAX] {
                for operation in [
                    Operation::Lt(PrimitiveLiteral::Int(bound)),
                    Operation::LtEq(PrimitiveLiteral::Int(bound)),
                    Operation::Gt(PrimitiveLiteral::Int(bound)),
                    Operation::GtEq(PrimitiveLiteral::Int(bound)),
                ] {
                    let actual = Domain::value(Some(&value)).evaluate(&operation, &primitive);
                    let evidence = Predicate::leaf(1, &primitive, operation)
                        .partition(&spec, &[Some(partition.clone())]);
                    assert_eq!(
                        actual.intersect(evidence),
                        actual,
                        "source={source} bound={bound}"
                    );
                }
            }
        }
        for operation in [
            Operation::Eq(PrimitiveLiteral::Int(i32::MIN)),
            Operation::Lt(PrimitiveLiteral::Int(0)),
        ] {
            assert!(
                Predicate::leaf(1, &primitive, operation)
                    .partition(&spec, &[Some(partition.clone())])
                    .may_match()
            );
        }
    }

    #[test]
    fn promoted_integer_and_extreme_hour_partitions_preserve_row_truth() {
        for (primitive, written_type, transform, values) in [
            (
                PrimitiveType::Long,
                PrimitiveType::Int,
                Transform::Truncate(10),
                vec![
                    i64::from(i32::MIN),
                    i64::from(i32::MIN) + 1,
                    -1,
                    0,
                    i64::from(i32::MAX),
                ],
            ),
            (
                PrimitiveType::Timestamp,
                PrimitiveType::Timestamp,
                Transform::Hour,
                vec![
                    i64::MIN,
                    -7_730_941_132_800_000_000,
                    -1,
                    0,
                    7_730_941_132_800_000_000,
                    i64::MAX,
                ],
            ),
        ] {
            let spec = PartitionSpec::builder()
                .add_field_with_id(1, 1000, "p", transform)
                .build();
            for source in &values {
                let value = Literal::Primitive(PrimitiveLiteral::Long(*source));
                let written = if written_type == PrimitiveType::Int {
                    PrimitiveLiteral::Int(*source as i32)
                } else {
                    PrimitiveLiteral::Long(*source)
                };
                let partition = apply_transform(
                    transform,
                    &Type::Primitive(written_type.clone()),
                    Some(Literal::Primitive(written)),
                )
                .expect("partition");
                for bound in &values {
                    for operation in [
                        Operation::Eq(PrimitiveLiteral::Long(*bound)),
                        Operation::Lt(PrimitiveLiteral::Long(*bound)),
                        Operation::LtEq(PrimitiveLiteral::Long(*bound)),
                        Operation::Gt(PrimitiveLiteral::Long(*bound)),
                        Operation::GtEq(PrimitiveLiteral::Long(*bound)),
                    ] {
                        let actual = Domain::value(Some(&value)).evaluate(&operation, &primitive);
                        let evidence = Predicate::leaf(1, &primitive, operation)
                            .partition(&spec, &[Some(partition.clone())]);
                        assert_eq!(
                            actual.intersect(evidence),
                            actual,
                            "{transform:?}: {source} {bound} {partition:?}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn escaped_like_and_same_name_udfs_do_not_claim_builtin_semantics() {
        use std::sync::Arc;

        use datafusion::arrow::datatypes::DataType;
        use datafusion_expr::{ColumnarValue, Volatility, create_udf};
        let schema = Schema::builder()
            .with_fields([
                NestedField::optional(1, "p", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(2, "f", Type::Primitive(PrimitiveType::Double)).into(),
            ])
            .build()
            .expect("schema");
        for pattern in [r"a\%", r"a\\%", r"a\_%"] {
            assert!(!Predicate::new(&schema, &col("p").like(lit(pattern))).supported());
        }
        for (name, args, types) in [
            ("isnan", vec![col("f")], vec![DataType::Float64]),
            (
                "starts_with",
                vec![col("p"), lit("a")],
                vec![DataType::Utf8, DataType::Utf8],
            ),
            (
                "get_field",
                vec![col("p"), lit("a")],
                vec![DataType::Utf8, DataType::Utf8],
            ),
        ] {
            let function = create_udf(
                name,
                types,
                DataType::Boolean,
                Volatility::Immutable,
                Arc::new(|_| Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(false))))),
            );
            let predicate = Predicate::new(&schema, &function.call(args).eq(lit(true)));
            assert!(!predicate.supported());
            assert_eq!(predicate.evaluate(&|_, _, _| Truth::TRUE), Truth::UNKNOWN);
        }
    }

    #[test]
    fn nan_evidence_contains_physical_comparison_outcomes() -> datafusion_common::Result<()> {
        use std::sync::Arc;

        use datafusion::arrow::array::{Float64Array, as_boolean_array};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::prelude::SessionContext;
        use datafusion_common::ToDFSchema;
        let schema = Schema::builder()
            .with_fields([
                NestedField::optional(1, "p", Type::Primitive(PrimitiveType::Double)).into(),
            ])
            .build()
            .expect("schema");
        let arrow = Arc::new(crate::datasource::type_converter::iceberg_schema_to_arrow(
            &schema,
        )?);
        let values = [
            f64::NAN,
            f64::from_bits(0xfff8_0000_0000_0000),
            f64::from_bits(0x7ff8_0000_0000_0001),
        ];
        let batch = RecordBatch::try_new(
            arrow.clone(),
            vec![Arc::new(Float64Array::from(values.to_vec()))],
        )?;
        let domain = Domain {
            lower: None,
            upper: None,
            null: false,
            nan: true,
            ordinary: false,
        };
        for value in [0.0, f64::NAN, values[1], values[2]] {
            for op in [Operator::Eq, Operator::NotEq, Operator::Lt, Operator::Gt] {
                let expr = Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(col("p")),
                    op,
                    Box::new(lit(value)),
                ));
                let expected = SessionContext::new()
                    .state()
                    .create_physical_expr(expr.clone(), &arrow.clone().to_dfschema()?)?
                    .evaluate(&batch)?
                    .into_array(values.len())?;
                let evidence = Predicate::new(&schema, &expr)
                    .evaluate(&|_, primitive, operation| domain.evaluate(operation, primitive));
                for actual in as_boolean_array(expected.as_ref()).iter().flatten() {
                    assert_eq!(
                        evidence.intersect(Truth::boolean(actual)),
                        Truth::boolean(actual)
                    );
                }
            }
        }
        Ok(())
    }
}
