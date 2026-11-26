// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use datafusion::arrow::datatypes::{FieldRef, Schema};
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{Result, plan_err};
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_expr::{HigherOrderFunctionExpr, Partitioning, PhysicalExpr};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::generated::datafusion_common as gen_datafusion_common;
use datafusion_proto::physical_plan::to_proto::serialize_partitioning;
use datafusion_proto::physical_plan::{
    PhysicalExtensionCodec, PhysicalPlanNodeExt, PhysicalProtoConverterExtension,
};
use datafusion_proto::protobuf::{PhysicalExprNode, PhysicalPlanNode};
use prost::Message;
use sail_function::scalar::array::array_aggregate::ArrayAggregate;
use sail_function::scalar::array::array_exists::ArrayExists;
use sail_function::scalar::array::array_filter::ArrayFilter;
use sail_function::scalar::array::array_forall::ArrayForall;
use sail_function::scalar::array::array_sort::ArraySort;
use sail_function::scalar::array::array_transform::ArrayTransform;
use sail_function::scalar::array::array_zip_with::ArrayZipWith;
use sail_function::scalar::array::sequence::SequenceLazy;
use sail_function::scalar::conditional::coalesce_lazy::CoalesceLazy;
use sail_function::scalar::datetime::convert_tz::ConvertTzLazy;
use sail_function::scalar::map::map_filter::MapFilter;
use sail_function::scalar::map::map_zip_with::MapZipWith;
use sail_function::scalar::map::transform_keys::TransformKeys;
use sail_function::scalar::map::transform_values::TransformValues;
use sail_function::scalar::string::regexp_instr::RegexpInstr;
use sail_physical_plan::data_source::RemoteDataSourceExec;

use crate::plan::r#gen;
use crate::plan::r#gen::higher_order_udf::HigherOrderUdfKind;
use crate::proto::converter::RemotePhysicalProtoConverter;

pub fn encode_remote_physical_plan(
    codec: &dyn PhysicalExtensionCodec,
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Vec<u8>> {
    let plan = plan
        .transform(|node| {
            if let Some(data_source) = node.downcast_ref::<DataSourceExec>() {
                // TODO: Add SQL planner support and distributed SQL tests for
                // DataFusion's `file_row_index()`.
                let node =
                    Arc::new(RemoteDataSourceExec::new(data_source)) as Arc<dyn ExecutionPlan>;
                Ok(Transformed::yes(node))
            } else {
                Ok(Transformed::no(node))
            }
        })
        .data()?;
    // TODO(distributed-execution): Route dynamic-filter updates and completion from each producer
    // to every consumer with the same expression ID across stage boundaries, including
    // attempt-scoped recreation on retry and route cleanup on cancellation.
    try_encode_physical_plan(codec, plan)
}

pub fn encode_remote_physical_expr(
    codec: &dyn PhysicalExtensionCodec,
    expr: &Arc<dyn PhysicalExpr>,
) -> Result<Vec<u8>> {
    try_encode_physical_expr(codec, expr)
}

pub fn encode_remote_partitioning(
    codec: &dyn PhysicalExtensionCodec,
    partitioning: &Partitioning,
) -> Result<Vec<u8>> {
    let converter = RemotePhysicalProtoConverter::default();
    let partitioning = serialize_partitioning(partitioning, codec, &converter)?;
    try_encode_message(partitioning)
}

pub(super) fn try_encode_message<M>(message: M) -> Result<Vec<u8>>
where
    M: Message,
{
    Ok(message.encode_to_vec())
}

pub(super) fn try_encode_schema(schema: &Schema) -> Result<Vec<u8>> {
    try_encode_message::<gen_datafusion_common::Schema>(schema.try_into()?)
}

pub(super) fn try_encode_field_ref(field: &FieldRef) -> Result<Vec<u8>> {
    try_encode_message::<gen_datafusion_common::Field>(field.as_ref().try_into()?)
}

pub(super) fn try_encode_physical_plan(
    codec: &dyn PhysicalExtensionCodec,
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Vec<u8>> {
    try_encode_message(physical_plan_to_proto(codec, plan)?)
}

pub(super) fn try_encode_physical_plan_with_converter(
    codec: &dyn PhysicalExtensionCodec,
    proto_converter: &dyn PhysicalProtoConverterExtension,
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Vec<u8>> {
    try_encode_message(physical_plan_to_proto_with_converter(
        codec,
        proto_converter,
        plan,
    )?)
}

pub(super) fn physical_plan_to_proto(
    codec: &dyn PhysicalExtensionCodec,
    plan: Arc<dyn ExecutionPlan>,
) -> Result<PhysicalPlanNode> {
    let converter = RemotePhysicalProtoConverter::default();
    physical_plan_to_proto_with_converter(codec, &converter, plan)
}

pub(super) fn physical_plan_to_proto_with_converter(
    codec: &dyn PhysicalExtensionCodec,
    proto_converter: &dyn PhysicalProtoConverterExtension,
    plan: Arc<dyn ExecutionPlan>,
) -> Result<PhysicalPlanNode> {
    PhysicalPlanNode::try_from_physical_plan_with_converter(plan, codec, proto_converter)
}

pub(super) fn try_encode_physical_expr(
    codec: &dyn PhysicalExtensionCodec,
    expr: &Arc<dyn PhysicalExpr>,
) -> Result<Vec<u8>> {
    try_encode_message(physical_expr_to_proto(codec, expr)?)
}

pub(super) fn try_encode_physical_expr_with_converter(
    codec: &dyn PhysicalExtensionCodec,
    proto_converter: &dyn PhysicalProtoConverterExtension,
    expr: &Arc<dyn PhysicalExpr>,
) -> Result<Vec<u8>> {
    try_encode_message(physical_expr_to_proto_with_converter(
        codec,
        proto_converter,
        expr,
    )?)
}

pub(super) fn physical_expr_to_proto(
    codec: &dyn PhysicalExtensionCodec,
    expr: &Arc<dyn PhysicalExpr>,
) -> Result<PhysicalExprNode> {
    let converter = RemotePhysicalProtoConverter::default();
    physical_expr_to_proto_with_converter(codec, &converter, expr)
}

pub(super) fn physical_expr_to_proto_with_converter(
    codec: &dyn PhysicalExtensionCodec,
    proto_converter: &dyn PhysicalProtoConverterExtension,
    expr: &Arc<dyn PhysicalExpr>,
) -> Result<PhysicalExprNode> {
    proto_converter.physical_expr_to_proto(expr, codec)
}

pub(super) fn try_encode_higher_order_udf(
    hof: &HigherOrderFunctionExpr,
) -> Result<r#gen::HigherOrderUdf> {
    let udf_inner = hof.fun().inner().as_ref() as &dyn std::any::Any;
    let udf_kind = if let Some(filter) = udf_inner.downcast_ref::<ArrayFilter>() {
        HigherOrderUdfKind::Filter(r#gen::SparkArrayFilterUdf {
            index_first: filter.is_index_first(),
        })
    } else if udf_inner.is::<MapFilter>() {
        HigherOrderUdfKind::MapFilter(r#gen::SparkMapFilterUdf {})
    } else if udf_inner.is::<TransformValues>() {
        HigherOrderUdfKind::TransformValues(r#gen::SparkTransformValuesUdf {})
    } else if let Some(transform_keys) = udf_inner.downcast_ref::<TransformKeys>() {
        HigherOrderUdfKind::TransformKeys(r#gen::SparkTransformKeysUdf {
            last_value_wins: transform_keys.is_last_value_wins(),
            preserve_duplicates: transform_keys.preserves_duplicates(),
        })
    } else if let Some(transform) = udf_inner.downcast_ref::<ArrayTransform>() {
        HigherOrderUdfKind::Transform(r#gen::SparkArrayTransformUdf {
            index_first: transform.is_index_first(),
        })
    } else if let Some(aggregate) = udf_inner.downcast_ref::<ArrayAggregate>() {
        HigherOrderUdfKind::Aggregate(r#gen::SparkArrayAggregateUdf {
            element_first: aggregate.is_element_first(),
        })
    } else if udf_inner.is::<ArrayExists>() {
        HigherOrderUdfKind::Exists(r#gen::SparkArrayExistsUdf {})
    } else if udf_inner.is::<ArrayForall>() {
        HigherOrderUdfKind::Forall(r#gen::SparkArrayForallUdf {})
    } else if let Some(sort) = udf_inner.downcast_ref::<ArraySort>() {
        HigherOrderUdfKind::Sort(r#gen::SparkArraySortUdf {
            swapped: sort.is_swapped(),
        })
    } else if let Some(sequence) = udf_inner.downcast_ref::<SequenceLazy>() {
        HigherOrderUdfKind::SparkSequenceLazy(r#gen::SparkSequenceUdf {
            session_timezone: sequence.session_timezone().to_owned(),
            ansi_mode: sequence.ansi_mode(),
        })
    } else if udf_inner.is::<CoalesceLazy>() {
        HigherOrderUdfKind::CoalesceLazy(r#gen::CoalesceLazyUdf {})
    } else if let Some(convert_tz) = udf_inner.downcast_ref::<ConvertTzLazy>() {
        HigherOrderUdfKind::ConvertTzLazy(r#gen::ConvertTzUdf {
            classic: convert_tz.classic(),
            null_short_circuit: convert_tz.null_short_circuit(),
            ..Default::default()
        })
    } else if let Some(regexp_instr) = udf_inner.downcast_ref::<RegexpInstr>() {
        HigherOrderUdfKind::RegexpInstr(r#gen::SparkRegexpInstrUdf {
            ansi_mode: regexp_instr.ansi_mode(),
        })
    } else if udf_inner.is::<ArrayZipWith>() {
        HigherOrderUdfKind::ZipWith(r#gen::SparkArrayZipWithUdf {})
    } else if let Some(map_zip_with) = udf_inner.downcast_ref::<MapZipWith>() {
        HigherOrderUdfKind::MapZipWith(r#gen::SparkMapZipWithUdf {
            ansi_mode: map_zip_with.ansi_mode(),
            session_timezone: map_zip_with.session_timezone().to_owned(),
        })
    } else {
        return plan_err!("unsupported higher-order function: {}", hof.name());
    };
    Ok(r#gen::HigherOrderUdf {
        higher_order_udf_kind: Some(udf_kind),
    })
}
