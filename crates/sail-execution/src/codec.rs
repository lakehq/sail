use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use datafusion::common::{plan_datafusion_err, plan_err, Result};
use datafusion::execution::FunctionRegistry;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_proto::generated::datafusion_common as gen_datafusion_common;
use datafusion_proto::physical_plan::{AsExecutionPlan, PhysicalExtensionCodec};
use datafusion_proto::protobuf::PhysicalPlanNode;
use prost::bytes::BytesMut;
use prost::Message;
use sail_plan::extension::logical::{Range, ShowStringFormat, ShowStringStyle};
use sail_plan::extension::physical::{RangeExec, ShowStringExec};

use crate::plan::gen;
use crate::plan::gen::extended_physical_plan_node::NodeKind;
use crate::plan::gen::ExtendedPhysicalPlanNode;

pub struct RemoteExecutionCodec {
    context: SessionContext,
}

impl Debug for RemoteExecutionCodec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "RemoteExecutionCodec")
    }
}

impl PhysicalExtensionCodec for RemoteExecutionCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        _inputs: &[Arc<dyn ExecutionPlan>],
        registry: &dyn FunctionRegistry,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let node = ExtendedPhysicalPlanNode::decode(buf)
            .map_err(|e| plan_datafusion_err!("failed to decode plan: {e:?}"))?;
        let ExtendedPhysicalPlanNode { node_kind } = node;
        let node_kind = match node_kind {
            Some(x) => x,
            None => return plan_err!("no physical plan node found"),
        };
        match node_kind {
            NodeKind::Range(gen::RangeExecNode {
                start,
                end,
                step,
                num_partitions,
                schema,
            }) => Ok(Arc::new(RangeExec::new(
                Range { start, end, step },
                num_partitions as usize,
                self.try_decode_schema(&schema)?,
            ))),
            NodeKind::ShowString(gen::ShowStringExecNode {
                input,
                names,
                limit,
                style,
                truncate,
                schema,
            }) => Ok(Arc::new(ShowStringExec::new(
                self.try_decode_plan(&input, registry)?,
                names,
                limit as usize,
                ShowStringFormat::new(self.try_decode_show_string_style(style)?, truncate as usize),
                self.try_decode_schema(&schema)?,
            ))),
        }
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
        let node_kind = if let Some(range) = node.as_any().downcast_ref::<RangeExec>() {
            NodeKind::Range(gen::RangeExecNode {
                start: range.range().start,
                end: range.range().end,
                step: range.range().step,
                num_partitions: range.num_partitions() as u64,
                schema: self.try_encode_schema(range.schema().as_ref())?,
            })
        } else if let Some(show_string) = node.as_any().downcast_ref::<ShowStringExec>() {
            NodeKind::ShowString(gen::ShowStringExecNode {
                input: self.try_encode_plan(show_string.input().clone())?,
                names: show_string.names().to_vec(),
                limit: show_string.limit() as u64,
                style: self.try_encode_show_string_style(show_string.format().style())?,
                truncate: show_string.format().truncate() as u64,
                schema: self.try_encode_schema(show_string.schema().as_ref())?,
            })
        } else {
            return plan_err!("unsupported physical plan node");
        };
        let node = ExtendedPhysicalPlanNode {
            node_kind: Some(node_kind),
        };
        node.encode(buf)
            .map_err(|e| plan_datafusion_err!("failed to encode plan: {e:?}"))
    }
}

impl RemoteExecutionCodec {
    pub fn new(context: SessionContext) -> Self {
        Self { context }
    }

    fn try_decode_show_string_style(&self, style: i32) -> Result<ShowStringStyle> {
        let style = gen::ShowStringStyle::try_from(style)
            .map_err(|e| plan_datafusion_err!("failed to decode style: {e:?}"))?;
        let style = match style {
            gen::ShowStringStyle::Default => ShowStringStyle::Default,
            gen::ShowStringStyle::Vertical => ShowStringStyle::Vertical,
            gen::ShowStringStyle::Html => ShowStringStyle::Html,
        };
        Ok(style)
    }

    fn try_encode_show_string_style(&self, style: ShowStringStyle) -> Result<i32> {
        let style = match style {
            ShowStringStyle::Default => gen::ShowStringStyle::Default,
            ShowStringStyle::Vertical => gen::ShowStringStyle::Vertical,
            ShowStringStyle::Html => gen::ShowStringStyle::Html,
        };
        Ok(style as i32)
    }

    fn try_decode_plan(
        &self,
        buf: &[u8],
        registry: &dyn FunctionRegistry,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = PhysicalPlanNode::decode(buf)
            .map_err(|e| plan_datafusion_err!("failed to decode plan: {e:?}"))?;
        plan.try_into_physical_plan(registry, self.context.runtime_env().as_ref(), self)
    }

    fn try_encode_plan(&self, plan: Arc<dyn ExecutionPlan>) -> Result<Vec<u8>> {
        let plan = PhysicalPlanNode::try_from_physical_plan(plan, self)?;
        let mut buffer = BytesMut::new();
        plan.encode(&mut buffer)
            .map_err(|e| plan_datafusion_err!("failed to encode plan: {e:?}"))?;
        Ok(buffer.freeze().into())
    }

    fn try_decode_schema(&self, buf: &[u8]) -> Result<SchemaRef> {
        let schema = gen_datafusion_common::Schema::decode(buf)
            .map_err(|e| plan_datafusion_err!("failed to decode schema: {e:?}"))?;
        Ok(Arc::new((&schema).try_into()?))
    }

    fn try_encode_schema(&self, schema: &Schema) -> Result<Vec<u8>> {
        let schema = gen_datafusion_common::Schema::try_from(schema)?;
        let mut buffer = BytesMut::new();
        schema
            .encode(&mut buffer)
            .map_err(|e| plan_datafusion_err!("failed to encode schema: {e:?}"))?;
        Ok(buffer.freeze().into())
    }
}
