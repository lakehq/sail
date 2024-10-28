use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use datafusion::common::{plan_datafusion_err, plan_err, Result};
use datafusion::execution::FunctionRegistry;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion::prelude::SessionContext;
use datafusion_proto::generated::datafusion_common as gen_datafusion_common;
use datafusion_proto::physical_plan::from_proto::parse_protobuf_partitioning;
use datafusion_proto::physical_plan::to_proto::serialize_partitioning;
use datafusion_proto::physical_plan::{AsExecutionPlan, PhysicalExtensionCodec};
use datafusion_proto::protobuf::PhysicalPlanNode;
use prost::bytes::BytesMut;
use prost::Message;
use sail_common::utils::{read_record_batches, write_record_batches};
use sail_plan::extension::logical::{Range, ShowStringFormat, ShowStringStyle};
use sail_plan::extension::physical::{RangeExec, ShowStringExec};

use crate::plan::gen::extended_physical_plan_node::NodeKind;
use crate::plan::gen::ExtendedPhysicalPlanNode;
use crate::plan::{
    gen, ShuffleReadExec, ShuffleReadLocation, ShuffleWriteExec, ShuffleWriteLocation,
};

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
            }) => {
                let schema = self.try_decode_message::<gen_datafusion_common::Schema>(&schema)?;
                Ok(Arc::new(RangeExec::new(
                    Range { start, end, step },
                    num_partitions as usize,
                    Arc::new((&schema).try_into()?),
                )))
            }
            NodeKind::ShowString(gen::ShowStringExecNode {
                input,
                names,
                limit,
                style,
                truncate,
                schema,
            }) => {
                let schema = self.try_decode_message::<gen_datafusion_common::Schema>(&schema)?;
                Ok(Arc::new(ShowStringExec::new(
                    self.try_decode_plan(&input, registry)?,
                    names,
                    limit as usize,
                    ShowStringFormat::new(
                        self.try_decode_show_string_style(style)?,
                        truncate as usize,
                    ),
                    Arc::new((&schema).try_into()?),
                )))
            }
            NodeKind::ShuffleRead(gen::ShuffleReadExecNode {
                job_id,
                stage,
                schema,
                partitioning,
                locations,
            }) => {
                let schema = self.try_decode_message::<gen_datafusion_common::Schema>(&schema)?;
                let schema: SchemaRef = Arc::new((&schema).try_into()?);
                let partitioning =
                    self.try_decode_partitioning(&partitioning, registry, &schema)?;
                let locations = locations
                    .into_iter()
                    .map(|x| self.try_decode_shuffle_read_location_list(x))
                    .collect::<Result<_>>()?;
                let node =
                    ShuffleReadExec::new(job_id.into(), stage as usize, schema, partitioning);
                let node = node.with_locations(locations);
                Ok(Arc::new(node))
            }
            NodeKind::ShuffleWrite(gen::ShuffleWriteExecNode {
                job_id,
                stage,
                plan,
                partitioning,
                locations,
            }) => {
                let plan = self.try_decode_plan(&plan, registry)?;
                let partitioning =
                    self.try_decode_partitioning(&partitioning, registry, &plan.schema())?;
                let locations = locations
                    .into_iter()
                    .map(|x| self.try_decode_shuffle_write_location_list(x))
                    .collect::<Result<_>>()?;
                let node = ShuffleWriteExec::new(job_id.into(), stage as usize, plan, partitioning);
                let node = node.with_locations(locations);
                Ok(Arc::new(node))
            }
            NodeKind::Memory(gen::MemoryExecNode {
                partitions,
                schema,
                projection,
            }) => {
                let schema = self.try_decode_message::<gen_datafusion_common::Schema>(&schema)?;
                let schema = Arc::new((&schema).try_into()?);
                let partitions = partitions
                    .into_iter()
                    .map(read_record_batches)
                    .collect::<Result<Vec<_>>>()?;
                let projection =
                    projection.map(|x| x.columns.into_iter().map(|c| c as usize).collect());
                Ok(Arc::new(MemoryExec::try_new(
                    &partitions,
                    schema,
                    projection,
                )?))
            }
        }
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
        let node_kind = if let Some(range) = node.as_any().downcast_ref::<RangeExec>() {
            let schema = self.try_encode_message::<gen_datafusion_common::Schema>(
                range.schema().as_ref().try_into()?,
            )?;
            NodeKind::Range(gen::RangeExecNode {
                start: range.range().start,
                end: range.range().end,
                step: range.range().step,
                num_partitions: range.num_partitions() as u64,
                schema,
            })
        } else if let Some(show_string) = node.as_any().downcast_ref::<ShowStringExec>() {
            let schema = self.try_encode_message::<gen_datafusion_common::Schema>(
                show_string.schema().as_ref().try_into()?,
            )?;
            NodeKind::ShowString(gen::ShowStringExecNode {
                input: self.try_encode_plan(show_string.input().clone())?,
                names: show_string.names().to_vec(),
                limit: show_string.limit() as u64,
                style: self.try_encode_show_string_style(show_string.format().style())?,
                truncate: show_string.format().truncate() as u64,
                schema,
            })
        } else if let Some(shuffle_read) = node.as_any().downcast_ref::<ShuffleReadExec>() {
            let schema = self.try_encode_message::<gen_datafusion_common::Schema>(
                shuffle_read.schema().as_ref().try_into()?,
            )?;
            let partitioning = self.try_encode_partitioning(shuffle_read.partitioning())?;
            let locations = shuffle_read
                .locations()
                .iter()
                .map(|x| self.try_encode_shuffle_read_location_list(x))
                .collect::<Result<_>>()?;
            NodeKind::ShuffleRead(gen::ShuffleReadExecNode {
                job_id: shuffle_read.job_id().into(),
                stage: shuffle_read.stage() as u64,
                schema,
                partitioning,
                locations,
            })
        } else if let Some(shuffle_write) = node.as_any().downcast_ref::<ShuffleWriteExec>() {
            let partitioning = self.try_encode_partitioning(shuffle_write.partitioning())?;
            let locations = shuffle_write
                .locations()
                .iter()
                .map(|x| self.try_encode_shuffle_write_location_list(x))
                .collect::<Result<_>>()?;
            NodeKind::ShuffleWrite(gen::ShuffleWriteExecNode {
                job_id: shuffle_write.job_id().into(),
                stage: shuffle_write.stage() as u64,
                plan: self.try_encode_plan(shuffle_write.plan().clone())?,
                partitioning,
                locations,
            })
        } else if let Some(memory) = node.as_any().downcast_ref::<MemoryExec>() {
            // `memory.schema()` is the schema after projection.
            // We must use the original schema here.
            let schema = memory.original_schema();
            let partitions = memory
                .partitions()
                .iter()
                .map(|x| write_record_batches(x, schema.as_ref()))
                .collect::<Result<_>>()?;
            let projection = memory
                .projection()
                .as_ref()
                .map(|x| gen::PhysicalProjection {
                    columns: x.iter().map(|c| *c as u64).collect(),
                });
            let schema = self
                .try_encode_message::<gen_datafusion_common::Schema>(schema.as_ref().try_into()?)?;
            NodeKind::Memory(gen::MemoryExecNode {
                partitions,
                schema,
                projection,
            })
        } else {
            return plan_err!("unsupported physical plan node: {node:?}");
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

    fn try_decode_partitioning(
        &self,
        buf: &[u8],
        registry: &dyn FunctionRegistry,
        schema: &Schema,
    ) -> Result<Partitioning> {
        let partitioning = self.try_decode_message(buf)?;
        parse_protobuf_partitioning(Some(&partitioning), registry, schema, self)?
            .ok_or_else(|| plan_datafusion_err!("no partitioning found"))
    }

    fn try_encode_partitioning(&self, partitioning: &Partitioning) -> Result<Vec<u8>> {
        let partitioning = serialize_partitioning(partitioning, self)?;
        self.try_encode_message(partitioning)
    }

    fn try_decode_shuffle_read_location(
        &self,
        location: gen::ShuffleReadLocation,
    ) -> Result<ShuffleReadLocation> {
        let gen::ShuffleReadLocation { location } = location;
        let location = match location {
            Some(gen::shuffle_read_location::Location::ThisWorker(
                gen::ShuffleReadLocationThisWorker { channel },
            )) => ShuffleReadLocation::ThisWorker {
                channel: channel.into(),
            },
            Some(gen::shuffle_read_location::Location::OtherWorker(
                gen::ShuffleReadLocationOtherWorker {
                    host,
                    port,
                    channel,
                },
            )) => ShuffleReadLocation::OtherWorker {
                host,
                port: u16::try_from(port)
                    .map_err(|_| plan_datafusion_err!("failed to decode port: {port:?}"))?,
                channel: channel.into(),
            },
            Some(gen::shuffle_read_location::Location::Remote(
                gen::ShuffleReadLocationRemote { uri },
            )) => ShuffleReadLocation::Remote { uri },
            None => return plan_err!("no shuffle read location found"),
        };
        Ok(location)
    }

    fn try_encode_shuffle_read_location(
        &self,
        location: &ShuffleReadLocation,
    ) -> Result<gen::ShuffleReadLocation> {
        let location = match location {
            ShuffleReadLocation::ThisWorker { channel } => gen::ShuffleReadLocation {
                location: Some(gen::shuffle_read_location::Location::ThisWorker(
                    gen::ShuffleReadLocationThisWorker {
                        channel: channel.clone().into(),
                    },
                )),
            },
            ShuffleReadLocation::OtherWorker {
                host,
                port,
                channel,
            } => gen::ShuffleReadLocation {
                location: Some(gen::shuffle_read_location::Location::OtherWorker(
                    gen::ShuffleReadLocationOtherWorker {
                        host: host.clone(),
                        port: *port as u32,
                        channel: channel.clone().into(),
                    },
                )),
            },
            ShuffleReadLocation::Remote { uri } => gen::ShuffleReadLocation {
                location: Some(gen::shuffle_read_location::Location::Remote(
                    gen::ShuffleReadLocationRemote { uri: uri.clone() },
                )),
            },
        };
        Ok(location)
    }

    fn try_decode_shuffle_read_location_list(
        &self,
        locations: gen::ShuffleReadLocationList,
    ) -> Result<Vec<ShuffleReadLocation>> {
        let gen::ShuffleReadLocationList { locations } = locations;
        locations
            .into_iter()
            .map(|location| self.try_decode_shuffle_read_location(location))
            .collect()
    }

    fn try_encode_shuffle_read_location_list(
        &self,
        locations: &[ShuffleReadLocation],
    ) -> Result<gen::ShuffleReadLocationList> {
        let locations = locations
            .iter()
            .map(|location| self.try_encode_shuffle_read_location(location))
            .collect::<Result<_>>()?;
        Ok(gen::ShuffleReadLocationList { locations })
    }

    fn try_decode_shuffle_write_location(
        &self,
        location: gen::ShuffleWriteLocation,
    ) -> Result<ShuffleWriteLocation> {
        let gen::ShuffleWriteLocation { location } = location;
        let location = match location {
            Some(gen::shuffle_write_location::Location::Memory(
                gen::ShuffleWriteLocationMemory { channel },
            )) => ShuffleWriteLocation::Memory {
                channel: channel.into(),
            },
            Some(gen::shuffle_write_location::Location::Disk(gen::ShuffleWriteLocationDisk {
                channel,
            })) => ShuffleWriteLocation::Disk {
                channel: channel.into(),
            },
            Some(gen::shuffle_write_location::Location::Remote(
                gen::ShuffleWriteLocationRemote { uri },
            )) => ShuffleWriteLocation::Remote { uri },
            None => return plan_err!("no shuffle write location found"),
        };
        Ok(location)
    }

    fn try_encode_shuffle_write_location(
        &self,
        location: &ShuffleWriteLocation,
    ) -> Result<gen::ShuffleWriteLocation> {
        let location = match location {
            ShuffleWriteLocation::Memory { channel } => gen::ShuffleWriteLocation {
                location: Some(gen::shuffle_write_location::Location::Memory(
                    gen::ShuffleWriteLocationMemory {
                        channel: channel.clone().into(),
                    },
                )),
            },
            ShuffleWriteLocation::Disk { channel } => gen::ShuffleWriteLocation {
                location: Some(gen::shuffle_write_location::Location::Disk(
                    gen::ShuffleWriteLocationDisk {
                        channel: channel.clone().into(),
                    },
                )),
            },
            ShuffleWriteLocation::Remote { uri } => gen::ShuffleWriteLocation {
                location: Some(gen::shuffle_write_location::Location::Remote(
                    gen::ShuffleWriteLocationRemote { uri: uri.clone() },
                )),
            },
        };
        Ok(location)
    }

    fn try_decode_shuffle_write_location_list(
        &self,
        locations: gen::ShuffleWriteLocationList,
    ) -> Result<Vec<ShuffleWriteLocation>> {
        let gen::ShuffleWriteLocationList { locations } = locations;
        locations
            .into_iter()
            .map(|location| self.try_decode_shuffle_write_location(location))
            .collect()
    }

    fn try_encode_shuffle_write_location_list(
        &self,
        locations: &[ShuffleWriteLocation],
    ) -> Result<gen::ShuffleWriteLocationList> {
        let locations = locations
            .iter()
            .map(|location| self.try_encode_shuffle_write_location(location))
            .collect::<Result<_>>()?;
        Ok(gen::ShuffleWriteLocationList { locations })
    }

    fn try_decode_message<M>(&self, buf: &[u8]) -> Result<M>
    where
        M: Message + Default,
    {
        let message =
            M::decode(buf).map_err(|e| plan_datafusion_err!("failed to decode message: {e:?}"))?;
        Ok(message)
    }

    fn try_encode_message<M>(&self, message: M) -> Result<Vec<u8>>
    where
        M: Message,
    {
        let mut buffer = BytesMut::new();
        message
            .encode(&mut buffer)
            .map_err(|e| plan_datafusion_err!("failed to encode message: {e:?}"))?;
        Ok(buffer.freeze().into())
    }
}
