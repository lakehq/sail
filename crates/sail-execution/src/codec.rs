use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::{DataType, Schema, TimeUnit};
use datafusion::common::parsers::CompressionTypeVariant;
use datafusion::common::{plan_datafusion_err, plan_err, JoinSide, Result};
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::physical_plan::{ArrowExec, NdJsonExec};
use datafusion::execution::FunctionRegistry;
use datafusion::functions::string::overlay::OverlayFunc;
use datafusion::logical_expr::{AggregateUDF, AggregateUDFImpl, ScalarUDF, ScalarUDFImpl};
use datafusion::physical_expr::LexOrdering;
use datafusion::physical_plan::joins::utils::{ColumnIndex, JoinFilter};
use datafusion::physical_plan::joins::SortMergeJoinExec;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::recursive_query::RecursiveQueryExec;
use datafusion::physical_plan::sorts::partial_sort::PartialSortExec;
use datafusion::physical_plan::values::ValuesExec;
use datafusion::physical_plan::work_table::WorkTableExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion::prelude::SessionContext;
use datafusion_proto::generated::datafusion_common as gen_datafusion_common;
use datafusion_proto::physical_plan::from_proto::{
    parse_physical_expr, parse_physical_sort_exprs, parse_protobuf_file_scan_config,
    parse_protobuf_partitioning,
};
use datafusion_proto::physical_plan::to_proto::{
    serialize_file_scan_config, serialize_partitioning, serialize_physical_expr,
    serialize_physical_sort_exprs,
};
use datafusion_proto::physical_plan::{AsExecutionPlan, PhysicalExtensionCodec};
use datafusion_proto::protobuf::{
    JoinType as ProtoJoinType, PhysicalPlanNode, PhysicalSortExprNode,
};
use prost::bytes::BytesMut;
use prost::Message;
use sail_common::udf::StreamUDF;
use sail_common::utils::{read_record_batches, write_record_batches};
use sail_plan::extension::function::array::{ArrayEmptyToNull, ArrayItemWithPosition, MapToArray};
use sail_plan::extension::function::array_min_max::{ArrayMax, ArrayMin};
use sail_plan::extension::function::drop_struct_field::DropStructField;
use sail_plan::extension::function::explode::{explode_name_to_kind, Explode};
use sail_plan::extension::function::kurtosis::KurtosisFunction;
use sail_plan::extension::function::least_greatest::{Greatest, Least};
use sail_plan::extension::function::levenshtein::Levenshtein;
use sail_plan::extension::function::map_function::MapFunction;
use sail_plan::extension::function::max_min_by::{MaxByFunction, MinByFunction};
use sail_plan::extension::function::mode::ModeFunction;
use sail_plan::extension::function::multi_expr::MultiExpr;
use sail_plan::extension::function::raise_error::RaiseError;
use sail_plan::extension::function::randn::Randn;
use sail_plan::extension::function::random::Random;
use sail_plan::extension::function::size::Size;
use sail_plan::extension::function::skewness::SkewnessFunc;
use sail_plan::extension::function::spark_aes::{
    SparkAESDecrypt, SparkAESEncrypt, SparkTryAESDecrypt, SparkTryAESEncrypt,
};
use sail_plan::extension::function::spark_array::SparkArray;
use sail_plan::extension::function::spark_base64::{SparkBase64, SparkUnbase64};
use sail_plan::extension::function::spark_concat::SparkConcat;
use sail_plan::extension::function::spark_hex_unhex::{SparkHex, SparkUnHex};
use sail_plan::extension::function::spark_murmur3_hash::SparkMurmur3Hash;
use sail_plan::extension::function::spark_reverse::SparkReverse;
use sail_plan::extension::function::spark_unix_timestamp::SparkUnixTimestamp;
use sail_plan::extension::function::spark_weekofyear::SparkWeekOfYear;
use sail_plan::extension::function::spark_xxhash64::SparkXxhash64;
use sail_plan::extension::function::struct_function::StructFunction;
use sail_plan::extension::function::timestamp_now::TimestampNow;
use sail_plan::extension::function::update_struct_field::UpdateStructField;
use sail_plan::extension::logical::{Range, ShowStringFormat, ShowStringStyle};
use sail_plan::extension::physical::{
    MapPartitionsExec, RangeExec, SchemaPivotExec, ShowStringExec,
};
use sail_python_udf::config::PySparkUdfConfig;
use sail_python_udf::udf::pyspark_batch_collector::PySparkBatchCollectorUDF;
use sail_python_udf::udf::pyspark_cogroup_map_udf::PySparkCoGroupMapUDF;
use sail_python_udf::udf::pyspark_group_map_udf::PySparkGroupMapUDF;
use sail_python_udf::udf::pyspark_map_iter_udf::{PySparkMapIterKind, PySparkMapIterUDF};
use sail_python_udf::udf::pyspark_udaf::PySparkGroupAggregateUDF;
use sail_python_udf::udf::pyspark_udf::{PySparkUDF, PySparkUdfKind};
use sail_python_udf::udf::pyspark_udtf::{PySparkUDTF, PySparkUdtfKind};

use crate::plan::gen::extended_aggregate_udf::UdafKind;
use crate::plan::gen::extended_physical_plan_node::NodeKind;
use crate::plan::gen::extended_scalar_udf::UdfKind;
use crate::plan::gen::extended_stream_udf::StreamUdfKind;
use crate::plan::gen::{
    ExtendedAggregateUdf, ExtendedPhysicalPlanNode, ExtendedScalarUdf, ExtendedStreamUdf,
};
use crate::plan::{gen, ShuffleConsumption, ShuffleReadExec, ShuffleWriteExec};
use crate::stream::{LocalStreamStorage, TaskReadLocation, TaskWriteLocation};

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
            .map_err(|e| plan_datafusion_err!("failed to decode plan: {e}"))?;
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
                let schema = self.try_decode_schema(&schema)?;
                Ok(Arc::new(RangeExec::new(
                    Range { start, end, step },
                    num_partitions as usize,
                    Arc::new(schema),
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
                let schema = self.try_decode_schema(&schema)?;
                Ok(Arc::new(ShowStringExec::new(
                    self.try_decode_plan(&input, registry)?,
                    names,
                    limit as usize,
                    ShowStringFormat::new(
                        self.try_decode_show_string_style(style)?,
                        truncate as usize,
                    ),
                    Arc::new(schema),
                )))
            }
            NodeKind::SchemaPivot(gen::SchemaPivotExecNode {
                input,
                names,
                schema,
            }) => {
                let schema = self.try_decode_schema(&schema)?;
                Ok(Arc::new(SchemaPivotExec::new(
                    self.try_decode_plan(&input, registry)?,
                    names,
                    Arc::new(schema),
                )))
            }
            NodeKind::MapPartitions(gen::MapPartitionsExecNode { input, udf, schema }) => {
                let Some(udf) = udf else {
                    return plan_err!("no UDF found for MapPartitionsExec");
                };
                let schema = self.try_decode_schema(&schema)?;
                Ok(Arc::new(MapPartitionsExec::new(
                    self.try_decode_plan(&input, registry)?,
                    self.try_decode_stream_udf(udf)?,
                    Arc::new(schema),
                )))
            }
            NodeKind::ShuffleRead(gen::ShuffleReadExecNode {
                stage,
                schema,
                partitioning,
                locations,
            }) => {
                let schema = self.try_decode_schema(&schema)?;
                let partitioning =
                    self.try_decode_partitioning(&partitioning, registry, &schema)?;
                let locations = locations
                    .into_iter()
                    .map(|x| self.try_decode_task_read_location_list(x))
                    .collect::<Result<_>>()?;
                let node = ShuffleReadExec::new(stage as usize, Arc::new(schema), partitioning);
                let node = node.with_locations(locations);
                Ok(Arc::new(node))
            }
            NodeKind::ShuffleWrite(gen::ShuffleWriteExecNode {
                stage,
                plan,
                partitioning,
                consumption,
                locations,
            }) => {
                let plan = self.try_decode_plan(&plan, registry)?;
                let partitioning =
                    self.try_decode_partitioning(&partitioning, registry, &plan.schema())?;
                let consumption = self.try_decode_shuffle_consumption(consumption)?;
                let locations = locations
                    .into_iter()
                    .map(|x| self.try_decode_task_write_location_list(x))
                    .collect::<Result<_>>()?;
                let node = ShuffleWriteExec::new(stage as usize, plan, partitioning, consumption);
                let node = node.with_locations(locations);
                Ok(Arc::new(node))
            }
            NodeKind::Memory(gen::MemoryExecNode {
                partitions,
                schema,
                projection,
                show_sizes,
                sort_information,
            }) => {
                let schema = self.try_decode_schema(&schema)?;
                let partitions = partitions
                    .into_iter()
                    .map(|x| read_record_batches(&x))
                    .collect::<Result<Vec<_>>>()?;
                let projection =
                    projection.map(|x| x.columns.into_iter().map(|c| c as usize).collect());
                let sort_information =
                    self.try_decode_lex_orderings(&sort_information, registry, &schema)?;
                Ok(Arc::new(
                    MemoryExec::try_new(&partitions, Arc::new(schema), projection)?
                        .with_show_sizes(show_sizes)
                        .try_with_sort_information(sort_information)?,
                ))
            }
            NodeKind::Values(gen::ValuesExecNode { data, schema }) => {
                let schema = self.try_decode_schema(&schema)?;
                let data = read_record_batches(&data)?;
                Ok(Arc::new(ValuesExec::try_new_from_batches(
                    Arc::new(schema),
                    data,
                )?))
            }
            NodeKind::NdJson(gen::NdJsonExecNode {
                base_config,
                file_compression_type,
            }) => {
                let base_config = parse_protobuf_file_scan_config(
                    &self.try_decode_message(&base_config)?,
                    registry,
                    self,
                )?;
                let file_compression_type: FileCompressionType =
                    self.try_decode_file_compression_type(file_compression_type)?;
                Ok(Arc::new(NdJsonExec::new(
                    base_config,
                    file_compression_type,
                )))
            }
            NodeKind::Arrow(gen::ArrowExecNode { base_config }) => {
                let base_config = parse_protobuf_file_scan_config(
                    &self.try_decode_message(&base_config)?,
                    registry,
                    self,
                )?;
                Ok(Arc::new(ArrowExec::new(base_config)))
            }
            NodeKind::WorkTable(gen::WorkTableExecNode { name, schema }) => {
                let schema = self.try_decode_schema(&schema)?;
                Ok(Arc::new(WorkTableExec::new(name, Arc::new(schema))))
            }
            NodeKind::RecursiveQuery(gen::RecursiveQueryExecNode {
                name,
                static_term,
                recursive_term,
                is_distinct,
            }) => {
                let static_term = self.try_decode_plan(&static_term, registry)?;
                let recursive_term = self.try_decode_plan(&recursive_term, registry)?;
                Ok(Arc::new(RecursiveQueryExec::try_new(
                    name,
                    static_term,
                    recursive_term,
                    is_distinct,
                )?))
            }
            NodeKind::SortMergeJoin(gen::SortMergeJoinExecNode {
                left,
                right,
                on,
                filter,
                join_type,
                sort_options,
                null_equals_null,
            }) => {
                let left = self.try_decode_plan(&left, registry)?;
                let right = self.try_decode_plan(&right, registry)?;
                let on = on
                    .into_iter()
                    .map(|join_on| {
                        let left = parse_physical_expr(
                            &self.try_decode_message(&join_on.left)?,
                            registry,
                            &left.schema(),
                            self,
                        )?;
                        let right = parse_physical_expr(
                            &self.try_decode_message(&join_on.right)?,
                            registry,
                            &right.schema(),
                            self,
                        )?;
                        Ok((left, right))
                    })
                    .collect::<Result<_>>()?;
                let filter = if let Some(join_filter) = filter {
                    let schema = self.try_decode_schema(&join_filter.schema)?;
                    let expression = parse_physical_expr(
                        &self.try_decode_message(&join_filter.expression)?,
                        registry,
                        &schema,
                        self,
                    )?;
                    let column_indices = join_filter
                        .column_indices
                        .into_iter()
                        .map(|idx| {
                            let side = gen_datafusion_common::JoinSide::from_str_name(&idx.side)
                                .ok_or_else(|| {
                                    plan_datafusion_err!("invalid join side: {}", idx.side)
                                })?;
                            let side: JoinSide = side.into();
                            Ok(ColumnIndex {
                                index: idx.index as usize,
                                side,
                            })
                        })
                        .collect::<Result<Vec<_>>>()?;
                    Some(JoinFilter::new(expression, column_indices, schema))
                } else {
                    None
                };
                let join_type = ProtoJoinType::from_str_name(&join_type)
                    .ok_or_else(|| plan_datafusion_err!("invalid join type: {}", join_type))?;
                let join_type: datafusion::common::JoinType = join_type.into();
                let sort_options: Vec<SortOptions> = sort_options
                    .into_iter()
                    .map(|opt| SortOptions {
                        descending: opt.descending,
                        nulls_first: opt.nulls_first,
                    })
                    .collect();
                Ok(Arc::new(SortMergeJoinExec::try_new(
                    left,
                    right,
                    on,
                    filter,
                    join_type,
                    sort_options,
                    null_equals_null,
                )?))
            }
            // TODO: StreamingTableExec?
            _ => plan_err!("unsupported physical plan node: {node_kind:?}"),
        }
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
        let node_kind = if let Some(range) = node.as_any().downcast_ref::<RangeExec>() {
            let schema = self.try_encode_schema(range.schema().as_ref())?;
            NodeKind::Range(gen::RangeExecNode {
                start: range.range().start,
                end: range.range().end,
                step: range.range().step,
                num_partitions: range.num_partitions() as u64,
                schema,
            })
        } else if let Some(show_string) = node.as_any().downcast_ref::<ShowStringExec>() {
            let schema = self.try_encode_schema(show_string.schema().as_ref())?;
            NodeKind::ShowString(gen::ShowStringExecNode {
                input: self.try_encode_plan(show_string.input().clone())?,
                names: show_string.names().to_vec(),
                limit: show_string.limit() as u64,
                style: self.try_encode_show_string_style(show_string.format().style())?,
                truncate: show_string.format().truncate() as u64,
                schema,
            })
        } else if let Some(schema_pivot) = node.as_any().downcast_ref::<SchemaPivotExec>() {
            let schema = self.try_encode_schema(schema_pivot.schema().as_ref())?;
            NodeKind::SchemaPivot(gen::SchemaPivotExecNode {
                input: self.try_encode_plan(schema_pivot.input().clone())?,
                names: schema_pivot.names().to_vec(),
                schema,
            })
        } else if let Some(map_partitions) = node.as_any().downcast_ref::<MapPartitionsExec>() {
            let udf = self.try_encode_stream_udf(map_partitions.udf().as_ref())?;
            let schema = self.try_encode_schema(map_partitions.schema().as_ref())?;
            NodeKind::MapPartitions(gen::MapPartitionsExecNode {
                input: self.try_encode_plan(map_partitions.input().clone())?,
                udf: Some(udf),
                schema,
            })
        } else if let Some(shuffle_read) = node.as_any().downcast_ref::<ShuffleReadExec>() {
            let schema = self.try_encode_schema(shuffle_read.schema().as_ref())?;
            let partitioning = self.try_encode_partitioning(shuffle_read.partitioning())?;
            let locations = shuffle_read
                .locations()
                .iter()
                .map(|x| self.try_encode_task_read_location_list(x))
                .collect::<Result<_>>()?;
            NodeKind::ShuffleRead(gen::ShuffleReadExecNode {
                stage: shuffle_read.stage() as u64,
                schema,
                partitioning,
                locations,
            })
        } else if let Some(shuffle_write) = node.as_any().downcast_ref::<ShuffleWriteExec>() {
            let plan = self.try_encode_plan(shuffle_write.plan().clone())?;
            let partitioning =
                self.try_encode_partitioning(shuffle_write.shuffle_partitioning())?;
            let consumption = self.try_encode_shuffle_consumption(shuffle_write.consumption())?;
            let locations = shuffle_write
                .locations()
                .iter()
                .map(|x| self.try_encode_task_write_location_list(x))
                .collect::<Result<_>>()?;
            NodeKind::ShuffleWrite(gen::ShuffleWriteExecNode {
                stage: shuffle_write.stage() as u64,
                plan,
                partitioning,
                consumption,
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
            let schema = self.try_encode_schema(schema.as_ref())?;
            let sort_information = self.try_encode_lex_orderings(memory.sort_information())?;
            NodeKind::Memory(gen::MemoryExecNode {
                partitions,
                schema,
                projection,
                show_sizes: memory.show_sizes(),
                sort_information,
            })
        } else if let Some(values) = node.as_any().downcast_ref::<ValuesExec>() {
            let data = write_record_batches(&values.data(), &values.schema())?;
            let schema = self.try_encode_schema(values.schema().as_ref())?;
            NodeKind::Values(gen::ValuesExecNode { data, schema })
        } else if let Some(nd_json) = node.as_any().downcast_ref::<NdJsonExec>() {
            let base_config =
                self.try_encode_message(serialize_file_scan_config(nd_json.base_config(), self)?)?;
            let file_compression_type =
                self.try_encode_file_compression_type(*nd_json.file_compression_type())?;
            NodeKind::NdJson(gen::NdJsonExecNode {
                base_config,
                file_compression_type,
            })
        } else if let Some(arrow) = node.as_any().downcast_ref::<ArrowExec>() {
            let base_config =
                self.try_encode_message(serialize_file_scan_config(arrow.base_config(), self)?)?;
            NodeKind::Arrow(gen::ArrowExecNode { base_config })
        } else if let Some(work_table) = node.as_any().downcast_ref::<WorkTableExec>() {
            let name = work_table.name().to_string();
            let schema = self.try_encode_schema(work_table.schema().as_ref())?;
            NodeKind::WorkTable(gen::WorkTableExecNode { name, schema })
        } else if let Some(recursive_query) = node.as_any().downcast_ref::<RecursiveQueryExec>() {
            let name = recursive_query.name().to_string();
            let static_term = self.try_encode_plan(recursive_query.static_term().clone())?;
            let recursive_term = self.try_encode_plan(recursive_query.recursive_term().clone())?;
            let is_distinct = recursive_query.is_distinct();
            NodeKind::RecursiveQuery(gen::RecursiveQueryExecNode {
                name,
                static_term,
                recursive_term,
                is_distinct,
            })
        } else if let Some(sort_merge_join) = node.as_any().downcast_ref::<SortMergeJoinExec>() {
            let left = self.try_encode_plan(sort_merge_join.left().clone())?;
            let right = self.try_encode_plan(sort_merge_join.right().clone())?;
            let on: Vec<gen::JoinOn> = sort_merge_join
                .on()
                .iter()
                .map(|(left, right)| {
                    let left = self.try_encode_message(serialize_physical_expr(left, self)?)?;
                    let right = self.try_encode_message(serialize_physical_expr(right, self)?)?;
                    Ok(gen::JoinOn { left, right })
                })
                .collect::<Result<_>>()?;
            let filter = sort_merge_join
                .filter()
                .as_ref()
                .map(|join_filter| {
                    let expression = self.try_encode_message(serialize_physical_expr(
                        join_filter.expression(),
                        self,
                    )?)?;
                    let column_indices = join_filter
                        .column_indices()
                        .iter()
                        .map(|i| {
                            let index = i.index as u32;
                            let side: gen_datafusion_common::JoinSide = i.side.into();
                            let side = side.as_str_name().to_string();
                            gen::ColumnIndex { index, side }
                        })
                        .collect();
                    let schema = self.try_encode_schema(join_filter.schema())?;
                    Ok(gen::JoinFilter {
                        expression,
                        column_indices,
                        schema,
                    })
                })
                .map_or(Ok(None), |v: Result<gen::JoinFilter>| v.map(Some))?;
            let join_type: ProtoJoinType = sort_merge_join.join_type().into();
            let join_type = join_type.as_str_name().to_string();
            let sort_options = sort_merge_join
                .sort_options()
                .iter()
                .map(|x| gen::SortOptions {
                    descending: x.descending,
                    nulls_first: x.nulls_first,
                })
                .collect();
            NodeKind::SortMergeJoin(gen::SortMergeJoinExecNode {
                left,
                right,
                on,
                filter,
                join_type,
                sort_options,
                null_equals_null: sort_merge_join.null_equals_null(),
            })
        } else if let Some(partial_sort) = node.as_any().downcast_ref::<PartialSortExec>() {
            let expr = Some(self.try_encode_lex_ordering(partial_sort.expr())?);
            let input = self.try_encode_plan(partial_sort.input().clone())?;
            let common_prefix_length = partial_sort.common_prefix_length() as u64;
            NodeKind::PartialSort(gen::PartialSortExecNode {
                expr,
                input,
                common_prefix_length,
            })
        } else {
            return plan_err!("unsupported physical plan node: {node:?}");
        };
        // TODO: StreamingTableExec?
        let node = ExtendedPhysicalPlanNode {
            node_kind: Some(node_kind),
        };
        node.encode(buf)
            .map_err(|e| plan_datafusion_err!("failed to encode plan: {e}"))
    }

    fn try_decode_udf(&self, name: &str, buf: &[u8]) -> Result<Arc<ScalarUDF>> {
        // TODO: Implement custom registry to avoid codec for built-in functions
        let udf = ExtendedScalarUdf::decode(buf)
            .map_err(|e| plan_datafusion_err!("failed to decode udf: {e}"))?;
        let ExtendedScalarUdf { udf_kind } = udf;
        let udf_kind = match udf_kind {
            Some(x) => x,
            None => return plan_err!("ExtendedScalarUdf: no UDF found for {name}"),
        };
        match udf_kind {
            UdfKind::Standard(gen::StandardUdf {}) => {}
            UdfKind::PySpark(gen::PySparkUdf {
                kind,
                name,
                payload,
                deterministic,
                input_types,
                output_type,
                config,
            }) => {
                let kind = self.try_decode_pyspark_udf_kind(kind)?;
                let input_types = input_types
                    .iter()
                    .map(|x| self.try_decode_data_type(x))
                    .collect::<Result<Vec<_>>>()?;
                let output_type = self.try_decode_data_type(&output_type)?;
                let config = match config {
                    Some(config) => self.try_decode_pyspark_udf_config(config)?,
                    None => return plan_err!("missing config for PySparkUDF"),
                };
                let udf = PySparkUDF::new(
                    kind,
                    name,
                    payload,
                    deterministic,
                    input_types,
                    output_type,
                    Arc::new(config),
                );
                return Ok(Arc::new(ScalarUDF::from(udf)));
            }
            UdfKind::PySparkCoGroupMap(gen::PySparkCoGroupMapUdf {
                name,
                payload,
                deterministic,
                left_types,
                left_names,
                right_types,
                right_names,
                output_type,
                config,
            }) => {
                let left_types = left_types
                    .iter()
                    .map(|x| self.try_decode_data_type(x))
                    .collect::<Result<Vec<_>>>()?;
                let right_types = right_types
                    .iter()
                    .map(|x| self.try_decode_data_type(x))
                    .collect::<Result<Vec<_>>>()?;
                let output_type = self.try_decode_data_type(&output_type)?;
                let config = match config {
                    Some(config) => self.try_decode_pyspark_udf_config(config)?,
                    None => return plan_err!("missing config for PySparkCoGroupMapUDF"),
                };
                let udf = PySparkCoGroupMapUDF::try_new(
                    name,
                    payload,
                    deterministic,
                    left_types,
                    left_names,
                    right_types,
                    right_names,
                    output_type,
                    Arc::new(config),
                )?;
                return Ok(Arc::new(ScalarUDF::from(udf)));
            }
            UdfKind::MapToArray(gen::MapToArrayUdf { nullable }) => {
                let udf = MapToArray::new(nullable);
                return Ok(Arc::new(ScalarUDF::from(udf)));
            }
            UdfKind::DropStructField(gen::DropStructFieldUdf { field_names }) => {
                let udf = DropStructField::new(field_names);
                return Ok(Arc::new(ScalarUDF::from(udf)));
            }
            UdfKind::Explode(gen::ExplodeUdf { name }) => {
                let kind = explode_name_to_kind(&name)?;
                let udf = Explode::new(kind);
                return Ok(Arc::new(ScalarUDF::from(udf)));
            }
            UdfKind::SparkUnixTimestamp(gen::SparkUnixTimestampUdf { timezone }) => {
                let udf = SparkUnixTimestamp::new(Arc::from(timezone));
                return Ok(Arc::new(ScalarUDF::from(udf)));
            }
            UdfKind::StructFunction(gen::StructFunctionUdf { field_names }) => {
                let udf = StructFunction::new(field_names);
                return Ok(Arc::new(ScalarUDF::from(udf)));
            }
            UdfKind::UpdateStructField(gen::UpdateStructFieldUdf { field_names }) => {
                let udf = UpdateStructField::new(field_names);
                return Ok(Arc::new(ScalarUDF::from(udf)));
            }
            UdfKind::SparkWeekOfYear(gen::SparkWeekOfYearUdf { timezone }) => {
                let udf = SparkWeekOfYear::new(Arc::from(timezone));
                return Ok(Arc::new(ScalarUDF::from(udf)));
            }
            UdfKind::TimestampNow(gen::TimestampNowUdf {
                timezone,
                time_unit,
            }) => {
                let time_unit = gen_datafusion_common::TimeUnit::from_str_name(time_unit.as_str())
                    .ok_or_else(|| plan_datafusion_err!("invalid time unit: {time_unit}"))?;
                let time_unit: TimeUnit = time_unit.into();
                let udf = TimestampNow::new(Arc::from(timezone), time_unit);
                return Ok(Arc::new(ScalarUDF::from(udf)));
            }
        };
        match name {
            "array_item_with_position" => {
                Ok(Arc::new(ScalarUDF::from(ArrayItemWithPosition::new())))
            }
            "array_empty_to_null" => Ok(Arc::new(ScalarUDF::from(ArrayEmptyToNull::new()))),
            "array_min" => Ok(Arc::new(ScalarUDF::from(ArrayMin::new()))),
            "array_max" => Ok(Arc::new(ScalarUDF::from(ArrayMax::new()))),
            "greatest" => Ok(Arc::new(ScalarUDF::from(Greatest::new()))),
            "least" => Ok(Arc::new(ScalarUDF::from(Least::new()))),
            "levenshtein" => Ok(Arc::new(ScalarUDF::from(Levenshtein::new()))),
            "map" => Ok(Arc::new(ScalarUDF::from(MapFunction::new()))),
            "multi_expr" => Ok(Arc::new(ScalarUDF::from(MultiExpr::new()))),
            "raise_error" => Ok(Arc::new(ScalarUDF::from(RaiseError::new()))),
            "randn" => Ok(Arc::new(ScalarUDF::from(Randn::new()))),
            "random" | "rand" => Ok(Arc::new(ScalarUDF::from(Random::new()))),
            "size" | "cardinality" => Ok(Arc::new(ScalarUDF::from(Size::new()))),
            "spark_array" | "spark_make_array" | "array" => {
                Ok(Arc::new(ScalarUDF::from(SparkArray::new())))
            }
            "spark_concat" | "concat" => Ok(Arc::new(ScalarUDF::from(SparkConcat::new()))),
            "spark_hex" | "hex" => Ok(Arc::new(ScalarUDF::from(SparkHex::new()))),
            "spark_unhex" | "unhex" => Ok(Arc::new(ScalarUDF::from(SparkUnHex::new()))),
            "spark_murmur3_hash" | "hash" => Ok(Arc::new(ScalarUDF::from(SparkMurmur3Hash::new()))),
            "spark_reverse" | "reverse" => Ok(Arc::new(ScalarUDF::from(SparkReverse::new()))),
            "spark_xxhash64" | "xxhash64" => Ok(Arc::new(ScalarUDF::from(SparkXxhash64::new()))),
            "overlay" => Ok(Arc::new(ScalarUDF::from(OverlayFunc::new()))),
            "json_length" | "json_len" => Ok(datafusion_functions_json::udfs::json_length_udf()),
            "json_as_text" => Ok(datafusion_functions_json::udfs::json_as_text_udf()),
            "spark_base64" | "base64" => Ok(Arc::new(ScalarUDF::from(SparkBase64::new()))),
            "spark_unbase64" | "unbase64" => Ok(Arc::new(ScalarUDF::from(SparkUnbase64::new()))),
            "spark_aes_encrypt" | "aes_encrypt" => {
                Ok(Arc::new(ScalarUDF::from(SparkAESEncrypt::new())))
            }
            "spark_try_aes_encrypt" | "try_aes_encrypt" => {
                Ok(Arc::new(ScalarUDF::from(SparkTryAESEncrypt::new())))
            }
            "spark_aes_decrypt" | "aes_decrypt" => {
                Ok(Arc::new(ScalarUDF::from(SparkAESDecrypt::new())))
            }
            "spark_try_aes_decrypt" | "try_aes_decrypt" => {
                Ok(Arc::new(ScalarUDF::from(SparkTryAESDecrypt::new())))
            }
            _ => plan_err!("could not find scalar function: {name}"),
        }
    }

    fn try_encode_udf(&self, node: &ScalarUDF, buf: &mut Vec<u8>) -> Result<()> {
        // TODO: Implement custom registry to avoid codec for built-in functions
        let udf_kind = if node.inner().as_any().is::<ArrayItemWithPosition>()
            || node.inner().as_any().is::<ArrayEmptyToNull>()
            || node.inner().as_any().is::<ArrayMin>()
            || node.inner().as_any().is::<ArrayMax>()
            || node.inner().as_any().is::<Greatest>()
            || node.inner().as_any().is::<Least>()
            || node.inner().as_any().is::<Levenshtein>()
            || node.inner().as_any().is::<MapFunction>()
            || node.inner().as_any().is::<MultiExpr>()
            || node.inner().as_any().is::<RaiseError>()
            || node.inner().as_any().is::<Randn>()
            || node.inner().as_any().is::<Random>()
            || node.inner().as_any().is::<Size>()
            || node.inner().as_any().is::<SparkArray>()
            || node.inner().as_any().is::<SparkConcat>()
            || node.inner().as_any().is::<SparkHex>()
            || node.inner().as_any().is::<SparkUnHex>()
            || node.inner().as_any().is::<SparkMurmur3Hash>()
            || node.inner().as_any().is::<SparkReverse>()
            || node.inner().as_any().is::<SparkXxhash64>()
            || node.inner().as_any().is::<OverlayFunc>()
            || node.inner().as_any().is::<SparkBase64>()
            || node.inner().as_any().is::<SparkUnbase64>()
            || node.inner().as_any().is::<SparkAESEncrypt>()
            || node.inner().as_any().is::<SparkTryAESEncrypt>()
            || node.inner().as_any().is::<SparkAESDecrypt>()
            || node.inner().as_any().is::<SparkTryAESDecrypt>()
            || node.name() == "json_length"
            || node.name() == "json_len"
            || node.name() == "json_as_text"
        {
            UdfKind::Standard(gen::StandardUdf {})
        } else if let Some(func) = node.inner().as_any().downcast_ref::<PySparkUDF>() {
            let kind = self.try_encode_pyspark_udf_kind(func.kind())?;
            let input_types = func
                .input_types()
                .iter()
                .map(|x| self.try_encode_data_type(x))
                .collect::<Result<Vec<_>>>()?;
            let output_type = self.try_encode_data_type(func.output_type())?;
            let config = self.try_encode_pyspark_udf_config(func.config())?;
            UdfKind::PySpark(gen::PySparkUdf {
                kind,
                name: func.name().to_string(),
                payload: func.payload().to_vec(),
                deterministic: func.deterministic(),
                input_types,
                output_type,
                config: Some(config),
            })
        } else if let Some(func) = node.inner().as_any().downcast_ref::<PySparkCoGroupMapUDF>() {
            let left_types = func
                .left_types()
                .iter()
                .map(|x| self.try_encode_data_type(x))
                .collect::<Result<Vec<_>>>()?;
            let right_types = func
                .right_types()
                .iter()
                .map(|x| self.try_encode_data_type(x))
                .collect::<Result<Vec<_>>>()?;
            let output_type = self.try_encode_data_type(func.output_type())?;
            let config = self.try_encode_pyspark_udf_config(func.config())?;
            UdfKind::PySparkCoGroupMap(gen::PySparkCoGroupMapUdf {
                name: func.name().to_string(),
                payload: func.payload().to_vec(),
                deterministic: func.deterministic(),
                left_types,
                left_names: func.left_names().to_vec(),
                right_types,
                right_names: func.right_names().to_vec(),
                output_type,
                config: Some(config),
            })
        } else if let Some(func) = node.inner().as_any().downcast_ref::<MapToArray>() {
            let nullable = func.nullable();
            UdfKind::MapToArray(gen::MapToArrayUdf { nullable })
        } else if let Some(func) = node.inner().as_any().downcast_ref::<DropStructField>() {
            let field_names = func.field_names().to_vec();
            UdfKind::DropStructField(gen::DropStructFieldUdf { field_names })
        } else if let Some(_func) = node.inner().as_any().downcast_ref::<Explode>() {
            let name = node.name().to_string();
            UdfKind::Explode(gen::ExplodeUdf { name })
        } else if let Some(func) = node.inner().as_any().downcast_ref::<SparkUnixTimestamp>() {
            let timezone = func.timezone().to_string();
            UdfKind::SparkUnixTimestamp(gen::SparkUnixTimestampUdf { timezone })
        } else if let Some(func) = node.inner().as_any().downcast_ref::<StructFunction>() {
            let field_names = func.field_names().to_vec();
            UdfKind::StructFunction(gen::StructFunctionUdf { field_names })
        } else if let Some(func) = node.inner().as_any().downcast_ref::<UpdateStructField>() {
            let field_names = func.field_names().to_vec();
            UdfKind::UpdateStructField(gen::UpdateStructFieldUdf { field_names })
        } else if let Some(func) = node.inner().as_any().downcast_ref::<SparkWeekOfYear>() {
            let timezone = func.timezone().to_string();
            UdfKind::SparkWeekOfYear(gen::SparkWeekOfYearUdf { timezone })
        } else if let Some(func) = node.inner().as_any().downcast_ref::<TimestampNow>() {
            let timezone = func.timezone().to_string();
            let time_unit: gen_datafusion_common::TimeUnit = func.time_unit().into();
            let time_unit = time_unit.as_str_name().to_string();
            UdfKind::TimestampNow(gen::TimestampNowUdf {
                timezone,
                time_unit,
            })
        } else {
            return Ok(());
        };
        let node = ExtendedScalarUdf {
            udf_kind: Some(udf_kind),
        };
        node.encode(buf)
            .map_err(|e| plan_datafusion_err!("failed to encode udf: {e}"))
    }

    fn try_decode_udaf(&self, name: &str, buf: &[u8]) -> Result<Arc<AggregateUDF>> {
        let udaf = ExtendedAggregateUdf::decode(buf)
            .map_err(|e| plan_datafusion_err!("failed to decode udaf: {e}"))?;
        let ExtendedAggregateUdf { udaf_kind } = udaf;
        match udaf_kind {
            Some(UdafKind::Standard(gen::StandardUdaf {})) => match name {
                "kurtosis" => Ok(Arc::new(AggregateUDF::from(KurtosisFunction::new()))),
                "max_by" => Ok(Arc::new(AggregateUDF::from(MaxByFunction::new()))),
                "min_by" => Ok(Arc::new(AggregateUDF::from(MinByFunction::new()))),
                "mode" => Ok(Arc::new(AggregateUDF::from(ModeFunction::new()))),
                "skewness" => Ok(Arc::new(AggregateUDF::from(SkewnessFunc::new()))),
                _ => plan_err!("Could not find Aggregate Function: {name}"),
            },
            Some(UdafKind::PySparkGroupAgg(gen::PySparkGroupAggUdaf {
                name,
                payload,
                deterministic,
                input_names,
                input_types,
                output_type,
                config,
            })) => {
                let input_types = input_types
                    .iter()
                    .map(|x| self.try_decode_data_type(x))
                    .collect::<Result<Vec<_>>>()?;
                let output_type = self.try_decode_data_type(&output_type)?;
                let config = match config {
                    Some(config) => self.try_decode_pyspark_udf_config(config)?,
                    None => return plan_err!("missing config for PySparkGroupAggUDF"),
                };
                let udaf = PySparkGroupAggregateUDF::new(
                    name,
                    payload,
                    deterministic,
                    input_names,
                    input_types,
                    output_type,
                    Arc::new(config),
                );
                Ok(Arc::new(AggregateUDF::from(udaf)))
            }
            Some(UdafKind::PySparkGroupMap(gen::PySparkGroupMapUdaf {
                name,
                payload,
                deterministic,
                input_names,
                input_types,
                output_type,
                config,
            })) => {
                let input_types = input_types
                    .iter()
                    .map(|x| self.try_decode_data_type(x))
                    .collect::<Result<Vec<_>>>()?;
                let output_type = self.try_decode_data_type(&output_type)?;
                let config = match config {
                    Some(config) => self.try_decode_pyspark_udf_config(config)?,
                    None => return plan_err!("missing config for PySparkGroupMapUDF"),
                };
                let udaf = PySparkGroupMapUDF::new(
                    name,
                    payload,
                    deterministic,
                    input_names,
                    input_types,
                    output_type,
                    Arc::new(config),
                );
                Ok(Arc::new(AggregateUDF::from(udaf)))
            }
            Some(UdafKind::PySparkBatchCollector(gen::PySparkBatchCollectorUdaf {
                input_types,
                input_names,
            })) => {
                let input_types = input_types
                    .iter()
                    .map(|x| self.try_decode_data_type(x))
                    .collect::<Result<Vec<_>>>()?;
                let udaf = PySparkBatchCollectorUDF::new(input_types, input_names);
                Ok(Arc::new(AggregateUDF::from(udaf)))
            }
            None => plan_err!("ExtendedScalarUdf: no UDF found for {name}"),
        }
    }

    fn try_encode_udaf(&self, node: &AggregateUDF, buf: &mut Vec<u8>) -> Result<()> {
        let udaf_kind = if node.inner().as_any().is::<KurtosisFunction>()
            || node.inner().as_any().is::<MaxByFunction>()
            || node.inner().as_any().is::<MinByFunction>()
            || node.inner().as_any().is::<ModeFunction>()
            || node.inner().as_any().is::<SkewnessFunc>()
        {
            UdafKind::Standard(gen::StandardUdaf {})
        } else if let Some(func) = node
            .inner()
            .as_any()
            .downcast_ref::<PySparkGroupAggregateUDF>()
        {
            let input_types = func
                .input_types()
                .iter()
                .map(|x| self.try_encode_data_type(x))
                .collect::<Result<Vec<_>>>()?;
            let output_type = self.try_encode_data_type(func.output_type())?;
            let config = self.try_encode_pyspark_udf_config(func.config())?;
            UdafKind::PySparkGroupAgg(gen::PySparkGroupAggUdaf {
                name: func.name().to_string(),
                payload: func.payload().to_vec(),
                deterministic: func.deterministic(),
                input_names: func.input_names().to_vec(),
                input_types,
                output_type,
                config: Some(config),
            })
        } else if let Some(func) = node.inner().as_any().downcast_ref::<PySparkGroupMapUDF>() {
            let input_types = func
                .input_types()
                .iter()
                .map(|x| self.try_encode_data_type(x))
                .collect::<Result<Vec<_>>>()?;
            let output_type = self.try_encode_data_type(func.output_type())?;
            let config = self.try_encode_pyspark_udf_config(func.config())?;
            UdafKind::PySparkGroupMap(gen::PySparkGroupMapUdaf {
                name: func.name().to_string(),
                payload: func.payload().to_vec(),
                deterministic: func.deterministic(),
                input_names: func.input_names().to_vec(),
                input_types,
                output_type,
                config: Some(config),
            })
        } else if let Some(func) = node
            .inner()
            .as_any()
            .downcast_ref::<PySparkBatchCollectorUDF>()
        {
            let input_types = func
                .input_types()
                .iter()
                .map(|x| self.try_encode_data_type(x))
                .collect::<Result<Vec<_>>>()?;
            UdafKind::PySparkBatchCollector(gen::PySparkBatchCollectorUdaf {
                input_types,
                input_names: func.input_names().to_vec(),
            })
        } else {
            return Ok(());
        };
        let node = ExtendedAggregateUdf {
            udaf_kind: Some(udaf_kind),
        };
        node.encode(buf)
            .map_err(|e| plan_datafusion_err!("failed to encode udaf: {e}"))
    }
}

impl RemoteExecutionCodec {
    pub fn new(context: SessionContext) -> Self {
        Self { context }
    }

    fn try_decode_stream_udf(&self, udf: ExtendedStreamUdf) -> Result<Arc<dyn StreamUDF>> {
        let ExtendedStreamUdf { stream_udf_kind } = udf;
        let stream_udf_kind = match stream_udf_kind {
            Some(x) => x,
            None => return plan_err!("ExtendedStreamUdf: no UDF found"),
        };
        let udf: Arc<dyn StreamUDF> = match stream_udf_kind {
            StreamUdfKind::PySparkMapIter(gen::PySparkMapIterUdf {
                kind,
                name,
                payload,
                input_names,
                output_schema,
                config,
            }) => {
                let kind = self.try_decode_pyspark_map_iter_kind(kind)?;
                let output_schema = self.try_decode_schema(&output_schema)?;
                let config = match config {
                    Some(config) => self.try_decode_pyspark_udf_config(config)?,
                    None => return plan_err!("missing config for PySparkMapIterUDF"),
                };
                Arc::new(PySparkMapIterUDF::new(
                    kind,
                    name,
                    payload,
                    input_names,
                    Arc::new(output_schema),
                    Arc::new(config),
                ))
            }
            StreamUdfKind::PySparkUdtf(gen::PySparkUdtf {
                kind,
                name,
                payload,
                input_names,
                input_types,
                passthrough_columns,
                function_return_type,
                function_output_names,
                deterministic,
                config,
            }) => {
                let kind = self.try_decode_pyspark_udtf_kind(kind)?;
                let input_types = input_types
                    .iter()
                    .map(|x| self.try_decode_data_type(x))
                    .collect::<Result<Vec<_>>>()?;
                let function_return_type = self.try_decode_data_type(&function_return_type)?;
                let function_output_names = if function_output_names.is_empty() {
                    None
                } else {
                    Some(function_output_names)
                };
                let config = match config {
                    Some(config) => self.try_decode_pyspark_udf_config(config)?,
                    None => return plan_err!("missing config for PySparkUdtf"),
                };
                Arc::new(PySparkUDTF::try_new(
                    kind,
                    name,
                    payload,
                    input_names,
                    input_types,
                    passthrough_columns as usize,
                    function_return_type,
                    function_output_names,
                    deterministic,
                    Arc::new(config),
                )?)
            }
        };
        Ok(udf)
    }

    fn try_encode_stream_udf(&self, udf: &dyn StreamUDF) -> Result<ExtendedStreamUdf> {
        let stream_udf_kind = if let Some(func) =
            udf.dyn_object_as_any().downcast_ref::<PySparkMapIterUDF>()
        {
            let kind = self.try_encode_pyspark_map_iter_kind(func.kind())?;
            let output_schema = self.try_encode_schema(func.output_schema().as_ref())?;
            let config = self.try_encode_pyspark_udf_config(func.config())?;
            StreamUdfKind::PySparkMapIter(gen::PySparkMapIterUdf {
                kind,
                name: func.name().to_string(),
                payload: func.payload().to_vec(),
                input_names: func.input_names().to_vec(),
                output_schema,
                config: Some(config),
            })
        } else if let Some(func) = udf.dyn_object_as_any().downcast_ref::<PySparkUDTF>() {
            let kind = self.try_encode_pyspark_udtf_kind(func.kind())?;
            let input_types = func
                .input_types()
                .iter()
                .map(|x| self.try_encode_data_type(x))
                .collect::<Result<Vec<_>>>()?;
            let function_return_type = self.try_encode_data_type(func.function_return_type())?;
            let function_output_names = func
                .function_output_names()
                .as_ref()
                .map(|x| x.to_vec())
                .unwrap_or_default();
            let config = self.try_encode_pyspark_udf_config(func.config())?;
            StreamUdfKind::PySparkUdtf(gen::PySparkUdtf {
                kind,
                name: func.name().to_string(),
                payload: func.payload().to_vec(),
                input_names: func.input_names().to_vec(),
                input_types,
                passthrough_columns: func.passthrough_columns() as u64,
                function_return_type,
                function_output_names,
                deterministic: func.deterministic(),
                config: Some(config),
            })
        } else {
            return plan_err!("unknown StreamUDF type");
        };
        Ok(ExtendedStreamUdf {
            stream_udf_kind: Some(stream_udf_kind),
        })
    }

    fn try_decode_lex_ordering(
        &self,
        lex_ordering: &gen::LexOrdering,
        registry: &dyn FunctionRegistry,
        schema: &Schema,
    ) -> Result<LexOrdering> {
        let lex_ordering: Vec<PhysicalSortExprNode> = lex_ordering
            .values
            .iter()
            .map(|x| self.try_decode_message(x))
            .collect::<Result<_>>()?;
        parse_physical_sort_exprs(&lex_ordering, registry, schema, self)
    }

    fn try_encode_lex_ordering(&self, lex_ordering: &LexOrdering) -> Result<gen::LexOrdering> {
        let lex_ordering = serialize_physical_sort_exprs(lex_ordering.to_vec(), self)?;
        let lex_ordering = lex_ordering
            .into_iter()
            .map(|x| self.try_encode_message(x))
            .collect::<Result<_>>()?;
        Ok(gen::LexOrdering {
            values: lex_ordering,
        })
    }

    fn try_decode_lex_orderings(
        &self,
        lex_orderings: &[gen::LexOrdering],
        registry: &dyn FunctionRegistry,
        schema: &Schema,
    ) -> Result<Vec<LexOrdering>> {
        let mut result: Vec<LexOrdering> = vec![];
        for lex_ordering in lex_orderings {
            let lex_ordering = self.try_decode_lex_ordering(lex_ordering, registry, schema)?;
            result.push(lex_ordering);
        }
        Ok(result)
    }

    fn try_encode_lex_orderings(
        &self,
        lex_orderings: &[LexOrdering],
    ) -> Result<Vec<gen::LexOrdering>> {
        let mut result = vec![];
        for lex_ordering in lex_orderings {
            let lex_ordering = self.try_encode_lex_ordering(lex_ordering)?;
            result.push(lex_ordering)
        }
        Ok(result)
    }

    fn try_decode_show_string_style(&self, style: i32) -> Result<ShowStringStyle> {
        let style = gen::ShowStringStyle::try_from(style)
            .map_err(|e| plan_datafusion_err!("failed to decode style: {e}"))?;
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

    fn try_decode_pyspark_udf_kind(&self, kind: i32) -> Result<PySparkUdfKind> {
        let kind = gen::PySparkUdfKind::try_from(kind)
            .map_err(|e| plan_datafusion_err!("failed to decode pyspark UDF kind: {e}"))?;
        let kind = match kind {
            gen::PySparkUdfKind::Batch => PySparkUdfKind::Batch,
            gen::PySparkUdfKind::ArrowBatch => PySparkUdfKind::ArrowBatch,
            gen::PySparkUdfKind::ScalarPandas => PySparkUdfKind::ScalarPandas,
            gen::PySparkUdfKind::ScalarPandasIter => PySparkUdfKind::ScalarPandasIter,
        };
        Ok(kind)
    }

    fn try_encode_pyspark_udf_kind(&self, kind: PySparkUdfKind) -> Result<i32> {
        let kind = match kind {
            PySparkUdfKind::Batch => gen::PySparkUdfKind::Batch,
            PySparkUdfKind::ArrowBatch => gen::PySparkUdfKind::ArrowBatch,
            PySparkUdfKind::ScalarPandas => gen::PySparkUdfKind::ScalarPandas,
            PySparkUdfKind::ScalarPandasIter => gen::PySparkUdfKind::ScalarPandasIter,
        };
        Ok(kind as i32)
    }

    fn try_decode_pyspark_map_iter_kind(&self, kind: i32) -> Result<PySparkMapIterKind> {
        let kind = gen::PySparkMapIterKind::try_from(kind)
            .map_err(|e| plan_datafusion_err!("failed to decode pyspark map iter kind: {e}"))?;
        let kind = match kind {
            gen::PySparkMapIterKind::Arrow => PySparkMapIterKind::Arrow,
            gen::PySparkMapIterKind::Pandas => PySparkMapIterKind::Pandas,
        };
        Ok(kind)
    }

    fn try_encode_pyspark_map_iter_kind(&self, kind: PySparkMapIterKind) -> Result<i32> {
        let kind = match kind {
            PySparkMapIterKind::Arrow => gen::PySparkMapIterKind::Arrow,
            PySparkMapIterKind::Pandas => gen::PySparkMapIterKind::Pandas,
        };
        Ok(kind as i32)
    }

    fn try_decode_pyspark_udtf_kind(&self, kind: i32) -> Result<PySparkUdtfKind> {
        let kind = gen::PySparkUdtfKind::try_from(kind)
            .map_err(|e| plan_datafusion_err!("failed to decode pyspark UDTF kind: {e}"))?;
        let kind = match kind {
            gen::PySparkUdtfKind::Table => PySparkUdtfKind::Table,
            gen::PySparkUdtfKind::ArrowTable => PySparkUdtfKind::ArrowTable,
        };
        Ok(kind)
    }

    fn try_encode_pyspark_udtf_kind(&self, kind: PySparkUdtfKind) -> Result<i32> {
        let kind = match kind {
            PySparkUdtfKind::Table => gen::PySparkUdtfKind::Table,
            PySparkUdtfKind::ArrowTable => gen::PySparkUdtfKind::ArrowTable,
        };
        Ok(kind as i32)
    }

    fn try_decode_pyspark_udf_config(
        &self,
        config: gen::PySparkUdfConfig,
    ) -> Result<PySparkUdfConfig> {
        let config = PySparkUdfConfig {
            session_timezone: config.session_timezone,
            pandas_window_bound_types: config.pandas_window_bound_types,
            pandas_grouped_map_assign_columns_by_name: config
                .pandas_grouped_map_assign_columns_by_name,
            pandas_convert_to_arrow_array_safely: config.pandas_convert_to_arrow_array_safely,
            arrow_max_records_per_batch: config.arrow_max_records_per_batch as usize,
        };
        Ok(config)
    }

    fn try_encode_pyspark_udf_config(
        &self,
        config: &PySparkUdfConfig,
    ) -> Result<gen::PySparkUdfConfig> {
        let config = gen::PySparkUdfConfig {
            session_timezone: config.session_timezone.clone(),
            pandas_window_bound_types: config.pandas_window_bound_types.clone(),
            pandas_grouped_map_assign_columns_by_name: config
                .pandas_grouped_map_assign_columns_by_name,
            pandas_convert_to_arrow_array_safely: config.pandas_convert_to_arrow_array_safely,
            arrow_max_records_per_batch: config.arrow_max_records_per_batch as u64,
        };
        Ok(config)
    }

    fn try_decode_shuffle_consumption(&self, consumption: i32) -> Result<ShuffleConsumption> {
        let consumption = gen::ShuffleConsumption::try_from(consumption)
            .map_err(|e| plan_datafusion_err!("failed to decode shuffle consumption: {e}"))?;
        let consumption = match consumption {
            gen::ShuffleConsumption::Single => ShuffleConsumption::Single,
            gen::ShuffleConsumption::Multiple => ShuffleConsumption::Multiple,
        };
        Ok(consumption)
    }

    fn try_encode_shuffle_consumption(&self, consumption: ShuffleConsumption) -> Result<i32> {
        let consumption = match consumption {
            ShuffleConsumption::Single => gen::ShuffleConsumption::Single,
            ShuffleConsumption::Multiple => gen::ShuffleConsumption::Multiple,
        };
        Ok(consumption as i32)
    }

    fn try_decode_local_stream_storage(&self, storage: i32) -> Result<LocalStreamStorage> {
        let storage = gen::LocalStreamStorage::try_from(storage)
            .map_err(|e| plan_datafusion_err!("failed to decode local stream storage: {e}"))?;
        let storage = match storage {
            gen::LocalStreamStorage::Ephemeral => LocalStreamStorage::Ephemeral,
            gen::LocalStreamStorage::Memory => LocalStreamStorage::Memory,
            gen::LocalStreamStorage::Disk => LocalStreamStorage::Disk,
        };
        Ok(storage)
    }

    fn try_encode_local_stream_storage(&self, storage: LocalStreamStorage) -> Result<i32> {
        let storage = match storage {
            LocalStreamStorage::Ephemeral => gen::LocalStreamStorage::Ephemeral,
            LocalStreamStorage::Memory => gen::LocalStreamStorage::Memory,
            LocalStreamStorage::Disk => gen::LocalStreamStorage::Disk,
        };
        Ok(storage as i32)
    }

    fn try_decode_file_compression_type(&self, variant: i32) -> Result<FileCompressionType> {
        let variant = gen::CompressionTypeVariant::try_from(variant)
            .map_err(|e| plan_datafusion_err!("failed to decode compression type variant: {e}"))?;
        let file_compression_type = match variant {
            gen::CompressionTypeVariant::Gzip => CompressionTypeVariant::GZIP,
            gen::CompressionTypeVariant::Bzip2 => CompressionTypeVariant::BZIP2,
            gen::CompressionTypeVariant::Xz => CompressionTypeVariant::XZ,
            gen::CompressionTypeVariant::Zstd => CompressionTypeVariant::ZSTD,
            gen::CompressionTypeVariant::Uncompressed => CompressionTypeVariant::UNCOMPRESSED,
        };
        Ok(file_compression_type.into())
    }

    fn try_encode_file_compression_type(
        &self,
        file_compression_type: FileCompressionType,
    ) -> Result<i32> {
        let variant: CompressionTypeVariant = file_compression_type.into();
        let variant = match variant {
            CompressionTypeVariant::GZIP => gen::CompressionTypeVariant::Gzip,
            CompressionTypeVariant::BZIP2 => gen::CompressionTypeVariant::Bzip2,
            CompressionTypeVariant::XZ => gen::CompressionTypeVariant::Xz,
            CompressionTypeVariant::ZSTD => gen::CompressionTypeVariant::Zstd,
            CompressionTypeVariant::UNCOMPRESSED => gen::CompressionTypeVariant::Uncompressed,
        };
        Ok(variant as i32)
    }

    fn try_decode_plan(
        &self,
        buf: &[u8],
        registry: &dyn FunctionRegistry,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = PhysicalPlanNode::decode(buf)
            .map_err(|e| plan_datafusion_err!("failed to decode plan: {e}"))?;
        plan.try_into_physical_plan(registry, self.context.runtime_env().as_ref(), self)
    }

    fn try_encode_plan(&self, plan: Arc<dyn ExecutionPlan>) -> Result<Vec<u8>> {
        let plan = PhysicalPlanNode::try_from_physical_plan(plan, self)?;
        let mut buffer = BytesMut::new();
        plan.encode(&mut buffer)
            .map_err(|e| plan_datafusion_err!("failed to encode plan: {e}"))?;
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

    fn try_decode_task_read_location(
        &self,
        location: gen::TaskReadLocation,
    ) -> Result<TaskReadLocation> {
        let gen::TaskReadLocation { location } = location;
        let location = match location {
            Some(gen::task_read_location::Location::Worker(gen::TaskReadLocationWorker {
                worker_id,
                host,
                port,
                channel,
            })) => TaskReadLocation::Worker {
                worker_id: worker_id.into(),
                host,
                port: u16::try_from(port)
                    .map_err(|_| plan_datafusion_err!("invalid port: {port}"))?,
                channel: channel.into(),
            },
            Some(gen::task_read_location::Location::Remote(gen::TaskReadLocationRemote {
                uri,
            })) => TaskReadLocation::Remote { uri },
            None => return plan_err!("no shuffle read location found"),
        };
        Ok(location)
    }

    fn try_encode_task_read_location(
        &self,
        location: &TaskReadLocation,
    ) -> Result<gen::TaskReadLocation> {
        let location = match location {
            TaskReadLocation::Worker {
                worker_id,
                host,
                port,
                channel,
            } => gen::TaskReadLocation {
                location: Some(gen::task_read_location::Location::Worker(
                    gen::TaskReadLocationWorker {
                        worker_id: (*worker_id).into(),
                        host: host.clone(),
                        port: *port as u32,
                        channel: channel.clone().into(),
                    },
                )),
            },
            TaskReadLocation::Remote { uri } => gen::TaskReadLocation {
                location: Some(gen::task_read_location::Location::Remote(
                    gen::TaskReadLocationRemote { uri: uri.clone() },
                )),
            },
        };
        Ok(location)
    }

    fn try_decode_task_read_location_list(
        &self,
        locations: gen::TaskReadLocationList,
    ) -> Result<Vec<TaskReadLocation>> {
        let gen::TaskReadLocationList { locations } = locations;
        locations
            .into_iter()
            .map(|location| self.try_decode_task_read_location(location))
            .collect()
    }

    fn try_encode_task_read_location_list(
        &self,
        locations: &[TaskReadLocation],
    ) -> Result<gen::TaskReadLocationList> {
        let locations = locations
            .iter()
            .map(|location| self.try_encode_task_read_location(location))
            .collect::<Result<_>>()?;
        Ok(gen::TaskReadLocationList { locations })
    }

    fn try_decode_task_write_location(
        &self,
        location: gen::TaskWriteLocation,
    ) -> Result<TaskWriteLocation> {
        let gen::TaskWriteLocation { location } = location;
        let location = match location {
            Some(gen::task_write_location::Location::Local(gen::TaskWriteLocationLocal {
                channel,
                storage,
            })) => TaskWriteLocation::Local {
                channel: channel.into(),
                storage: self.try_decode_local_stream_storage(storage)?,
            },
            Some(gen::task_write_location::Location::Remote(gen::TaskWriteLocationRemote {
                uri,
            })) => TaskWriteLocation::Remote { uri },
            None => return plan_err!("no shuffle write location found"),
        };
        Ok(location)
    }

    fn try_encode_task_write_location(
        &self,
        location: &TaskWriteLocation,
    ) -> Result<gen::TaskWriteLocation> {
        let location = match location {
            TaskWriteLocation::Local { channel, storage } => gen::TaskWriteLocation {
                location: Some(gen::task_write_location::Location::Local(
                    gen::TaskWriteLocationLocal {
                        channel: channel.clone().into(),
                        storage: self.try_encode_local_stream_storage(*storage)?,
                    },
                )),
            },
            TaskWriteLocation::Remote { uri } => gen::TaskWriteLocation {
                location: Some(gen::task_write_location::Location::Remote(
                    gen::TaskWriteLocationRemote { uri: uri.clone() },
                )),
            },
        };
        Ok(location)
    }

    fn try_decode_task_write_location_list(
        &self,
        locations: gen::TaskWriteLocationList,
    ) -> Result<Vec<TaskWriteLocation>> {
        let gen::TaskWriteLocationList { locations } = locations;
        locations
            .into_iter()
            .map(|location| self.try_decode_task_write_location(location))
            .collect()
    }

    fn try_encode_task_write_location_list(
        &self,
        locations: &[TaskWriteLocation],
    ) -> Result<gen::TaskWriteLocationList> {
        let locations = locations
            .iter()
            .map(|location| self.try_encode_task_write_location(location))
            .collect::<Result<_>>()?;
        Ok(gen::TaskWriteLocationList { locations })
    }

    fn try_decode_data_type(&self, buf: &[u8]) -> Result<DataType> {
        let arrow_type = self.try_decode_message::<gen_datafusion_common::ArrowType>(buf)?;
        Ok((&arrow_type).try_into()?)
    }

    fn try_encode_data_type(&self, data_type: &DataType) -> Result<Vec<u8>> {
        self.try_encode_message::<gen_datafusion_common::ArrowType>(data_type.try_into()?)
    }

    fn try_decode_schema(&self, buf: &[u8]) -> Result<Schema> {
        let schema = self.try_decode_message::<gen_datafusion_common::Schema>(buf)?;
        Ok((&schema).try_into()?)
    }

    fn try_encode_schema(&self, schema: &Schema) -> Result<Vec<u8>> {
        self.try_encode_message::<gen_datafusion_common::Schema>(schema.try_into()?)
    }

    fn try_decode_message<M>(&self, buf: &[u8]) -> Result<M>
    where
        M: Message + Default,
    {
        let message =
            M::decode(buf).map_err(|e| plan_datafusion_err!("failed to decode message: {e}"))?;
        Ok(message)
    }

    fn try_encode_message<M>(&self, message: M) -> Result<Vec<u8>>
    where
        M: Message,
    {
        let mut buffer = BytesMut::new();
        message
            .encode(&mut buffer)
            .map_err(|e| plan_datafusion_err!("failed to encode message: {e}"))?;
        Ok(buffer.freeze().into())
    }
}
