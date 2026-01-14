use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::{DataType, Schema, TimeUnit};
use datafusion::common::parsers::CompressionTypeVariant;
use datafusion::common::{plan_datafusion_err, plan_err, JoinSide, Result};
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::physical_plan::{
    ArrowSource, AvroSource, FileScanConfig, FileScanConfigBuilder, FileSink, FileSinkConfig,
    JsonSource,
};
use datafusion::datasource::sink::DataSinkExec;
use datafusion::datasource::source::{DataSource, DataSourceExec};
use datafusion::execution::TaskContext;
use datafusion::functions::core::greatest::GreatestFunc;
use datafusion::functions::core::least::LeastFunc;
use datafusion::functions::string::overlay::OverlayFunc;
use datafusion::logical_expr::{AggregateUDF, AggregateUDFImpl, ScalarUDF, ScalarUDFImpl};
use datafusion::physical_expr::{LexOrdering, LexRequirement, Partitioning, PhysicalSortExpr};
use datafusion::physical_plan::joins::utils::{ColumnIndex, JoinFilter};
use datafusion::physical_plan::joins::SortMergeJoinExec;
use datafusion::physical_plan::recursive_query::RecursiveQueryExec;
use datafusion::physical_plan::sorts::partial_sort::PartialSortExec;
use datafusion::physical_plan::work_table::WorkTableExec;
use datafusion::physical_plan::ExecutionPlan;
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
use datafusion_spark::function::array::shuffle::SparkShuffle;
use datafusion_spark::function::bitmap::bitmap_count::BitmapCount;
use datafusion_spark::function::bitwise::bit_count::SparkBitCount;
use datafusion_spark::function::bitwise::bit_get::SparkBitGet;
use datafusion_spark::function::bitwise::bitwise_not::SparkBitwiseNot;
use datafusion_spark::function::datetime::make_dt_interval::SparkMakeDtInterval;
use datafusion_spark::function::datetime::make_interval::SparkMakeInterval;
use datafusion_spark::function::hash::crc32::SparkCrc32;
use datafusion_spark::function::hash::sha1::SparkSha1;
use datafusion_spark::function::math::expm1::SparkExpm1;
use datafusion_spark::function::math::hex::SparkHex;
use datafusion_spark::function::math::modulus::SparkPmod;
use datafusion_spark::function::math::width_bucket::SparkWidthBucket;
use datafusion_spark::function::string::elt::SparkElt;
use datafusion_spark::function::string::format_string::FormatStringFunc;
use datafusion_spark::function::string::luhn_check::SparkLuhnCheck;
use prost::Message;
use sail_common_datafusion::array::record_batch::{read_record_batches, write_record_batches};
use sail_common_datafusion::datasource::PhysicalSinkMode;
use sail_common_datafusion::physical_expr::PhysicalExprWithSource;
use sail_common_datafusion::udf::StreamUDF;
use sail_data_source::formats::binary::source::BinarySource;
use sail_data_source::formats::console::ConsoleSinkExec;
use sail_data_source::formats::rate::{RateSourceExec, TableRateOptions};
use sail_data_source::formats::socket::{SocketSourceExec, TableSocketOptions};
use sail_data_source::formats::text::source::TextSource;
use sail_data_source::formats::text::writer::{TextSink, TextWriterOptions};
use sail_delta_lake::physical_plan::{
    DeltaCommitExec, DeltaDiscoveryExec, DeltaLogScanExec, DeltaRemoveActionsExec,
    DeltaScanByAddsExec, DeltaWriterExec,
};
use sail_function::aggregate::kurtosis::KurtosisFunction;
use sail_function::aggregate::max_min_by::{MaxByFunction, MinByFunction};
use sail_function::aggregate::mode::ModeFunction;
use sail_function::aggregate::percentile_disc::PercentileDisc;
use sail_function::aggregate::skewness::SkewnessFunc;
use sail_function::aggregate::try_avg::TryAvgFunction;
use sail_function::aggregate::try_sum::TrySumFunction;
use sail_function::scalar::array::arrays_zip::ArraysZip;
use sail_function::scalar::array::spark_array::SparkArray;
use sail_function::scalar::array::spark_array_item_with_position::ArrayItemWithPosition;
use sail_function::scalar::array::spark_array_min_max::{ArrayMax, ArrayMin};
use sail_function::scalar::array::spark_sequence::SparkSequence;
use sail_function::scalar::collection::spark_concat::SparkConcat;
use sail_function::scalar::collection::spark_reverse::SparkReverse;
use sail_function::scalar::csv::spark_from_csv::SparkFromCSV;
use sail_function::scalar::datetime::convert_tz::ConvertTz;
use sail_function::scalar::datetime::spark_date::SparkDate;
use sail_function::scalar::datetime::spark_interval::{
    SparkCalendarInterval, SparkDayTimeInterval, SparkYearMonthInterval,
};
use sail_function::scalar::datetime::spark_last_day::SparkLastDay;
use sail_function::scalar::datetime::spark_make_timestamp::SparkMakeTimestampNtz;
use sail_function::scalar::datetime::spark_make_ym_interval::SparkMakeYmInterval;
use sail_function::scalar::datetime::spark_next_day::SparkNextDay;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::datetime::spark_to_chrono_fmt::SparkToChronoFmt;
use sail_function::scalar::datetime::spark_try_to_timestamp::SparkTryToTimestamp;
use sail_function::scalar::datetime::spark_unix_timestamp::SparkUnixTimestamp;
use sail_function::scalar::datetime::timestamp_now::TimestampNow;
use sail_function::scalar::drop_struct_field::DropStructField;
use sail_function::scalar::explode::{explode_name_to_kind, Explode};
use sail_function::scalar::hash::spark_murmur3_hash::SparkMurmur3Hash;
use sail_function::scalar::hash::spark_xxhash64::SparkXxhash64;
use sail_function::scalar::map::map_from_arrays::MapFromArrays;
use sail_function::scalar::map::map_from_entries::MapFromEntries;
use sail_function::scalar::map::str_to_map::StrToMap;
use sail_function::scalar::math::rand_poisson::RandPoisson;
use sail_function::scalar::math::randn::Randn;
use sail_function::scalar::math::random::Random;
use sail_function::scalar::math::spark_abs::SparkAbs;
use sail_function::scalar::math::spark_bin::SparkBin;
use sail_function::scalar::math::spark_bround::SparkBRound;
use sail_function::scalar::math::spark_ceil_floor::{SparkCeil, SparkFloor};
use sail_function::scalar::math::spark_conv::SparkConv;
use sail_function::scalar::math::spark_div::SparkIntervalDiv;
use sail_function::scalar::math::spark_signum::SparkSignum;
use sail_function::scalar::math::spark_try_add::SparkTryAdd;
use sail_function::scalar::math::spark_try_div::SparkTryDiv;
use sail_function::scalar::math::spark_try_mod::SparkTryMod;
use sail_function::scalar::math::spark_try_mult::SparkTryMult;
use sail_function::scalar::math::spark_try_subtract::SparkTrySubtract;
use sail_function::scalar::math::spark_unhex::SparkUnHex;
use sail_function::scalar::misc::raise_error::RaiseError;
use sail_function::scalar::misc::spark_aes::{
    SparkAESDecrypt, SparkAESEncrypt, SparkTryAESDecrypt, SparkTryAESEncrypt,
};
use sail_function::scalar::misc::version::SparkVersion;
use sail_function::scalar::multi_expr::MultiExpr;
use sail_function::scalar::spark_to_string::{SparkToLargeUtf8, SparkToUtf8, SparkToUtf8View};
use sail_function::scalar::string::levenshtein::Levenshtein;
use sail_function::scalar::string::make_valid_utf8::MakeValidUtf8;
use sail_function::scalar::string::spark_base64::{SparkBase64, SparkUnbase64};
use sail_function::scalar::string::spark_encode_decode::{SparkDecode, SparkEncode};
use sail_function::scalar::string::spark_mask::SparkMask;
use sail_function::scalar::string::spark_split::SparkSplit;
use sail_function::scalar::string::spark_to_binary::{SparkToBinary, SparkTryToBinary};
use sail_function::scalar::string::spark_to_number::SparkToNumber;
use sail_function::scalar::string::spark_try_to_number::SparkTryToNumber;
use sail_function::scalar::struct_function::StructFunction;
use sail_function::scalar::update_struct_field::UpdateStructField;
use sail_function::scalar::url::parse_url::ParseUrl;
use sail_function::scalar::url::spark_try_parse_url::SparkTryParseUrl;
use sail_function::scalar::url::url_decode::UrlDecode;
use sail_function::scalar::url::url_encode::UrlEncode;
use sail_iceberg::physical_plan::{IcebergCommitExec, IcebergWriterExec};
use sail_iceberg::TableIcebergOptions;
use sail_logical_plan::range::Range;
use sail_logical_plan::show_string::{ShowStringFormat, ShowStringStyle};
use sail_physical_plan::map_partitions::MapPartitionsExec;
use sail_physical_plan::merge_cardinality_check::MergeCardinalityCheckExec;
use sail_physical_plan::range::RangeExec;
use sail_physical_plan::schema_pivot::SchemaPivotExec;
use sail_physical_plan::show_string::ShowStringExec;
use sail_physical_plan::streaming::collector::StreamCollectorExec;
use sail_physical_plan::streaming::limit::StreamLimitExec;
use sail_physical_plan::streaming::source_adapter::StreamSourceAdapterExec;
use sail_python_udf::config::PySparkUdfConfig;
use sail_python_udf::udf::pyspark_batch_collector::PySparkBatchCollectorUDF;
use sail_python_udf::udf::pyspark_cogroup_map_udf::PySparkCoGroupMapUDF;
use sail_python_udf::udf::pyspark_group_map_udf::PySparkGroupMapUDF;
use sail_python_udf::udf::pyspark_map_iter_udf::{PySparkMapIterKind, PySparkMapIterUDF};
use sail_python_udf::udf::pyspark_udaf::PySparkGroupAggregateUDF;
use sail_python_udf::udf::pyspark_udf::{PySparkUDF, PySparkUdfKind};
use sail_python_udf::udf::pyspark_udtf::{PySparkUDTF, PySparkUdtfKind};
use url::Url;

use crate::plan::gen::extended_aggregate_udf::UdafKind;
use crate::plan::gen::extended_physical_plan_node::NodeKind;
use crate::plan::gen::extended_scalar_udf::UdfKind;
use crate::plan::gen::extended_stream_udf::StreamUdfKind;
use crate::plan::gen::{
    ExtendedAggregateUdf, ExtendedPhysicalPlanNode, ExtendedScalarUdf, ExtendedStreamUdf,
};
use crate::plan::{gen, StageInputExec};

pub struct RemoteExecutionCodec;

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
        ctx: &TaskContext,
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
                    self.try_decode_plan(&input, ctx)?,
                    names,
                    limit as usize,
                    ShowStringFormat::new(
                        self.try_decode_show_string_style(style)?,
                        truncate as usize,
                    ),
                    Arc::new(schema),
                )))
            }
            NodeKind::StageInput(gen::StageInputExecNode {
                input,
                schema,
                partitioning,
            }) => {
                let schema = self.try_decode_schema(&schema)?;
                let partitioning = self.try_decode_partitioning(&partitioning, &schema, ctx)?;
                let node = StageInputExec::new(input as usize, Arc::new(schema), partitioning);
                Ok(Arc::new(node))
            }
            NodeKind::SchemaPivot(gen::SchemaPivotExecNode {
                input,
                names,
                schema,
            }) => {
                let schema = self.try_decode_schema(&schema)?;
                Ok(Arc::new(SchemaPivotExec::new(
                    self.try_decode_plan(&input, ctx)?,
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
                    self.try_decode_plan(&input, ctx)?,
                    self.try_decode_stream_udf(udf)?,
                    Arc::new(schema),
                )))
            }
            NodeKind::Memory(gen::MemoryExecNode {
                partitions,
                schema,
                projection,
                show_sizes,
                sort_information,
                limit,
            }) => {
                let schema = self.try_decode_schema(&schema)?;
                let partitions = partitions
                    .into_iter()
                    .map(|x| read_record_batches(&x))
                    .collect::<Result<Vec<_>>>()?;
                let projection =
                    projection.map(|x| x.columns.into_iter().map(|c| c as usize).collect());
                let sort_information =
                    self.try_decode_lex_orderings(&sort_information, &schema, ctx)?;
                let source =
                    MemorySourceConfig::try_new(&partitions, Arc::new(schema), projection)?
                        .with_show_sizes(show_sizes)
                        .try_with_sort_information(sort_information)?
                        .with_limit(limit.map(|x| x as usize));
                Ok(Arc::new(DataSourceExec::new(Arc::new(source))))
            }
            NodeKind::Values(gen::ValuesExecNode { data, schema }) => {
                let schema = self.try_decode_schema(&schema)?;
                let data = read_record_batches(&data)?;
                let source = MemorySourceConfig::try_new_from_batches(Arc::new(schema), data)?;
                Ok(source)
            }
            NodeKind::NdJson(gen::NdJsonExecNode {
                base_config,
                file_compression_type,
            }) => {
                let file_compression_type: FileCompressionType =
                    self.try_decode_file_compression_type(file_compression_type)?;
                let source = parse_protobuf_file_scan_config(
                    &self.try_decode_message(&base_config)?,
                    ctx,
                    self,
                    Arc::new(JsonSource::new()), // TODO: Look into configuring this if needed
                )?;
                let source = FileScanConfigBuilder::from(source)
                    .with_file_compression_type(file_compression_type)
                    .build();
                Ok(Arc::new(DataSourceExec::new(Arc::new(source))))
            }
            NodeKind::Arrow(gen::ArrowExecNode { base_config }) => {
                let source = parse_protobuf_file_scan_config(
                    &self.try_decode_message(&base_config)?,
                    ctx,
                    self,
                    Arc::new(ArrowSource::default()), // TODO: Look into configuring this if needed
                )?;
                Ok(Arc::new(DataSourceExec::new(Arc::new(source))))
            }
            NodeKind::Text(gen::TextExecNode {
                base_config,
                file_compression_type,
                whole_text,
                line_sep,
            }) => {
                let file_compression_type: FileCompressionType =
                    self.try_decode_file_compression_type(file_compression_type)?;
                let line_sep: Option<u8> = match line_sep {
                    None => None,
                    Some(bytes) if bytes.is_empty() => None,
                    Some(bytes) if bytes.len() == 1 => Some(bytes[0]),
                    Some(bytes) => {
                        return plan_err!(
                            "try_decode: line separator must be a single byte or empty, got: {bytes:?}"
                        );
                    }
                };
                let source = parse_protobuf_file_scan_config(
                    &self.try_decode_message(&base_config)?,
                    ctx,
                    self,
                    Arc::new(TextSource::new(whole_text, line_sep)),
                )?;
                let source = FileScanConfigBuilder::from(source)
                    .with_file_compression_type(file_compression_type)
                    .build();
                Ok(Arc::new(DataSourceExec::new(Arc::new(source))))
            }
            NodeKind::BinarySource(gen::BinarySourceExecNode {
                base_config,
                path_glob_filter,
            }) => {
                let source = parse_protobuf_file_scan_config(
                    &self.try_decode_message(&base_config)?,
                    ctx,
                    self,
                    Arc::new(BinarySource::new(path_glob_filter)),
                )?;
                let source = FileScanConfigBuilder::from(source).build();
                Ok(Arc::new(DataSourceExec::new(Arc::new(source))))
            }
            NodeKind::Avro(gen::AvroExecNode { base_config }) => {
                let source = parse_protobuf_file_scan_config(
                    &self.try_decode_message(&base_config)?,
                    ctx,
                    self,
                    Arc::new(AvroSource::new()),
                )?;
                Ok(Arc::new(DataSourceExec::new(Arc::new(source))))
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
                let static_term = self.try_decode_plan(&static_term, ctx)?;
                let recursive_term = self.try_decode_plan(&recursive_term, ctx)?;
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
                let left = self.try_decode_plan(&left, ctx)?;
                let right = self.try_decode_plan(&right, ctx)?;
                let on = on
                    .into_iter()
                    .map(|join_on| {
                        let left = parse_physical_expr(
                            &self.try_decode_message(&join_on.left)?,
                            ctx,
                            &left.schema(),
                            self,
                        )?;
                        let right = parse_physical_expr(
                            &self.try_decode_message(&join_on.right)?,
                            ctx,
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
                        ctx,
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
                    Some(JoinFilter::new(
                        expression,
                        column_indices,
                        Arc::new(schema),
                    ))
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
                let null_equality = if null_equals_null {
                    datafusion::common::NullEquality::NullEqualsNull
                } else {
                    datafusion::common::NullEquality::NullEqualsNothing
                };
                Ok(Arc::new(SortMergeJoinExec::try_new(
                    left,
                    right,
                    on,
                    filter,
                    join_type,
                    sort_options,
                    null_equality,
                )?))
            }
            NodeKind::DeltaWriter(gen::DeltaWriterExecNode {
                input,
                table_url,
                options,
                sink_schema,
                partition_columns,
                table_exists,
                sink_mode,
                operation_override_json,
            }) => {
                let input = self.try_decode_plan(&input, ctx)?;
                let sink_schema = self.try_decode_schema(&sink_schema)?;
                let sink_mode = match sink_mode {
                    Some(mode) => mode,
                    None => return plan_err!("Missing sink_mode"),
                };
                let sink_mode =
                    self.try_decode_physical_sink_mode(sink_mode, &input.schema(), ctx)?;

                let table_url = Url::parse(&table_url)
                    .map_err(|e| plan_datafusion_err!("failed to parse table URL: {e}"))?;
                let options =
                    serde_json::from_str(&options).map_err(|e| plan_datafusion_err!("{e}"))?;

                let operation_override = if let Some(s) = operation_override_json.as_ref() {
                    Some(serde_json::from_str(s).map_err(|e| plan_datafusion_err!("{e}"))?)
                } else {
                    None
                };
                Ok(Arc::new(DeltaWriterExec::new(
                    input,
                    table_url,
                    options,
                    partition_columns,
                    sink_mode,
                    table_exists,
                    Arc::new(sink_schema),
                    operation_override,
                )?))
            }
            NodeKind::DeltaCommit(gen::DeltaCommitExecNode {
                input,
                table_url,
                partition_columns,
                table_exists,
                sink_schema,
                sink_mode,
            }) => {
                let input = self.try_decode_plan(&input, ctx)?;
                let sink_schema = self.try_decode_schema(&sink_schema)?;
                let table_url = Url::parse(&table_url)
                    .map_err(|e| plan_datafusion_err!("failed to parse table URL: {e}"))?;

                let sink_mode = if let Some(sink_mode) = sink_mode {
                    self.try_decode_physical_sink_mode(sink_mode, &sink_schema, ctx)?
                } else {
                    return plan_err!("Missing sink_mode for DeltaCommitExec");
                };

                Ok(Arc::new(DeltaCommitExec::new(
                    input,
                    table_url,
                    partition_columns,
                    table_exists,
                    Arc::new(sink_schema),
                    sink_mode,
                )))
            }
            NodeKind::DeltaScanByAdds(gen::DeltaScanByAddsExecNode {
                input,
                table_url,
                table_schema,
            }) => {
                let input = self.try_decode_plan(&input, ctx)?;
                let table_url = Url::parse(&table_url)
                    .map_err(|e| plan_datafusion_err!("failed to parse table URL: {e}"))?;
                let table_schema = Arc::new(self.try_decode_schema(&table_schema)?);
                Ok(Arc::new(DeltaScanByAddsExec::new(
                    input,
                    table_url,
                    table_schema,
                )))
            }
            NodeKind::DeltaDiscovery(gen::DeltaDiscoveryExecNode {
                table_url,
                predicate,
                table_schema,
                version,
                input,
                input_partition_columns,
                input_partition_scan,
            }) => {
                let table_url = Url::parse(&table_url)
                    .map_err(|e| plan_datafusion_err!("failed to parse table URL: {e}"))?;
                let table_schema = if let Some(schema_bytes) = table_schema {
                    Some(Arc::new(self.try_decode_schema(&schema_bytes)?))
                } else {
                    None
                };
                let predicate = if let Some(pred_bytes) = predicate {
                    let empty_schema = Arc::new(Schema::empty());
                    let schema = table_schema.as_ref().unwrap_or(&empty_schema);
                    Some(parse_physical_expr(
                        &self.try_decode_message(&pred_bytes)?,
                        ctx,
                        schema,
                        self,
                    )?)
                } else {
                    None
                };
                let input = input
                    .ok_or_else(|| plan_datafusion_err!("Missing input for DeltaDiscoveryExec"))?;
                let input = self.try_decode_plan(&input, ctx)?;
                Ok(Arc::new(DeltaDiscoveryExec::with_input(
                    input,
                    table_url,
                    predicate,
                    table_schema,
                    version,
                    input_partition_columns,
                    input_partition_scan,
                )?))
            }
            NodeKind::DeltaRemoveActions(gen::DeltaRemoveActionsExecNode { input }) => {
                let input = self.try_decode_plan(&input, ctx)?;
                Ok(Arc::new(DeltaRemoveActionsExec::new(input)?))
            }
            NodeKind::DeltaLogScan(gen::DeltaLogScanExecNode {
                input,
                table_url,
                version,
                partition_columns,
                checkpoint_files,
                commit_files,
            }) => {
                let input = self.try_decode_plan(&input, ctx)?;
                let table_url = Url::parse(&table_url)
                    .map_err(|e| plan_datafusion_err!("failed to parse table URL: {e}"))?;
                Ok(Arc::new(DeltaLogScanExec::new(
                    input,
                    table_url,
                    version,
                    partition_columns,
                    checkpoint_files,
                    commit_files,
                )))
            }
            NodeKind::ConsoleSink(gen::ConsoleSinkExecNode { input }) => {
                let input = self.try_decode_plan(&input, ctx)?;
                Ok(Arc::new(ConsoleSinkExec::new(input)))
            }
            NodeKind::SocketSource(gen::SocketSourceExecNode {
                host,
                port,
                max_batch_size,
                timeout_sec,
                schema,
                projection,
            }) => {
                let options = TableSocketOptions {
                    host,
                    port: u16::try_from(port)
                        .map_err(|_| plan_datafusion_err!("invalid port for socket source"))?,
                    max_batch_size: usize::try_from(max_batch_size).map_err(|_| {
                        plan_datafusion_err!("invalid max batch size for socket source")
                    })?,
                    timeout_sec,
                };
                let schema = self.try_decode_schema(&schema)?;
                let projection = self.try_decode_projection(&projection)?;
                Ok(Arc::new(SocketSourceExec::try_new(
                    options,
                    Arc::new(schema),
                    projection,
                )?))
            }
            NodeKind::RateSource(gen::RateSourceExecNode {
                rows_per_second,
                num_partitions,
                schema,
                projection,
            }) => {
                let options = TableRateOptions {
                    rows_per_second: usize::try_from(rows_per_second).map_err(|_| {
                        plan_datafusion_err!("invalid rows per second for rate source")
                    })?,
                    num_partitions: usize::try_from(num_partitions).map_err(|_| {
                        plan_datafusion_err!("invalid number of partitions for rate source")
                    })?,
                };
                let projection = self.try_decode_projection(&projection)?;
                let schema = self.try_decode_schema(&schema)?;
                Ok(Arc::new(RateSourceExec::try_new(
                    options,
                    Arc::new(schema),
                    projection,
                )?))
            }
            NodeKind::TextSink(gen::TextSinkExecNode {
                input,
                base_config,
                schema,
                line_sep,
                compression_type_variant,
                sort_order,
            }) => {
                let input = self.try_decode_plan(&input, ctx)?;
                let schema = self.try_decode_schema(&schema)?;
                let compression_type_variant =
                    self.try_decode_compression_type_variant(compression_type_variant)?;
                let line_sep = if line_sep.len() == 1 {
                    line_sep[0]
                } else {
                    return plan_err!(
                        "try_decode: line separator must be a single byte, got: {line_sep:?}"
                    );
                };
                let file_sink_config: datafusion_proto::protobuf::FileSinkConfig =
                    self.try_decode_message(&base_config)?;
                let file_sink_config = FileSinkConfig::try_from(&file_sink_config)?;
                let writer_options = TextWriterOptions::new(line_sep, compression_type_variant);
                let data_sink = TextSink::new(file_sink_config, writer_options);
                let physical_sort_expr_nodes = if let Some(sort_order) = sort_order {
                    let physical_sort_expr_nodes: Vec<PhysicalSortExprNode> = sort_order
                        .physical_sort_expr_nodes
                        .iter()
                        .map(|x| self.try_decode_message(x))
                        .collect::<Result<Vec<_>>>()?;
                    Some(physical_sort_expr_nodes)
                } else {
                    None
                };
                let sort_order = physical_sort_expr_nodes
                    .as_ref()
                    .map(|physical_sort_expr_nodes| {
                        parse_physical_sort_exprs(physical_sort_expr_nodes, ctx, &schema, self).map(
                            |sort_exprs| {
                                LexRequirement::new(sort_exprs.into_iter().map(Into::into))
                            },
                        )
                    })
                    .transpose()?
                    .flatten();
                Ok(Arc::new(DataSinkExec::new(
                    input,
                    Arc::new(data_sink),
                    sort_order,
                )))
            }
            NodeKind::StreamCollector(gen::StreamCollectorExecNode { input }) => {
                let input = self.try_decode_plan(&input, ctx)?;
                Ok(Arc::new(StreamCollectorExec::try_new(input)?))
            }
            NodeKind::StreamLimit(gen::StreamLimitExecNode { input, skip, fetch }) => {
                let input = self.try_decode_plan(&input, ctx)?;
                let skip = usize::try_from(skip)
                    .map_err(|_| plan_datafusion_err!("invalid skip value for StreamLimitExec"))?;
                let fetch = fetch
                    .map(usize::try_from)
                    .transpose()
                    .map_err(|_| plan_datafusion_err!("invalid fetch value for StreamLimitExec"))?;
                Ok(Arc::new(StreamLimitExec::try_new(input, skip, fetch)?))
            }
            NodeKind::StreamSourceAdapter(gen::StreamSourceAdapterExecNode { input }) => {
                let input = self.try_decode_plan(&input, ctx)?;
                Ok(Arc::new(StreamSourceAdapterExec::new(input)))
            }
            NodeKind::MergeCardinalityCheck(gen::MergeCardinalityCheckExecNode {
                input,
                target_row_id_col,
                target_present_col,
                source_present_col,
            }) => Ok(Arc::new(MergeCardinalityCheckExec::new(
                self.try_decode_plan(&input, ctx)?,
                target_row_id_col,
                target_present_col,
                source_present_col,
            )?)),
            NodeKind::IcebergWriter(gen::IcebergWriterExecNode {
                input,
                table_url,
                partition_columns,
                sink_mode,
                table_exists,
                options,
            }) => {
                let input = self.try_decode_plan(&input, ctx)?;
                let sink_mode = match sink_mode {
                    Some(mode) => mode,
                    None => return plan_err!("Missing sink_mode for IcebergWriterExec"),
                };
                let sink_mode =
                    self.try_decode_physical_sink_mode(sink_mode, &input.schema(), ctx)?;
                let table_url = Url::parse(&table_url)
                    .map_err(|e| plan_datafusion_err!("failed to parse table URL: {e}"))?;
                let options = if options.is_empty() {
                    TableIcebergOptions::default()
                } else {
                    serde_json::from_str(&options).map_err(|e| {
                        plan_datafusion_err!("failed to decode Iceberg options: {e}")
                    })?
                };

                Ok(Arc::new(IcebergWriterExec::new(
                    input,
                    table_url,
                    partition_columns,
                    sink_mode,
                    table_exists,
                    options,
                )))
            }
            NodeKind::IcebergCommit(gen::IcebergCommitExecNode { input, table_url }) => {
                let input = self.try_decode_plan(&input, ctx)?;
                let table_url = Url::parse(&table_url)
                    .map_err(|e| plan_datafusion_err!("failed to parse table URL: {e}"))?;

                Ok(Arc::new(IcebergCommitExec::new(input, table_url)))
            }
            _ => plan_err!("unsupported physical plan node: {node_kind:?}"),
        }
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
        #[allow(deprecated)]
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
        } else if let Some(stage_input) = node.as_any().downcast_ref::<StageInputExec<usize>>() {
            let schema = self.try_encode_schema(stage_input.schema().as_ref())?;
            let partitioning =
                self.try_encode_partitioning(stage_input.properties().output_partitioning())?;
            NodeKind::StageInput(gen::StageInputExecNode {
                input: *stage_input.input() as u64,
                schema,
                partitioning,
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
            let null_equals_null = match sort_merge_join.null_equality() {
                datafusion::common::NullEquality::NullEqualsNull => true,
                datafusion::common::NullEquality::NullEqualsNothing => false,
            };
            NodeKind::SortMergeJoin(gen::SortMergeJoinExecNode {
                left,
                right,
                on,
                filter,
                join_type,
                sort_options,
                null_equals_null,
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
        } else if let Some(data_source) = node.as_any().downcast_ref::<DataSourceExec>() {
            let source = data_source.data_source();
            if let Some(file_scan) = source.as_any().downcast_ref::<FileScanConfig>() {
                let file_source = file_scan.file_source();
                if let Some(text_source) = file_source.as_any().downcast_ref::<TextSource>() {
                    let base_config =
                        self.try_encode_message(serialize_file_scan_config(file_scan, self)?)?;
                    let file_compression_type =
                        self.try_encode_file_compression_type(file_scan.file_compression_type)?;
                    NodeKind::Text(gen::TextExecNode {
                        base_config,
                        file_compression_type,
                        whole_text: text_source.whole_text(),
                        line_sep: text_source.line_sep().map(|x| vec![x]),
                    })
                } else if let Some(binary_source) =
                    file_source.as_any().downcast_ref::<BinarySource>()
                {
                    let base_config =
                        self.try_encode_message(serialize_file_scan_config(file_scan, self)?)?;
                    NodeKind::BinarySource(gen::BinarySourceExecNode {
                        base_config,
                        path_glob_filter: binary_source.path_glob_filter().cloned(),
                    })
                } else if file_source.as_any().is::<JsonSource>() {
                    // TODO: Check if we still need to have JsonSource: https://github.com/apache/datafusion/pull/14224
                    let base_config =
                        self.try_encode_message(serialize_file_scan_config(file_scan, self)?)?;
                    let file_compression_type =
                        self.try_encode_file_compression_type(file_scan.file_compression_type)?;
                    NodeKind::NdJson(gen::NdJsonExecNode {
                        base_config,
                        file_compression_type,
                    })
                } else if file_source.as_any().is::<ArrowSource>() {
                    // TODO: Check if we still need to have ArrowSource: https://github.com/apache/datafusion/pull/14224
                    let base_config =
                        self.try_encode_message(serialize_file_scan_config(file_scan, self)?)?;
                    NodeKind::Arrow(gen::ArrowExecNode { base_config })
                } else if file_source.as_any().is::<AvroSource>() {
                    let base_config =
                        self.try_encode_message(serialize_file_scan_config(file_scan, self)?)?;
                    NodeKind::Avro(gen::AvroExecNode { base_config })
                } else {
                    return plan_err!("unsupported data source node: {data_source:?}");
                }
            } else if let Some(memory) = source.as_any().downcast_ref::<MemorySourceConfig>() {
                // TODO: Check if we still need to have MemorySourceConfig: https://github.com/apache/datafusion/pull/14224
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
                    limit: memory.fetch().map(|x| x as u64),
                })
            } else {
                return plan_err!("unsupported data source node: {data_source:?}");
            }
        } else if let Some(delta_writer_exec) = node.as_any().downcast_ref::<DeltaWriterExec>() {
            let input = self.try_encode_plan(delta_writer_exec.input().clone())?;
            let sink_mode = self.try_encode_physical_sink_mode(delta_writer_exec.sink_mode())?;
            let operation_override_json = if let Some(op) = delta_writer_exec.operation_override() {
                Some(serde_json::to_string(op).map_err(|e| plan_datafusion_err!("{e}"))?)
            } else {
                None
            };
            NodeKind::DeltaWriter(gen::DeltaWriterExecNode {
                input,
                table_url: delta_writer_exec.table_url().to_string(),
                options: serde_json::to_string(delta_writer_exec.options())
                    .map_err(|e| plan_datafusion_err!("{e}"))?,
                sink_schema: self.try_encode_schema(delta_writer_exec.sink_schema())?,
                partition_columns: delta_writer_exec.partition_columns().to_vec(),
                table_exists: delta_writer_exec.table_exists(),
                sink_mode: Some(sink_mode),
                operation_override_json,
            })
        } else if let Some(delta_commit_exec) = node.as_any().downcast_ref::<DeltaCommitExec>() {
            let input = self.try_encode_plan(delta_commit_exec.input().clone())?;
            let sink_mode = self.try_encode_physical_sink_mode(delta_commit_exec.sink_mode())?;
            NodeKind::DeltaCommit(gen::DeltaCommitExecNode {
                input,
                table_url: delta_commit_exec.table_url().to_string(),
                partition_columns: delta_commit_exec.partition_columns().to_vec(),
                table_exists: delta_commit_exec.table_exists(),
                sink_schema: self.try_encode_schema(delta_commit_exec.sink_schema())?,
                sink_mode: Some(sink_mode),
            })
        } else if let Some(delta_scan_by_adds_exec) =
            node.as_any().downcast_ref::<DeltaScanByAddsExec>()
        {
            let input = self.try_encode_plan(delta_scan_by_adds_exec.input().clone())?;
            let table_schema = self.try_encode_schema(delta_scan_by_adds_exec.table_schema())?;
            NodeKind::DeltaScanByAdds(gen::DeltaScanByAddsExecNode {
                input,
                table_url: delta_scan_by_adds_exec.table_url().to_string(),
                table_schema,
            })
        } else if let Some(delta_discovery_exec) =
            node.as_any().downcast_ref::<DeltaDiscoveryExec>()
        {
            let input = Some(self.try_encode_plan(delta_discovery_exec.input())?);
            let predicate = if let Some(pred) = delta_discovery_exec.predicate() {
                let predicate_node = serialize_physical_expr(&pred.clone(), self)?;
                Some(self.try_encode_message(predicate_node)?)
            } else {
                None
            };
            let table_schema = if let Some(schema) = delta_discovery_exec.table_schema() {
                Some(self.try_encode_schema(schema)?)
            } else {
                None
            };
            NodeKind::DeltaDiscovery(gen::DeltaDiscoveryExecNode {
                table_url: delta_discovery_exec.table_url().to_string(),
                predicate,
                table_schema,
                version: delta_discovery_exec.version(),
                input,
                input_partition_columns: delta_discovery_exec.input_partition_columns().to_vec(),
                input_partition_scan: delta_discovery_exec.input_partition_scan(),
            })
        } else if let Some(delta_remove_actions_exec) =
            node.as_any().downcast_ref::<DeltaRemoveActionsExec>()
        {
            let input = self.try_encode_plan(delta_remove_actions_exec.children()[0].clone())?;
            NodeKind::DeltaRemoveActions(gen::DeltaRemoveActionsExecNode { input })
        } else if let Some(delta_log_scan_exec) = node.as_any().downcast_ref::<DeltaLogScanExec>() {
            let input = self.try_encode_plan(delta_log_scan_exec.children()[0].clone())?;
            NodeKind::DeltaLogScan(gen::DeltaLogScanExecNode {
                input,
                table_url: delta_log_scan_exec.table_url().to_string(),
                version: delta_log_scan_exec.version(),
                partition_columns: delta_log_scan_exec.partition_columns().to_vec(),
                checkpoint_files: delta_log_scan_exec.checkpoint_files().to_vec(),
                commit_files: delta_log_scan_exec.commit_files().to_vec(),
            })
        } else if let Some(console_sink) = node.as_any().downcast_ref::<ConsoleSinkExec>() {
            let input = self.try_encode_plan(console_sink.input().clone())?;
            NodeKind::ConsoleSink(gen::ConsoleSinkExecNode { input })
        } else if let Some(socket_source) = node.as_any().downcast_ref::<SocketSourceExec>() {
            let options = socket_source.options();
            let max_batch_size = u64::try_from(options.max_batch_size).map_err(|_| {
                plan_datafusion_err!("cannot encode max batch size for socket source")
            })?;
            let schema = self.try_encode_schema(socket_source.original_schema())?;
            let projection = self.try_encode_projection(socket_source.projection())?;
            NodeKind::SocketSource(gen::SocketSourceExecNode {
                host: options.host.clone(),
                port: options.port as u32,
                max_batch_size,
                timeout_sec: options.timeout_sec,
                schema,
                projection,
            })
        } else if let Some(rate_source) = node.as_any().downcast_ref::<RateSourceExec>() {
            let options = rate_source.options();
            let rows_per_second = u64::try_from(options.rows_per_second).map_err(|_| {
                plan_datafusion_err!("cannot encode rows per second for rate source")
            })?;
            let num_partitions = u64::try_from(options.num_partitions).map_err(|_| {
                plan_datafusion_err!("cannot encode number of partitions for rate source")
            })?;
            let schema = self.try_encode_schema(rate_source.original_schema())?;
            let projection = self.try_encode_projection(rate_source.projection())?;
            NodeKind::RateSource(gen::RateSourceExecNode {
                rows_per_second,
                num_partitions,
                schema,
                projection,
            })
        } else if let Some(data_sink) = node.as_any().downcast_ref::<DataSinkExec>() {
            let input = self.try_encode_plan(data_sink.input().clone())?;
            let sort_order = match data_sink.sort_order() {
                Some(requirements) => {
                    let expr = requirements
                        .iter()
                        .map(|requirement| {
                            let expr: PhysicalSortExpr = requirement.to_owned().into();
                            let sort_expr = PhysicalSortExprNode {
                                expr: Some(Box::new(serialize_physical_expr(&expr.expr, self)?)),
                                asc: !expr.options.descending,
                                nulls_first: expr.options.nulls_first,
                            };
                            Ok(sort_expr)
                        })
                        .collect::<Result<Vec<_>>>()?;
                    Some(datafusion_proto::protobuf::PhysicalSortExprNodeCollection {
                        physical_sort_expr_nodes: expr,
                    })
                }
                None => None,
            };
            let sort_order = if let Some(sort_order) = sort_order {
                let physical_sort_expr_nodes = sort_order
                    .physical_sort_expr_nodes
                    .into_iter()
                    .map(|x| self.try_encode_message(x))
                    .collect::<Result<_>>()?;
                Some(gen::PhysicalSortExprNodeCollection {
                    physical_sort_expr_nodes,
                })
            } else {
                None
            };
            if let Some(sink) = data_sink.sink().as_any().downcast_ref::<TextSink>() {
                let base_config = self.try_encode_message(
                    datafusion_proto::protobuf::FileSinkConfig::try_from(sink.config())
                        .map_err(|e| plan_datafusion_err!("failed to encode text sink: {e}"))?,
                )?;
                let writer_options = sink.writer_options();
                let compression_type_variant =
                    self.try_encode_compression_type_variant(writer_options.compression)?;
                let line_sep = vec![writer_options.line_sep];
                let schema = self.try_encode_schema(data_sink.schema().as_ref())?;
                NodeKind::TextSink(gen::TextSinkExecNode {
                    input,
                    base_config,
                    schema,
                    line_sep,
                    compression_type_variant,
                    sort_order,
                })
            } else {
                return plan_err!("unsupported data sink node: {data_sink:?}");
            }
        } else if let Some(stream_collector) = node.as_any().downcast_ref::<StreamCollectorExec>() {
            let input = self.try_encode_plan(stream_collector.input().clone())?;
            NodeKind::StreamCollector(gen::StreamCollectorExecNode { input })
        } else if let Some(stream_limit) = node.as_any().downcast_ref::<StreamLimitExec>() {
            let input = self.try_encode_plan(stream_limit.input().clone())?;
            let skip = u64::try_from(stream_limit.skip()).map_err(|_| {
                plan_datafusion_err!("cannot encode skip value for StreamLimitExec")
            })?;
            let fetch = stream_limit
                .fetch()
                .map(u64::try_from)
                .transpose()
                .map_err(|_| {
                    plan_datafusion_err!("cannot encode fetch value for StreamLimitExec")
                })?;
            NodeKind::StreamLimit(gen::StreamLimitExecNode { input, skip, fetch })
        } else if let Some(stream_source_adapter) =
            node.as_any().downcast_ref::<StreamSourceAdapterExec>()
        {
            let input = self.try_encode_plan(stream_source_adapter.input().clone())?;
            NodeKind::StreamSourceAdapter(gen::StreamSourceAdapterExecNode { input })
        } else if let Some(cardinality_check) =
            node.as_any().downcast_ref::<MergeCardinalityCheckExec>()
        {
            let input = self.try_encode_plan(cardinality_check.input().clone())?;
            NodeKind::MergeCardinalityCheck(gen::MergeCardinalityCheckExecNode {
                input,
                target_row_id_col: cardinality_check.target_row_id_col().to_string(),
                target_present_col: cardinality_check.target_present_col().to_string(),
                source_present_col: cardinality_check.source_present_col().to_string(),
            })
        } else if let Some(iceberg_writer_exec) = node.as_any().downcast_ref::<IcebergWriterExec>()
        {
            let input = self.try_encode_plan(iceberg_writer_exec.input().clone())?;
            let sink_mode = self.try_encode_physical_sink_mode(iceberg_writer_exec.sink_mode())?;
            let options = serde_json::to_string(iceberg_writer_exec.options())
                .map_err(|e| plan_datafusion_err!("failed to encode Iceberg options: {e}"))?;
            NodeKind::IcebergWriter(gen::IcebergWriterExecNode {
                input,
                table_url: iceberg_writer_exec.table_url().to_string(),
                partition_columns: iceberg_writer_exec.partition_columns().to_vec(),
                sink_mode: Some(sink_mode),
                table_exists: iceberg_writer_exec.table_exists(),
                options,
            })
        } else if let Some(iceberg_commit_exec) = node.as_any().downcast_ref::<IcebergCommitExec>()
        {
            let input = self.try_encode_plan(iceberg_commit_exec.input().clone())?;
            NodeKind::IcebergCommit(gen::IcebergCommitExecNode {
                input,
                table_url: iceberg_commit_exec.table_url().to_string(),
            })
        } else {
            return plan_err!("unsupported physical plan node: {node:?}");
        };
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
                is_pandas,
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
                    is_pandas,
                    Arc::new(config),
                )?;
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
            UdfKind::SparkTimestamp(gen::SparkTimestampUdf { timezone }) => {
                let udf = SparkTimestamp::try_new(timezone.map(Arc::from))?;
                return Ok(Arc::new(ScalarUDF::from(udf)));
            }
        };
        match name {
            "array_item_with_position" => {
                Ok(Arc::new(ScalarUDF::from(ArrayItemWithPosition::new())))
            }
            "array_min" => Ok(Arc::new(ScalarUDF::from(ArrayMin::new()))),
            "array_max" => Ok(Arc::new(ScalarUDF::from(ArrayMax::new()))),
            "arrays_zip" => Ok(Arc::new(ScalarUDF::from(ArraysZip::new()))),
            "bitmap_count" => Ok(Arc::new(ScalarUDF::from(BitmapCount::new()))),
            "convert_tz" => Ok(Arc::new(ScalarUDF::from(ConvertTz::new()))),
            "format_string" => Ok(Arc::new(ScalarUDF::from(FormatStringFunc::new()))),
            "greatest" => Ok(Arc::new(ScalarUDF::from(GreatestFunc::new()))),
            "least" => Ok(Arc::new(ScalarUDF::from(LeastFunc::new()))),
            "levenshtein" => Ok(Arc::new(ScalarUDF::from(Levenshtein::new()))),
            "make_valid_utf8" => Ok(Arc::new(ScalarUDF::from(MakeValidUtf8::new()))),
            "map_from_arrays" => Ok(Arc::new(ScalarUDF::from(MapFromArrays::new()))),
            "map_from_entries" => Ok(Arc::new(ScalarUDF::from(MapFromEntries::new()))),
            "multi_expr" => Ok(Arc::new(ScalarUDF::from(MultiExpr::new()))),
            "raise_error" => Ok(Arc::new(ScalarUDF::from(RaiseError::new()))),
            "random_poisson" => Ok(Arc::new(ScalarUDF::from(RandPoisson::new()))),
            "randn" => Ok(Arc::new(ScalarUDF::from(Randn::new()))),
            "random" | "rand" => Ok(Arc::new(ScalarUDF::from(Random::new()))),
            "spark_array" | "spark_make_array" | "array" => {
                Ok(Arc::new(ScalarUDF::from(SparkArray::new())))
            }
            "spark_concat" | "concat" => Ok(Arc::new(ScalarUDF::from(SparkConcat::new()))),
            "spark_from_csv" | "from_csv" => Ok(Arc::new(ScalarUDF::from(SparkFromCSV::new()))),
            "spark_to_number" | "to_number" => Ok(Arc::new(ScalarUDF::from(SparkToNumber::new()))),
            "spark_try_to_number" | "try_to_number" => {
                Ok(Arc::new(ScalarUDF::from(SparkTryToNumber::new())))
            }
            "spark_split" | "split" => Ok(Arc::new(ScalarUDF::from(SparkSplit::new()))),
            "spark_hex" | "hex" => Ok(Arc::new(ScalarUDF::from(SparkHex::new()))),
            "spark_unhex" | "unhex" => Ok(Arc::new(ScalarUDF::from(SparkUnHex::new()))),
            "spark_murmur3_hash" | "hash" => Ok(Arc::new(ScalarUDF::from(SparkMurmur3Hash::new()))),
            "spark_reverse" | "reverse" => Ok(Arc::new(ScalarUDF::from(SparkReverse::new()))),
            "spark_xxhash64" | "xxhash64" => Ok(Arc::new(ScalarUDF::from(SparkXxhash64::new()))),
            "spark_sha1" | "sha" | "sha1" => Ok(Arc::new(ScalarUDF::from(SparkSha1::new()))),
            "crc32" => Ok(Arc::new(ScalarUDF::from(SparkCrc32::new()))),
            "overlay" => Ok(Arc::new(ScalarUDF::from(OverlayFunc::new()))),
            "json_length" | "json_len" => Ok(sail_function::scalar::json::json_length_udf()),
            "json_as_text" => Ok(sail_function::scalar::json::json_as_text_udf()),
            "json_object_keys" | "json_keys" => {
                Ok(sail_function::scalar::json::json_object_keys_udf())
            }
            "spark_base64" | "base64" => Ok(Arc::new(ScalarUDF::from(SparkBase64::new()))),
            "spark_bround" | "bround" => Ok(Arc::new(ScalarUDF::from(SparkBRound::new()))),
            "spark_interval_div" => Ok(Arc::new(ScalarUDF::from(SparkIntervalDiv::new()))),
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
            "spark_to_binary" | "to_binary" => Ok(Arc::new(ScalarUDF::from(SparkToBinary::new()))),
            "spark_try_to_binary" | "try_to_binary" => {
                Ok(Arc::new(ScalarUDF::from(SparkTryToBinary::new())))
            }
            "spark_abs" | "abs" => Ok(Arc::new(ScalarUDF::from(SparkAbs::new()))),
            "spark_bit_count" | "bit_count" => Ok(Arc::new(ScalarUDF::from(SparkBitCount::new()))),
            "spark_bit_get" | "bit_get" | "getbit" => {
                Ok(Arc::new(ScalarUDF::from(SparkBitGet::new())))
            }
            "spark_bitwise_not" | "bitwise_not" => {
                Ok(Arc::new(ScalarUDF::from(SparkBitwiseNot::new())))
            }
            "spark_conv" | "conv" => Ok(Arc::new(ScalarUDF::from(SparkConv::new()))),
            "spark_signum" | "signum" => Ok(Arc::new(ScalarUDF::from(SparkSignum::new()))),
            "spark_last_day" | "last_day" => Ok(Arc::new(ScalarUDF::from(SparkLastDay::new()))),
            "spark_luhn_check" | "luhn_check" => {
                Ok(Arc::new(ScalarUDF::from(SparkLuhnCheck::new())))
            }
            "spark_next_day" | "next_day" => Ok(Arc::new(ScalarUDF::from(SparkNextDay::new()))),
            "spark_make_dt_interval" | "make_dt_interval" => {
                Ok(Arc::new(ScalarUDF::from(SparkMakeDtInterval::new())))
            }
            "spark_make_interval" | "make_interval" => {
                Ok(Arc::new(ScalarUDF::from(SparkMakeInterval::new())))
            }
            "spark_make_ym_interval" | "make_ym_interval" => {
                Ok(Arc::new(ScalarUDF::from(SparkMakeYmInterval::new())))
            }
            "spark_make_timestamp_ntz" | "make_timestamp_ntz" => {
                Ok(Arc::new(ScalarUDF::from(SparkMakeTimestampNtz::new())))
            }
            "spark_mask" | "mask" => Ok(Arc::new(ScalarUDF::from(SparkMask::new()))),
            "spark_sequence" | "sequence" => Ok(Arc::new(ScalarUDF::from(SparkSequence::new()))),
            "spark_shuffle" | "shuffle" => Ok(Arc::new(ScalarUDF::from(SparkShuffle::new()))),
            "spark_encode" | "encode" => Ok(Arc::new(ScalarUDF::from(SparkEncode::new()))),
            "spark_elt" | "elt" => Ok(Arc::new(ScalarUDF::from(SparkElt::new()))),
            "spark_decode" | "decode" => Ok(Arc::new(ScalarUDF::from(SparkDecode::new()))),
            "spark_bin" | "bin" => Ok(Arc::new(ScalarUDF::from(SparkBin::new()))),
            "spark_date" => Ok(Arc::new(ScalarUDF::from(SparkDate::new()))),
            "spark_year_month_interval" => {
                Ok(Arc::new(ScalarUDF::from(SparkYearMonthInterval::new())))
            }
            "spark_day_time_interval" => Ok(Arc::new(ScalarUDF::from(SparkDayTimeInterval::new()))),
            "spark_calendar_interval" => {
                Ok(Arc::new(ScalarUDF::from(SparkCalendarInterval::new())))
            }
            "spark_to_chrono_fmt" => Ok(Arc::new(ScalarUDF::from(SparkToChronoFmt::new()))),
            "spark_try_to_timestamp" | "try_to_timestamp" => {
                Ok(Arc::new(ScalarUDF::from(SparkTryToTimestamp::new())))
            }
            "spark_expm1" | "expm1" => Ok(Arc::new(ScalarUDF::from(SparkExpm1::new()))),
            "spark_pmod" | "pmod" => Ok(Arc::new(ScalarUDF::from(SparkPmod::new()))),
            "spark_ceil" | "ceil" => Ok(Arc::new(ScalarUDF::from(SparkCeil::new()))),
            "spark_floor" | "floor" => Ok(Arc::new(ScalarUDF::from(SparkFloor::new()))),
            "spark_to_utf8" => Ok(Arc::new(ScalarUDF::from(SparkToUtf8::new()))),
            "spark_to_large_utf8" => Ok(Arc::new(ScalarUDF::from(SparkToLargeUtf8::new()))),
            "spark_to_utf8_view" => Ok(Arc::new(ScalarUDF::from(SparkToUtf8View::new()))),
            "spark_try_add" | "try_add" => Ok(Arc::new(ScalarUDF::from(SparkTryAdd::new()))),
            "spark_try_divide" | "try_divide" => Ok(Arc::new(ScalarUDF::from(SparkTryDiv::new()))),
            "spark_try_mod" | "try_mod" => Ok(Arc::new(ScalarUDF::from(SparkTryMod::new()))),
            "spark_try_multiply" | "try_multiply" => {
                Ok(Arc::new(ScalarUDF::from(SparkTryMult::new())))
            }
            "spark_version" | "version" => Ok(Arc::new(ScalarUDF::from(SparkVersion::new()))),
            "spark_try_subtract" | "try_subtract" => {
                Ok(Arc::new(ScalarUDF::from(SparkTrySubtract::new())))
            }
            "spark_width_bucket" | "width_bucket" => {
                Ok(Arc::new(ScalarUDF::from(SparkWidthBucket::new())))
            }
            "str_to_map" => Ok(Arc::new(ScalarUDF::from(StrToMap::new()))),
            "parse_url" => Ok(Arc::new(ScalarUDF::from(ParseUrl::new()))),
            "try_parse_url" | "spark_try_parse_url" => {
                Ok(Arc::new(ScalarUDF::from(SparkTryParseUrl::new())))
            }
            "url_decode" => Ok(Arc::new(ScalarUDF::from(UrlDecode::new()))),
            "url_encode" => Ok(Arc::new(ScalarUDF::from(UrlEncode::new()))),
            _ => plan_err!("could not find scalar function: {name}"),
        }
    }

    fn try_encode_udf(&self, node: &ScalarUDF, buf: &mut Vec<u8>) -> Result<()> {
        // TODO: Implement custom registry to avoid codec for built-in functions
        let node_inner = node.inner().as_any();
        let udf_kind: UdfKind = if node_inner.is::<ArrayItemWithPosition>()
            || node_inner.is::<ArrayMax>()
            || node_inner.is::<ArrayMin>()
            || node_inner.is::<ArraysZip>()
            || node_inner.is::<BitmapCount>()
            || node_inner.is::<ConvertTz>()
            || node_inner.is::<FormatStringFunc>()
            || node_inner.is::<GreatestFunc>()
            || node_inner.is::<LeastFunc>()
            || node_inner.is::<Levenshtein>()
            || node_inner.is::<MakeValidUtf8>()
            || node_inner.is::<MapFromArrays>()
            || node_inner.is::<MapFromEntries>()
            || node_inner.is::<MultiExpr>()
            || node_inner.is::<OverlayFunc>()
            || node_inner.is::<ParseUrl>()
            || node_inner.is::<RaiseError>()
            || node_inner.is::<Randn>()
            || node_inner.is::<Random>()
            || node_inner.is::<RandPoisson>()
            || node_inner.is::<SparkAbs>()
            || node_inner.is::<SparkAESDecrypt>()
            || node_inner.is::<SparkAESEncrypt>()
            || node_inner.is::<SparkArray>()
            || node_inner.is::<SparkBase64>()
            || node_inner.is::<SparkBin>()
            || node_inner.is::<SparkBitCount>()
            || node_inner.is::<SparkBitGet>()
            || node_inner.is::<SparkBitwiseNot>()
            || node_inner.is::<SparkBRound>()
            || node_inner.is::<SparkCalendarInterval>()
            || node_inner.is::<SparkCeil>()
            || node_inner.is::<SparkConcat>()
            || node_inner.is::<SparkConv>()
            || node_inner.is::<SparkCrc32>()
            || node_inner.is::<SparkDate>()
            || node_inner.is::<SparkDayTimeInterval>()
            || node_inner.is::<SparkDecode>()
            || node_inner.is::<SparkElt>()
            || node_inner.is::<SparkEncode>()
            || node_inner.is::<SparkExpm1>()
            || node_inner.is::<SparkFloor>()
            || node_inner.is::<SparkFromCSV>()
            || node_inner.is::<SparkHex>()
            || node_inner.is::<SparkIntervalDiv>()
            || node_inner.is::<SparkLastDay>()
            || node_inner.is::<SparkLuhnCheck>()
            || node_inner.is::<SparkMakeDtInterval>()
            || node_inner.is::<SparkMakeInterval>()
            || node_inner.is::<SparkMakeTimestampNtz>()
            || node_inner.is::<SparkMakeYmInterval>()
            || node_inner.is::<SparkMask>()
            || node_inner.is::<SparkMurmur3Hash>()
            || node_inner.is::<SparkNextDay>()
            || node_inner.is::<SparkPmod>()
            || node_inner.is::<SparkReverse>()
            || node_inner.is::<SparkSequence>()
            || node_inner.is::<SparkShuffle>()
            || node_inner.is::<SparkSha1>()
            || node_inner.is::<SparkSignum>()
            || node_inner.is::<SparkSplit>()
            || node_inner.is::<SparkToBinary>()
            || node_inner.is::<SparkToChronoFmt>()
            || node_inner.is::<SparkToLargeUtf8>()
            || node_inner.is::<SparkToNumber>()
            || node_inner.is::<SparkToUtf8>()
            || node_inner.is::<SparkToUtf8View>()
            || node_inner.is::<SparkTryAdd>()
            || node_inner.is::<SparkTryAESDecrypt>()
            || node_inner.is::<SparkTryAESEncrypt>()
            || node_inner.is::<SparkTryDiv>()
            || node_inner.is::<SparkTryMod>()
            || node_inner.is::<SparkTryMult>()
            || node_inner.is::<SparkTryParseUrl>()
            || node_inner.is::<SparkTrySubtract>()
            || node_inner.is::<SparkTryToBinary>()
            || node_inner.is::<SparkTryToNumber>()
            || node_inner.is::<SparkTryToTimestamp>()
            || node_inner.is::<SparkUnbase64>()
            || node_inner.is::<SparkUnHex>()
            || node_inner.is::<SparkVersion>()
            || node_inner.is::<SparkWidthBucket>()
            || node_inner.is::<SparkXxhash64>()
            || node_inner.is::<SparkYearMonthInterval>()
            || node_inner.is::<StrToMap>()
            || node_inner.is::<UrlDecode>()
            || node_inner.is::<UrlEncode>()
            || node.name() == "json_as_text"
            || node.name() == "json_len"
            || node.name() == "json_length"
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
                is_pandas: func.is_pandas(),
                config: Some(config),
            })
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
        } else if let Some(func) = node.inner().as_any().downcast_ref::<TimestampNow>() {
            let timezone = func.timezone().to_string();
            let time_unit: gen_datafusion_common::TimeUnit = func.time_unit().into();
            let time_unit = time_unit.as_str_name().to_string();
            UdfKind::TimestampNow(gen::TimestampNowUdf {
                timezone,
                time_unit,
            })
        } else if let Some(func) = node.inner().as_any().downcast_ref::<SparkTimestamp>() {
            let timezone = func.timezone().map(|x| x.to_string());
            UdfKind::SparkTimestamp(gen::SparkTimestampUdf { timezone })
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
                "percentile_disc" => Ok(Arc::new(AggregateUDF::from(PercentileDisc::new()))),
                "skewness" => Ok(Arc::new(AggregateUDF::from(SkewnessFunc::new()))),
                "try_avg" => Ok(Arc::new(AggregateUDF::from(TryAvgFunction::new()))),
                "try_sum" => Ok(Arc::new(AggregateUDF::from(TrySumFunction::new()))),
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
                is_pandas,
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
                    is_pandas,
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
            || node.inner().as_any().is::<PercentileDisc>()
            || node.inner().as_any().is::<SkewnessFunc>()
            || node.inner().as_any().is::<TryAvgFunction>()
            || node.inner().as_any().is::<TrySumFunction>()
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
                is_pandas: func.is_pandas(),
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
    fn try_decode_physical_sink_mode(
        &self,
        proto_mode: gen::PhysicalSinkMode,
        schema: &Schema,
        ctx: &TaskContext,
    ) -> Result<PhysicalSinkMode> {
        let gen::PhysicalSinkMode { mode } = proto_mode;
        match mode {
            Some(gen::physical_sink_mode::Mode::Append(gen::AppendMode {})) => {
                Ok(PhysicalSinkMode::Append)
            }
            Some(gen::physical_sink_mode::Mode::Overwrite(gen::OverwriteMode {})) => {
                Ok(PhysicalSinkMode::Overwrite)
            }
            Some(gen::physical_sink_mode::Mode::OverwriteIf(gen::OverwriteIfMode {
                condition,
                source,
            })) => {
                let expr = self.try_decode_message(&condition)?;
                let expr = parse_physical_expr(&expr, ctx, schema, self)?;
                Ok(PhysicalSinkMode::OverwriteIf {
                    condition: PhysicalExprWithSource::new(expr, source),
                })
            }
            Some(gen::physical_sink_mode::Mode::ErrorIfExists(gen::ErrorIfExistsMode {})) => {
                Ok(PhysicalSinkMode::ErrorIfExists)
            }
            Some(gen::physical_sink_mode::Mode::IgnoreIfExists(gen::IgnoreIfExistsMode {})) => {
                Ok(PhysicalSinkMode::IgnoreIfExists)
            }
            Some(gen::physical_sink_mode::Mode::OverwritePartitions(
                gen::OverwritePartitionsMode {},
            )) => Ok(PhysicalSinkMode::OverwritePartitions),
            None => plan_err!("PhysicalSinkMode is missing"),
        }
    }

    fn try_encode_physical_sink_mode(
        &self,
        mode: &PhysicalSinkMode,
    ) -> Result<gen::PhysicalSinkMode> {
        let mode = match mode {
            PhysicalSinkMode::Append => gen::physical_sink_mode::Mode::Append(gen::AppendMode {}),
            PhysicalSinkMode::Overwrite => {
                gen::physical_sink_mode::Mode::Overwrite(gen::OverwriteMode {})
            }
            PhysicalSinkMode::OverwriteIf {
                condition: PhysicalExprWithSource { expr, source },
            } => {
                let expr = serialize_physical_expr(expr, self)?;
                gen::physical_sink_mode::Mode::OverwriteIf(gen::OverwriteIfMode {
                    condition: self.try_encode_message(expr)?,
                    source: source.clone(),
                })
            }
            PhysicalSinkMode::ErrorIfExists => {
                gen::physical_sink_mode::Mode::ErrorIfExists(gen::ErrorIfExistsMode {})
            }
            PhysicalSinkMode::IgnoreIfExists => {
                gen::physical_sink_mode::Mode::IgnoreIfExists(gen::IgnoreIfExistsMode {})
            }
            PhysicalSinkMode::OverwritePartitions => {
                gen::physical_sink_mode::Mode::OverwritePartitions(gen::OverwritePartitionsMode {})
            }
        };
        Ok(gen::PhysicalSinkMode { mode: Some(mode) })
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

    fn try_decode_projection(&self, projection: &[u64]) -> Result<Vec<usize>> {
        let projection = projection
            .iter()
            .map(|x| {
                usize::try_from(*x)
                    .map_err(|_| plan_datafusion_err!("failed to decode projection index: {x}"))
            })
            .collect::<Result<_>>()?;
        Ok(projection)
    }

    fn try_encode_projection(&self, projection: &[usize]) -> Result<Vec<u64>> {
        let projection = projection
            .iter()
            .map(|x| {
                u64::try_from(*x)
                    .map_err(|_| plan_datafusion_err!("failed to encode projection index: {x}"))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(projection)
    }

    fn try_decode_lex_ordering(
        &self,
        lex_ordering: &gen::LexOrdering,
        schema: &Schema,
        ctx: &TaskContext,
    ) -> Result<LexOrdering> {
        let lex_ordering: Vec<PhysicalSortExprNode> = lex_ordering
            .values
            .iter()
            .map(|x| self.try_decode_message(x))
            .collect::<Result<_>>()?;
        let lex_ordering = LexOrdering::new(
            parse_physical_sort_exprs(&lex_ordering, ctx, schema, self)
                .map_err(|e| plan_datafusion_err!("failed to decode lex ordering: {e}"))?,
        );
        match lex_ordering {
            Some(lex_ordering) => Ok(lex_ordering),
            None => plan_err!("failed to decode lex ordering: invalid sort expressions"),
        }
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
        schema: &Schema,
        ctx: &TaskContext,
    ) -> Result<Vec<LexOrdering>> {
        let mut result: Vec<LexOrdering> = vec![];
        for lex_ordering in lex_orderings {
            let lex_ordering = self.try_decode_lex_ordering(lex_ordering, schema, ctx)?;
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

    fn try_decode_file_compression_type(&self, variant: i32) -> Result<FileCompressionType> {
        Ok(self.try_decode_compression_type_variant(variant)?.into())
    }

    fn try_decode_compression_type_variant(&self, variant: i32) -> Result<CompressionTypeVariant> {
        let variant = gen::CompressionTypeVariant::try_from(variant)
            .map_err(|e| plan_datafusion_err!("failed to decode compression type variant: {e}"))?;
        let variant = match variant {
            gen::CompressionTypeVariant::Gzip => CompressionTypeVariant::GZIP,
            gen::CompressionTypeVariant::Bzip2 => CompressionTypeVariant::BZIP2,
            gen::CompressionTypeVariant::Xz => CompressionTypeVariant::XZ,
            gen::CompressionTypeVariant::Zstd => CompressionTypeVariant::ZSTD,
            gen::CompressionTypeVariant::Uncompressed => CompressionTypeVariant::UNCOMPRESSED,
        };
        Ok(variant)
    }

    fn try_encode_file_compression_type(
        &self,
        file_compression_type: FileCompressionType,
    ) -> Result<i32> {
        let variant: CompressionTypeVariant = file_compression_type.into();
        self.try_encode_compression_type_variant(variant)
    }

    fn try_encode_compression_type_variant(&self, variant: CompressionTypeVariant) -> Result<i32> {
        let variant = match variant {
            CompressionTypeVariant::GZIP => gen::CompressionTypeVariant::Gzip,
            CompressionTypeVariant::BZIP2 => gen::CompressionTypeVariant::Bzip2,
            CompressionTypeVariant::XZ => gen::CompressionTypeVariant::Xz,
            CompressionTypeVariant::ZSTD => gen::CompressionTypeVariant::Zstd,
            CompressionTypeVariant::UNCOMPRESSED => gen::CompressionTypeVariant::Uncompressed,
        };
        Ok(variant as i32)
    }

    fn try_decode_plan(&self, buf: &[u8], ctx: &TaskContext) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = PhysicalPlanNode::decode(buf)
            .map_err(|e| plan_datafusion_err!("failed to decode plan: {e}"))?;
        plan.try_into_physical_plan(ctx, self)
    }

    fn try_encode_plan(&self, plan: Arc<dyn ExecutionPlan>) -> Result<Vec<u8>> {
        let plan = PhysicalPlanNode::try_from_physical_plan(plan, self)?;
        Ok(plan.encode_to_vec())
    }

    fn try_decode_partitioning(
        &self,
        buf: &[u8],
        schema: &Schema,
        ctx: &TaskContext,
    ) -> Result<Partitioning> {
        let partitioning = self.try_decode_message(buf)?;
        parse_protobuf_partitioning(Some(&partitioning), ctx, schema, self)?
            .ok_or_else(|| plan_datafusion_err!("no partitioning found"))
    }

    fn try_encode_partitioning(&self, partitioning: &Partitioning) -> Result<Vec<u8>> {
        let partitioning = serialize_partitioning(partitioning, self)?;
        self.try_encode_message(partitioning)
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
        Ok(message.encode_to_vec())
    }
}
