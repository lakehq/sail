use std::any::Any;
use std::convert::TryInto;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema, TimeUnit};
use datafusion::common::parsers::CompressionTypeVariant;
use datafusion::common::{
    plan_datafusion_err, plan_err, Constraint, Constraints, JoinSide, Result, ScalarValue,
    Statistics,
};
use datafusion::config::ConfigOptions;
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
use datafusion::logical_expr::{
    AggregateUDF, AggregateUDFImpl, HigherOrderUDF, LambdaParametersProgress, ScalarUDF,
    ScalarUDFImpl, ValueOrLambda, WindowUDF,
};
use datafusion::physical_expr::equivalence::{EquivalenceClass, EquivalenceGroup};
use datafusion::physical_expr::expressions::{LambdaExpr, LambdaVariable};
use datafusion::physical_expr::{
    AcrossPartitions, ConstExpr, EquivalenceProperties, HigherOrderFunctionExpr, LexOrdering,
    LexRequirement, Partitioning, PhysicalExpr, PhysicalSortExpr,
};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::joins::utils::{ColumnIndex, JoinFilter};
use datafusion::physical_plan::joins::SortMergeJoinExec;
use datafusion::physical_plan::recursive_query::RecursiveQueryExec;
use datafusion::physical_plan::sorts::partial_sort::PartialSortExec;
use datafusion::physical_plan::work_table::WorkTableExec;
use datafusion::physical_plan::{ExecutionPlan, PlanProperties};
use datafusion_proto::generated::datafusion_common as gen_datafusion_common;
use datafusion_proto::physical_plan::from_proto::{
    parse_physical_expr, parse_physical_sort_exprs, parse_protobuf_file_scan_config,
    parse_protobuf_file_scan_schema, parse_protobuf_partitioning,
};
use datafusion_proto::physical_plan::to_proto::{
    serialize_file_scan_config, serialize_partitioning, serialize_physical_expr_with_converter,
    serialize_physical_sort_exprs,
};
use datafusion_proto::physical_plan::{
    AsExecutionPlan, PhysicalExtensionCodec, PhysicalPlanDecodeContext,
    PhysicalProtoConverterExtension,
};
use datafusion_proto::protobuf::{
    physical_expr_node, JoinType as ProtoJoinType, PhysicalExprNode, PhysicalExtensionExprNode,
    PhysicalPlanNode, PhysicalSortExprNode,
};
use datafusion_spark::function::aggregate::try_sum::SparkTrySum;
use datafusion_spark::function::array::shuffle::SparkShuffle;
use datafusion_spark::function::bitmap::bitmap_count::BitmapCount;
use datafusion_spark::function::bitwise::bit_count::SparkBitCount;
use datafusion_spark::function::bitwise::bit_get::SparkBitGet;
use datafusion_spark::function::bitwise::bitwise_not::SparkBitwiseNot;
use datafusion_spark::function::datetime::make_dt_interval::SparkMakeDtInterval;
use datafusion_spark::function::datetime::make_interval::SparkMakeInterval;
use datafusion_spark::function::hash::crc32::SparkCrc32;
use datafusion_spark::function::hash::sha1::SparkSha1;
use datafusion_spark::function::map::map_from_arrays::MapFromArrays;
use datafusion_spark::function::map::map_from_entries::MapFromEntries;
use datafusion_spark::function::math::expm1::SparkExpm1;
use datafusion_spark::function::math::hex::SparkHex;
use datafusion_spark::function::math::width_bucket::SparkWidthBucket;
use datafusion_spark::function::string::elt::SparkElt;
use datafusion_spark::function::string::format_string::FormatStringFunc;
use datafusion_spark::function::string::luhn_check::SparkLuhnCheck;
use datafusion_spark::function::url::try_url_decode::TryUrlDecode;
use datafusion_spark::function::url::url_decode::UrlDecode;
use datafusion_spark::function::url::url_encode::UrlEncode;
use prost::Message;
use sail_catalog_system::physical_plan::SystemTableExec;
use sail_common_datafusion::array::record_batch::{read_record_batches, write_record_batches};
use sail_common_datafusion::catalog::{
    CatalogPartitionField, LakehouseExecutionContext, PartitionTransform,
};
use sail_common_datafusion::datasource::PhysicalSinkMode;
use sail_common_datafusion::schema_evolution::SchemaEvolutionCastColumnExpr;
use sail_common_datafusion::system::catalog::SystemTable;
use sail_common_datafusion::udf::StreamUDF;
use sail_data_source::formats::binary::source::BinarySource;
use sail_data_source::formats::console::ConsoleSinkExec;
use sail_data_source::formats::python::{
    InputPartition, PythonDataSourceExec, PythonDataSourceWriteCommitExec,
    PythonDataSourceWriteExec,
};
use sail_data_source::formats::rate::RateSourceExec;
use sail_data_source::formats::socket::{SocketReadOptions, SocketSourceExec};
use sail_data_source::formats::text::source::TextSource;
use sail_data_source::formats::text::writer::{TextSink, TextWriterOptions};
use sail_data_source::listing::delete::FileDeleteExec;
use sail_data_source::options::gen::RateReadOptions;
use sail_delta_lake::physical_plan::{
    DeletionVectorRowsWriterExec, DeletionVectorWriterExec, DeltaCommitContext, DeltaCommitExec,
    DeltaDiscoveryExec, DeltaLogReplayExec, DeltaMetadataStatsExec, DeltaRemoveActionsExec,
    DeltaScanByAddsExec, DeltaSnapshotContext, DeltaWriteContext, DeltaWriterExec,
    RelaxedTzCastExec,
};
use sail_delta_lake::spec::{Action, ColumnMappingMode, DeltaOperation, StructType};
use sail_function::aggregate::bitmap_and_agg::BitmapAndAggFunction;
use sail_function::aggregate::bitmap_construct_agg::BitmapConstructAggFunction;
use sail_function::aggregate::bitmap_or_agg::BitmapOrAggFunction;
use sail_function::aggregate::count_min_sketch::CountMinSketchFunction;
use sail_function::aggregate::grouping_id::GroupingIdFunction;
use sail_function::aggregate::histogram_numeric::HistogramNumericFunction;
use sail_function::aggregate::hll_sketch::{HllSketchAggFunction, HllUnionAggFunction};
use sail_function::aggregate::kurtosis::KurtosisFunction;
use sail_function::aggregate::max_min_by::{MaxByFunction, MinByFunction};
use sail_function::aggregate::mode::ModeFunction;
use sail_function::aggregate::percentile::PercentileFunction;
use sail_function::aggregate::percentile_disc::PercentileDisc;
use sail_function::aggregate::product::ProductFunction;
use sail_function::aggregate::regr::{Regr, RegrType};
use sail_function::aggregate::schema_of_variant_agg::SchemaOfVariantAggFunction;
use sail_function::aggregate::skewness::SkewnessFunc;
use sail_function::aggregate::theta_sketch::{
    ThetaIntersectionAggFunction, ThetaSketchAggFunction, ThetaUnionAggFunction,
};
use sail_function::aggregate::try_avg::TryAvgFunction;
use sail_function::scalar::array::array_intersect::ArrayIntersect;
use sail_function::scalar::array::array_position::SparkArrayPosition;
use sail_function::scalar::array::arrays_zip::ArraysZip;
use sail_function::scalar::array::spark_array::SparkArray;
use sail_function::scalar::array::spark_array_compact::SparkArrayCompact;
use sail_function::scalar::array::spark_array_filter::SparkArrayFilter;
use sail_function::scalar::array::spark_array_item_with_position::ArrayItemWithPosition;
use sail_function::scalar::array::spark_array_min_max::{ArrayMax, ArrayMin};
use sail_function::scalar::array::spark_sequence::SparkSequence;
use sail_function::scalar::array_struct_field::ArrayStructField;
use sail_function::scalar::collection::spark_concat::SparkConcat;
use sail_function::scalar::collection::spark_reverse::SparkReverse;
use sail_function::scalar::csv::spark_from_csv::SparkFromCSV;
use sail_function::scalar::csv::spark_to_csv::SparkToCsv;
use sail_function::scalar::csv::SparkSchemaOfCsv;
use sail_function::scalar::datetime::convert_tz::ConvertTz;
use sail_function::scalar::datetime::negate_duration::NegateDuration;
use sail_function::scalar::datetime::spark_date::SparkDate;
use sail_function::scalar::datetime::spark_date_trunc::SparkDateTrunc;
use sail_function::scalar::datetime::spark_interval::{
    SparkCalendarInterval, SparkDayTimeInterval, SparkYearMonthInterval,
};
use sail_function::scalar::datetime::spark_last_day::SparkLastDay;
use sail_function::scalar::datetime::spark_make_time::SparkMakeTime;
use sail_function::scalar::datetime::spark_make_timestamp_ntz::SparkMakeTimestampNtz;
use sail_function::scalar::datetime::spark_make_ym_interval::SparkMakeYmInterval;
use sail_function::scalar::datetime::spark_next_day::SparkNextDay;
use sail_function::scalar::datetime::spark_time::SparkTime;
use sail_function::scalar::datetime::spark_time_diff::SparkTimeDiff;
use sail_function::scalar::datetime::spark_time_trunc::SparkTimeTrunc;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::datetime::spark_to_chrono_fmt::SparkToChronoFmt;
use sail_function::scalar::datetime::spark_try_to_timestamp::SparkTryToTimestamp;
use sail_function::scalar::datetime::spark_unix_timestamp::SparkUnixTimestamp;
use sail_function::scalar::datetime::spark_window_buckets::SparkWindowBuckets;
use sail_function::scalar::datetime::spark_year::SparkYear;
use sail_function::scalar::datetime::timestamp_now::TimestampNow;
use sail_function::scalar::drop_struct_field::DropStructField;
use sail_function::scalar::explode::{explode_name_to_kind, Explode};
use sail_function::scalar::geo::st_asbinary::StAsBinary;
use sail_function::scalar::geo::st_geogfromwkb::StGeogFromWKB;
use sail_function::scalar::geo::st_geomfromwkb::StGeomFromWKB;
use sail_function::scalar::hash::spark_murmur3_hash::SparkMurmur3Hash;
use sail_function::scalar::hash::spark_xxhash64::SparkXxhash64;
use sail_function::scalar::json::{SparkFromJson, SparkSchemaOfJson, SparkToJson};
use sail_function::scalar::map::map_entries::SparkMapEntries;
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
use sail_function::scalar::math::spark_negative::SparkNegative;
use sail_function::scalar::math::spark_pmod::SparkPmod;
use sail_function::scalar::math::spark_signum::SparkSignum;
use sail_function::scalar::math::spark_try_add::SparkTryAdd;
use sail_function::scalar::math::spark_try_div::SparkTryDiv;
use sail_function::scalar::math::spark_try_mod::SparkTryMod;
use sail_function::scalar::math::spark_try_mult::SparkTryMult;
use sail_function::scalar::math::spark_try_subtract::SparkTrySubtract;
use sail_function::scalar::math::spark_unhex::SparkUnHex;
use sail_function::scalar::math::spark_uniform::SparkUniform;
use sail_function::scalar::misc::hll_sketch::{HllSketchEstimateFunction, HllUnionFunction};
use sail_function::scalar::misc::raise_error::RaiseError;
use sail_function::scalar::misc::spark_aes::{
    SparkAESDecrypt, SparkAESEncrypt, SparkTryAESDecrypt, SparkTryAESEncrypt,
};
use sail_function::scalar::misc::theta_sketch::{
    ThetaDifferenceFunction, ThetaIntersectionFunction, ThetaSketchEstimateFunction,
    ThetaUnionFunction,
};
use sail_function::scalar::misc::version::SparkVersion;
use sail_function::scalar::multi_expr::MultiExpr;
use sail_function::scalar::predicate::rewrite_like_pattern::RewriteLikePatternFunc;
use sail_function::scalar::spark_struct_rename::SparkStructRename;
use sail_function::scalar::spark_to_string::{SparkToLargeUtf8, SparkToUtf8, SparkToUtf8View};
use sail_function::scalar::string::format_number::FormatNumber;
use sail_function::scalar::string::levenshtein::Levenshtein;
use sail_function::scalar::string::make_valid_utf8::MakeValidUtf8;
use sail_function::scalar::string::randstr::Randstr;
use sail_function::scalar::string::soundex::Soundex;
use sail_function::scalar::string::spark_base64::{SparkBase64, SparkUnbase64};
use sail_function::scalar::string::spark_concat_ws::SparkConcatWs;
use sail_function::scalar::string::spark_encode_decode::{SparkDecode, SparkEncode};
use sail_function::scalar::string::spark_mask::SparkMask;
use sail_function::scalar::string::spark_quote::SparkQuote;
use sail_function::scalar::string::spark_regexp_extract_all::{
    SparkRegexpExtract, SparkRegexpExtractAll,
};
use sail_function::scalar::string::spark_sentences::SparkSentences;
use sail_function::scalar::string::spark_split::SparkSplit;
use sail_function::scalar::string::spark_to_binary::{SparkToBinary, SparkTryToBinary};
use sail_function::scalar::string::spark_to_char::SparkToChar;
use sail_function::scalar::string::spark_to_number::SparkToNumber;
use sail_function::scalar::struct_function::StructFunction;
use sail_function::scalar::update_struct_field::UpdateStructField;
use sail_function::scalar::url::parse_url::ParseUrl;
use sail_function::scalar::url::spark_try_parse_url::SparkTryParseUrl;
use sail_function::scalar::variant::spark_cast_to_variant::SparkCastToVariant;
use sail_function::scalar::variant::spark_is_variant_null::SparkIsVariantNullUdf;
use sail_function::scalar::variant::spark_parse_json::SparkParseJson;
use sail_function::scalar::variant::spark_schema_of_variant::SparkSchemaOfVariantUdf;
use sail_function::scalar::variant::spark_to_variant_object::SparkToVariantObjectUdf;
use sail_function::scalar::variant::spark_variant_explode::SparkVariantExplodeUdf;
use sail_function::scalar::variant::spark_variant_get::SparkVariantGet;
use sail_function::scalar::variant::spark_variant_to_json::SparkVariantToJsonUdf;
use sail_function::scalar::xml::to_xml::SparkToXml;
use sail_function::scalar::xml::xpath::Xpath;
use sail_function::scalar::xml::xpath_typed::{xpath_typed_name_to_kind, XpathTyped};
use sail_function::window::SparkNtile;
use sail_iceberg::physical_plan::{
    IcebergCommitExec, IcebergDeleteApplyExec, IcebergDiscoveryExec, IcebergManifestScanExec,
    IcebergScanByDataFilesExec, IcebergWriterExec,
};
use sail_iceberg::IcebergWriterExecOptions;
use sail_logical_plan::range::Range;
use sail_logical_plan::show_string::{ShowStringFormat, ShowStringStyle};
use sail_physical_plan::barrier::BarrierExec;
use sail_physical_plan::catalog_command::CatalogCommandExec;
use sail_physical_plan::coalesce::CoalesceExec;
use sail_physical_plan::map_partitions::MapPartitionsExec;
use sail_physical_plan::merge_cardinality_check::MergeCardinalityCheckExec;
use sail_physical_plan::monotonic_id::MonotonicIdExec;
use sail_physical_plan::range::RangeExec;
use sail_physical_plan::schema_pivot::SchemaPivotExec;
use sail_physical_plan::show_string::ShowStringExec;
use sail_physical_plan::spark_partition_id::SparkPartitionIdExec;
use sail_physical_plan::streaming::collector::StreamCollectorExec;
use sail_physical_plan::streaming::filter::StreamFilterExec;
use sail_physical_plan::streaming::limit::StreamLimitExec;
use sail_physical_plan::streaming::source_adapter::StreamSourceAdapterExec;
use sail_python_udf::config::PySparkUdfConfig;
use sail_python_udf::udf::pyspark_batch_collector::PySparkBatchCollectorUDF;
use sail_python_udf::udf::pyspark_cogroup_map_udf::PySparkCoGroupMapUDF;
use sail_python_udf::udf::pyspark_group_map_udf::{PySparkGroupMapMode, PySparkGroupMapUDF};
use sail_python_udf::udf::pyspark_map_iter_udf::{PySparkMapIterKind, PySparkMapIterUDF};
use sail_python_udf::udf::pyspark_udaf::{PySparkGroupAggKind, PySparkGroupAggregateUDF};
use sail_python_udf::udf::pyspark_udf::{PySparkUDF, PySparkUdfKind};
use sail_python_udf::udf::pyspark_udtf::{PySparkUDTF, PySparkUdtfKind};
use serde::de::DeserializeOwned;
use serde::Serialize;
use url::Url;

use crate::plan::gen::extended_aggregate_udf::UdafKind;
use crate::plan::gen::extended_physical_expr_node::ExprKind;
use crate::plan::gen::extended_physical_plan_node::NodeKind;
use crate::plan::gen::extended_scalar_udf::UdfKind;
use crate::plan::gen::extended_stream_udf::StreamUdfKind;
use crate::plan::gen::extended_window_udf::UdwfKind;
use crate::plan::gen::higher_order_udf::UdfKind as HigherOrderUdfKind;
use crate::plan::gen::{
    CastColumnExprNode, ExtendedAggregateUdf, ExtendedPhysicalExprNode, ExtendedPhysicalPlanNode,
    ExtendedScalarUdf, ExtendedStreamUdf, ExtendedWindowUdf, HigherOrderFunctionExprNode,
    LambdaExprNode, LambdaVariableExprNode,
};
use crate::plan::{gen, StageInputExec};

pub struct RemoteExecutionCodec;

impl Debug for RemoteExecutionCodec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "RemoteExecutionCodec")
    }
}

#[derive(Debug, Default)]
struct RemotePhysicalProtoConverter;

impl PhysicalProtoConverterExtension for RemotePhysicalProtoConverter {
    fn proto_to_execution_plan(
        &self,
        proto: &PhysicalPlanNode,
        ctx: &PhysicalPlanDecodeContext<'_>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.default_proto_to_execution_plan(proto, ctx)
    }

    fn execution_plan_to_proto(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        codec: &dyn PhysicalExtensionCodec,
    ) -> Result<PhysicalPlanNode> {
        PhysicalPlanNode::try_from_physical_plan_with_converter(Arc::clone(plan), codec, self)
    }

    fn physical_expr_to_proto(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        codec: &dyn PhysicalExtensionCodec,
    ) -> Result<PhysicalExprNode> {
        if let Some(hof) = expr.downcast_ref::<HigherOrderFunctionExpr>() {
            return self.higher_order_expr_to_proto(expr, hof, codec);
        }
        if let Some(lambda) = expr.downcast_ref::<LambdaExpr>() {
            return self.lambda_expr_to_proto(expr, lambda, codec);
        }
        if let Some(var) = expr.downcast_ref::<LambdaVariable>() {
            return extension_expr_to_proto(
                expr,
                ExprKind::LambdaVariable(LambdaVariableExprNode {
                    index: var.index() as u64,
                }),
                vec![],
            );
        }

        serialize_physical_expr_with_converter(expr, codec, self)
    }

    fn proto_to_physical_expr(
        &self,
        proto: &PhysicalExprNode,
        input_schema: &Schema,
        ctx: &PhysicalPlanDecodeContext<'_>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        match decode_remote_expr_kind(proto)? {
            Some((ExprKind::HigherOrderFunction(node), inputs)) => {
                self.higher_order_proto_to_expr(node, inputs, input_schema, ctx)
            }
            Some((ExprKind::LambdaVariable(node), _)) => {
                let index = usize::try_from(node.index).map_err(|_| {
                    plan_datafusion_err!(
                        "LambdaVariable index {} does not fit in usize",
                        node.index
                    )
                })?;
                let field = input_schema
                    .fields()
                    .get(index)
                    .ok_or_else(|| {
                        plan_datafusion_err!(
                            "LambdaVariable index {index} out of bounds for schema with {} fields",
                            input_schema.fields().len()
                        )
                    })?
                    .clone();
                Ok(Arc::new(LambdaVariable::new(index, field)))
            }
            Some((ExprKind::Lambda(_), _)) => {
                plan_err!("lambda expressions must be decoded as higher-order function arguments")
            }
            _ => self.default_proto_to_physical_expr(proto, input_schema, ctx),
        }
    }
}

impl RemotePhysicalProtoConverter {
    fn higher_order_expr_to_proto(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        hof: &HigherOrderFunctionExpr,
        codec: &dyn PhysicalExtensionCodec,
    ) -> Result<PhysicalExprNode> {
        let inputs = expr
            .children()
            .into_iter()
            .map(|child| self.physical_expr_to_proto(child, codec))
            .collect::<Result<_>>()?;
        extension_expr_to_proto(
            expr,
            ExprKind::HigherOrderFunction(HigherOrderFunctionExprNode {
                udf: Some(try_encode_higher_order_udf(hof)?),
            }),
            inputs,
        )
    }

    fn lambda_expr_to_proto(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        lambda: &LambdaExpr,
        codec: &dyn PhysicalExtensionCodec,
    ) -> Result<PhysicalExprNode> {
        let inputs = expr
            .children()
            .into_iter()
            .map(|child| self.physical_expr_to_proto(child, codec))
            .collect::<Result<_>>()?;
        extension_expr_to_proto(
            expr,
            ExprKind::Lambda(LambdaExprNode {
                params: lambda.params().to_vec(),
            }),
            inputs,
        )
    }

    fn higher_order_proto_to_expr(
        &self,
        node: HigherOrderFunctionExprNode,
        inputs: &[PhysicalExprNode],
        input_schema: &Schema,
        ctx: &PhysicalPlanDecodeContext<'_>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let fun = try_decode_higher_order_udf(node.udf)?;
        let mut decoded_values = Vec::with_capacity(inputs.len());
        let mut value_or_lambda = Vec::with_capacity(inputs.len());

        for input in inputs {
            if lambda_proto_parts(input)?.is_some() {
                decoded_values.push(None);
                value_or_lambda.push(ValueOrLambda::Lambda(None));
            } else {
                let expr = self.proto_to_physical_expr(input, input_schema, ctx)?;
                value_or_lambda.push(ValueOrLambda::Value(expr.return_field(input_schema)?));
                decoded_values.push(Some(expr));
            }
        }

        let param_sets = match fun.lambda_parameters(0, &value_or_lambda)? {
            LambdaParametersProgress::Complete(params) => params,
            LambdaParametersProgress::Partial(_) => {
                return plan_err!("`{}` returned partial lambda parameters", fun.name())
            }
        };

        let mut lambda_index = 0usize;
        let args = inputs
            .iter()
            .enumerate()
            .map(|(index, input)| {
                if let Some((params, body)) = lambda_proto_parts(input)? {
                    let fields = param_sets.get(lambda_index).ok_or_else(|| {
                        plan_datafusion_err!(
                            "missing lambda parameter fields for `{}`",
                            fun.name()
                        )
                    })?;
                    lambda_index += 1;

                    let schema = extend_lambda_schema(input_schema, &params, fields);
                    let body = self.proto_to_physical_expr(body, &schema, ctx)?;
                    Ok(Arc::new(LambdaExpr::try_new(params, body)?) as Arc<dyn PhysicalExpr>)
                } else {
                    decoded_values[index]
                        .clone()
                        .ok_or_else(|| plan_datafusion_err!("missing decoded higher-order argument"))
                }
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Arc::new(HigherOrderFunctionExpr::try_new_with_schema(
            fun,
            args,
            input_schema,
            Arc::clone(ctx.task_ctx().session_config().options()),
        )?))
    }
}

pub(crate) fn encode_physical_plan_for_remote(
    plan: Arc<dyn ExecutionPlan>,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<Vec<u8>> {
    let converter = RemotePhysicalProtoConverter;
    Ok(PhysicalPlanNode::try_from_physical_plan_with_converter(plan, codec, &converter)?
        .encode_to_vec())
}

pub(crate) fn decode_physical_plan_for_remote(
    buf: &[u8],
    ctx: &TaskContext,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<Arc<dyn ExecutionPlan>> {
    let plan = PhysicalPlanNode::decode(buf)
        .map_err(|e| plan_datafusion_err!("failed to decode plan: {e}"))?;
    let converter = RemotePhysicalProtoConverter;
    plan.try_into_physical_plan_with_converter(ctx, codec, &converter)
}

pub(crate) fn encode_physical_expr_for_remote(
    expr: &Arc<dyn PhysicalExpr>,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<Vec<u8>> {
    let converter = RemotePhysicalProtoConverter;
    Ok(converter.physical_expr_to_proto(expr, codec)?.encode_to_vec())
}

pub(crate) fn decode_physical_expr_for_remote(
    buf: &[u8],
    ctx: &TaskContext,
    schema: &Schema,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<Arc<dyn PhysicalExpr>> {
    let proto = PhysicalExprNode::decode(buf)
        .map_err(|e| plan_datafusion_err!("failed to decode expr: {e}"))?;
    let converter = RemotePhysicalProtoConverter;
    converter.proto_to_physical_expr(&proto, schema, &PhysicalPlanDecodeContext::new(ctx, codec))
}

fn extension_expr_to_proto(
    expr: &Arc<dyn PhysicalExpr>,
    expr_kind: ExprKind,
    inputs: Vec<PhysicalExprNode>,
) -> Result<PhysicalExprNode> {
    let node = ExtendedPhysicalExprNode {
        expr_kind: Some(expr_kind),
    };
    Ok(PhysicalExprNode {
        expr_id: expr.expression_id(),
        expr_type: Some(physical_expr_node::ExprType::Extension(
            PhysicalExtensionExprNode {
                expr: node.encode_to_vec(),
                inputs,
            },
        )),
    })
}

fn decode_remote_expr_kind(
    proto: &PhysicalExprNode,
) -> Result<Option<(ExprKind, &[PhysicalExprNode])>> {
    let Some(physical_expr_node::ExprType::Extension(extension)) = proto.expr_type.as_ref() else {
        return Ok(None);
    };
    let node = ExtendedPhysicalExprNode::decode(extension.expr.as_slice())
        .map_err(|e| plan_datafusion_err!("failed to decode physical expr: {e}"))?;
    let expr_kind = node
        .expr_kind
        .ok_or_else(|| plan_datafusion_err!("missing physical expr node"))?;
    Ok(Some((expr_kind, extension.inputs.as_slice())))
}

fn lambda_proto_parts(proto: &PhysicalExprNode) -> Result<Option<(Vec<String>, &PhysicalExprNode)>> {
    match decode_remote_expr_kind(proto)? {
        Some((ExprKind::Lambda(node), inputs)) => {
            let [body] = inputs else {
                return plan_err!("LambdaExpr expects exactly one input, got {}", inputs.len());
            };
            Ok(Some((node.params, body)))
        }
        _ => Ok(None),
    }
}

fn extend_lambda_schema(base: &Schema, params: &[String], fields: &[FieldRef]) -> Schema {
    let mut output: Vec<FieldRef> = base.fields().iter().map(Arc::clone).collect();
    for (name, field) in params.iter().zip(fields) {
        output.push(Arc::new(field.as_ref().clone().with_name(name)));
    }
    Schema::new(output)
}

fn try_encode_higher_order_udf(hof: &HigherOrderFunctionExpr) -> Result<gen::HigherOrderUdf> {
    let udf_inner = hof.fun().inner().as_ref() as &dyn std::any::Any;
    let udf_kind = if let Some(filter) = udf_inner.downcast_ref::<SparkArrayFilter>() {
        HigherOrderUdfKind::Filter(gen::SparkArrayFilterUdf {
            index_first: filter.is_index_first(),
        })
    } else {
        return plan_err!("unsupported higher-order function: {}", hof.name());
    };
    Ok(gen::HigherOrderUdf {
        udf_kind: Some(udf_kind),
    })
}

fn try_decode_higher_order_udf(udf: Option<gen::HigherOrderUdf>) -> Result<Arc<HigherOrderUDF>> {
    let udf_kind = udf
        .and_then(|udf| udf.udf_kind)
        .ok_or_else(|| plan_datafusion_err!("missing higher-order function UDF"))?;
    Ok(match udf_kind {
        HigherOrderUdfKind::Filter(gen::SparkArrayFilterUdf { index_first }) => {
            if index_first {
                Arc::new(HigherOrderUDF::new_from_impl(
                    SparkArrayFilter::new_index_first(),
                ))
            } else {
                Arc::new(HigherOrderUDF::new_from_impl(SparkArrayFilter::new()))
            }
        }
    })
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
                projection,
            }) => {
                let schema = self.try_decode_schema(&schema)?;
                let projection = self.try_decode_projection(&projection)?;
                Ok(Arc::new(RangeExec::try_new(
                    Range { start, end, step },
                    num_partitions as usize,
                    Arc::new(schema),
                    projection,
                )?))
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
                eq_properties,
                partitioning,
                bounded,
            }) => {
                let eq_properties = match eq_properties {
                    Some(x) => self.try_decode_equivalence_properties(&x, ctx)?,
                    None => return plan_err!("no equivalence properties found for stage input"),
                };
                let partitioning =
                    self.try_decode_partitioning(&partitioning, eq_properties.schema(), ctx)?;
                let boundedness = if bounded {
                    Boundedness::Bounded
                } else {
                    Boundedness::Unbounded {
                        requires_infinite_memory: false,
                    }
                };
                let properties = Arc::new(PlanProperties::new(
                    eq_properties,
                    partitioning,
                    EmissionType::Both,
                    boundedness,
                ));
                let node = StageInputExec::new(input as usize, properties);
                Ok(Arc::new(node))
            }
            NodeKind::SystemTable(gen::SystemTableExecNode {
                table,
                projection,
                filters,
                fetch,
            }) => {
                let table: SystemTable =
                    serde_json::from_str(&table).map_err(|e| plan_datafusion_err!("{e}"))?;
                let schema = table.schema();
                let projection =
                    projection.map(|x| x.columns.into_iter().map(|c| c as usize).collect());
                let filters = filters
                    .iter()
                    .map(|expr| {
                        parse_physical_expr(&self.try_decode_message(expr)?, ctx, &schema, self)
                    })
                    .collect::<Result<Vec<_>>>()?;
                let fetch = fetch.map(|x| x as usize);
                let node = SystemTableExec::try_new(table, projection, filters, fetch)?;
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
                let proto = self.try_decode_message(&base_config)?;
                let table_schema = parse_protobuf_file_scan_schema(&proto)?;
                let source = parse_protobuf_file_scan_config(
                    &proto,
                    &PhysicalPlanDecodeContext::new(ctx, self),
                    &DefaultPhysicalProtoConverter {},
                    Arc::new(JsonSource::new(table_schema)),
                )?;
                let source = FileScanConfigBuilder::from(source)
                    .with_file_compression_type(file_compression_type)
                    .build();
                Ok(Arc::new(DataSourceExec::new(Arc::new(source))))
            }
            NodeKind::Arrow(gen::ArrowExecNode { base_config }) => {
                let proto = self.try_decode_message(&base_config)?;
                let table_schema = parse_protobuf_file_scan_schema(&proto)?;
                let source = parse_protobuf_file_scan_config(
                    &proto,
                    &PhysicalPlanDecodeContext::new(ctx, self),
                    &DefaultPhysicalProtoConverter {},
                    Arc::new(ArrowSource::new_file_source(table_schema)),
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
                let proto = self.try_decode_message(&base_config)?;
                let table_schema = parse_protobuf_file_scan_schema(&proto)?;
                let source = parse_protobuf_file_scan_config(
                    &proto,
                    &PhysicalPlanDecodeContext::new(ctx, self),
                    &DefaultPhysicalProtoConverter {},
                    Arc::new(TextSource::new(table_schema, whole_text, line_sep)),
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
                let proto = self.try_decode_message(&base_config)?;
                let table_schema = parse_protobuf_file_scan_schema(&proto)?;
                let source = parse_protobuf_file_scan_config(
                    &proto,
                    &PhysicalPlanDecodeContext::new(ctx, self),
                    &DefaultPhysicalProtoConverter {},
                    Arc::new(BinarySource::new(table_schema, path_glob_filter)),
                )?;
                let source = FileScanConfigBuilder::from(source).build();
                Ok(Arc::new(DataSourceExec::new(Arc::new(source))))
            }
            NodeKind::Avro(gen::AvroExecNode { base_config }) => {
                let proto = self.try_decode_message(&base_config)?;
                let table_schema = parse_protobuf_file_scan_schema(&proto)?;
                let source = parse_protobuf_file_scan_config(
                    &proto,
                    &PhysicalPlanDecodeContext::new(ctx, self),
                    &DefaultPhysicalProtoConverter {},
                    Arc::new(AvroSource::new(table_schema)),
                )?;
                Ok(Arc::new(DataSourceExec::new(Arc::new(source))))
            }
            NodeKind::WorkTable(gen::WorkTableExecNode { name, schema }) => {
                let schema = self.try_decode_schema(&schema)?;
                Ok(Arc::new(WorkTableExec::new(name, Arc::new(schema), None)?))
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
            NodeKind::DeltaWriter(delta_writer) => {
                let gen::DeltaWriterExecNode {
                    input,
                    table_url,
                    options,
                    sink_schema,
                    partition_columns,
                    table_exists,
                    sink_mode,
                    metadata_configuration,
                    write_context,
                    lakehouse_table_json,
                } = *delta_writer;
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
                let write_context = match write_context {
                    Some(write_context) => self.try_decode_delta_write_context(write_context)?,
                    None => return plan_err!("Missing write_context for DeltaWriterExec"),
                };
                let lakehouse_table = self.try_decode_lakehouse_table(&lakehouse_table_json)?;

                Ok(Arc::new(DeltaWriterExec::new(
                    input,
                    table_url,
                    options,
                    metadata_configuration,
                    partition_columns,
                    sink_mode,
                    table_exists,
                    Arc::new(sink_schema),
                    write_context,
                    lakehouse_table,
                )?))
            }
            NodeKind::DeltaCommit(gen::DeltaCommitExecNode {
                input,
                table_url,
                partition_columns,
                table_exists,
                sink_schema,
                sink_mode,
                user_metadata,
                commit_context,
                lakehouse_table_json,
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
                let commit_context = match commit_context {
                    Some(commit_context) => self.try_decode_delta_commit_context(commit_context)?,
                    None => return plan_err!("Missing commit_context for DeltaCommitExec"),
                };

                let lakehouse_table = self.try_decode_lakehouse_table(&lakehouse_table_json)?;
                Ok(Arc::new(DeltaCommitExec::new(
                    input,
                    table_url,
                    partition_columns,
                    table_exists,
                    Arc::new(sink_schema),
                    sink_mode,
                    user_metadata,
                    commit_context,
                    lakehouse_table,
                )))
            }
            NodeKind::DeltaScanByAdds(gen::DeltaScanByAddsExecNode {
                input,
                table_url,
                table_schema,
                output_schema,
                scan_config_json,
                projection,
                limit,
                pushdown_filter,
                version,
                statistics,
                lakehouse_table_json,
            }) => {
                let input = self.try_decode_plan(&input, ctx)?;
                let table_url = Url::parse(&table_url)
                    .map_err(|e| plan_datafusion_err!("failed to parse table URL: {e}"))?;
                let table_schema = Arc::new(self.try_decode_schema(&table_schema)?);
                let output_schema = if let Some(schema_bytes) = output_schema {
                    Arc::new(self.try_decode_schema(&schema_bytes)?)
                } else {
                    Arc::clone(&table_schema)
                };
                let scan_config: sail_delta_lake::datasource::DeltaScanConfig =
                    if scan_config_json.is_empty() {
                        sail_delta_lake::datasource::DeltaScanConfig::default()
                    } else {
                        serde_json::from_str(&scan_config_json).map_err(|e| {
                            plan_datafusion_err!("failed to decode Delta scan config: {e}")
                        })?
                    };
                let projection = projection
                    .map(|p| self.try_decode_projection(&p.columns))
                    .transpose()
                    .map_err(|_| {
                        plan_datafusion_err!("invalid projection for DeltaScanByAddsExec")
                    })?;
                let limit = limit
                    .map(usize::try_from)
                    .transpose()
                    .map_err(|_| plan_datafusion_err!("invalid limit for DeltaScanByAddsExec"))?;
                let pushdown_filter = if let Some(pred_bytes) = pushdown_filter {
                    let predicate = parse_physical_expr(
                        &self.try_decode_message(&pred_bytes)?,
                        ctx,
                        &output_schema,
                        self,
                    )?;
                    Some(predicate)
                } else {
                    None
                };
                let statistics = statistics
                    .as_ref()
                    .map(|bytes| self.try_decode_statistics(bytes))
                    .transpose()?;
                let lakehouse_table = self.try_decode_lakehouse_table(&lakehouse_table_json)?;
                Ok(Arc::new(
                    DeltaScanByAddsExec::new(
                        input,
                        table_url,
                        version,
                        table_schema,
                        output_schema,
                        scan_config,
                        projection,
                        limit,
                        pushdown_filter,
                        lakehouse_table,
                    )
                    .with_output_statistics(statistics),
                ))
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
            NodeKind::DeltaMetadataStats(gen::DeltaMetadataStatsExecNode {
                input,
                stats_schema,
            }) => {
                let input = self.try_decode_plan(&input, ctx)?;
                let stats_schema = Arc::new(self.try_decode_schema(&stats_schema)?);
                Ok(Arc::new(DeltaMetadataStatsExec::new(input, stats_schema)))
            }
            NodeKind::DeltaRemoveActions(gen::DeltaRemoveActionsExecNode {
                input,
                partition_value_columns_json,
            }) => {
                let input = self.try_decode_plan(&input, ctx)?;
                let partition_value_columns = partition_value_columns_json
                    .as_deref()
                    .map(serde_json::from_str::<Vec<(String, String)>>)
                    .transpose()
                    .map_err(|e| plan_datafusion_err!("{e}"))?;
                Ok(Arc::new(DeltaRemoveActionsExec::try_new(
                    input,
                    partition_value_columns,
                )?))
            }
            NodeKind::DeltaLogReplay(gen::DeltaLogReplayExecNode {
                input,
                table_url,
                version,
                partition_columns,
                checkpoint_files,
                commit_files,
                checkpoint_input,
                commits_input,
            }) => {
                let table_url = Url::parse(&table_url)
                    .map_err(|e| plan_datafusion_err!("failed to parse table URL: {e}"))?;
                match (checkpoint_input.as_ref(), commits_input.as_ref()) {
                    (Some(checkpoint_input), Some(commits_input)) => {
                        let checkpoint_input = self.try_decode_plan(checkpoint_input, ctx)?;
                        let commits_input = self.try_decode_plan(commits_input, ctx)?;
                        Ok(Arc::new(DeltaLogReplayExec::new_hash(
                            checkpoint_input,
                            commits_input,
                            table_url,
                            version,
                            partition_columns,
                            checkpoint_files,
                            commit_files,
                        )))
                    }
                    (None, None) => {
                        let input = self.try_decode_plan(&input, ctx)?;
                        Ok(Arc::new(DeltaLogReplayExec::new(
                            input,
                            table_url,
                            version,
                            partition_columns,
                            checkpoint_files,
                            commit_files,
                        )))
                    }
                    _ => plan_err!(
                        "DeltaLogReplayExec requires both checkpoint_input and commits_input when hash replay is encoded"
                    ),
                }
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
                let options = SocketReadOptions {
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
                let options = RateReadOptions {
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
                        parse_physical_sort_exprs(
                            physical_sort_expr_nodes,
                            &PhysicalPlanDecodeContext::new(ctx, self),
                            &schema,
                            &DefaultPhysicalProtoConverter {},
                        )
                        .map(|sort_exprs| {
                            LexRequirement::new(sort_exprs.into_iter().map(Into::into))
                        })
                    })
                    .transpose()?
                    .flatten();
                Ok(Arc::new(DataSinkExec::new(
                    input,
                    Arc::new(data_sink),
                    sort_order,
                )))
            }
            NodeKind::FileDelete(gen::FileDeleteExecNode {
                object_store_url,
                path,
            }) => {
                let object_store_url =
                    datafusion::execution::object_store::ObjectStoreUrl::parse(object_store_url)?;
                let path = object_store::path::Path::parse(path)
                    .map_err(|e| plan_datafusion_err!("invalid file delete path: {e}"))?;
                Ok(Arc::new(FileDeleteExec::new(object_store_url, path)))
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
            NodeKind::StreamFilter(gen::StreamFilterExecNode { input, predicate }) => {
                let input = self.try_decode_plan(&input, ctx)?;
                let predicate = parse_physical_expr(
                    &self.try_decode_message(&predicate)?,
                    ctx,
                    &input.schema(),
                    self,
                )?;
                Ok(Arc::new(StreamFilterExec::try_new(input, predicate)?))
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
            NodeKind::MonotonicId(gen::MonotonicIdExecNode {
                input,
                column_name,
                schema,
            }) => {
                let schema = self.try_decode_schema(&schema)?;
                Ok(Arc::new(MonotonicIdExec::try_new(
                    self.try_decode_plan(&input, ctx)?,
                    column_name,
                    Arc::new(schema),
                )?))
            }
            NodeKind::SparkPartitionId(gen::SparkPartitionIdExecNode {
                input,
                column_name,
                schema,
            }) => {
                let schema = self.try_decode_schema(&schema)?;
                Ok(Arc::new(SparkPartitionIdExec::try_new(
                    self.try_decode_plan(&input, ctx)?,
                    column_name,
                    Arc::new(schema),
                )?))
            }
            NodeKind::Coalesce(gen::CoalesceExecNode {
                input,
                output_partitions,
            }) => Ok(Arc::new(CoalesceExec::new(
                self.try_decode_plan(&input, ctx)?,
                usize::try_from(output_partitions).map_err(|e| plan_datafusion_err!("{e}"))?,
            ))),
            NodeKind::RelaxedTzCast(gen::RelaxedTzCastExecNode { input, schema }) => {
                let input = self.try_decode_plan(&input, ctx)?;
                let schema = Arc::new(self.try_decode_schema(&schema)?);
                Ok(Arc::new(RelaxedTzCastExec::new(input, schema)))
            }
            NodeKind::DeletionVectorWriter(gen::DeletionVectorWriterExecNode {
                input,
                table_url,
                condition,
                table_schema,
                version,
                operation_json,
                partition_value_columns_json,
            }) => {
                let input = self.try_decode_plan(&input, ctx)?;
                let table_url = Url::parse(&table_url)
                    .map_err(|e| plan_datafusion_err!("failed to parse table URL: {e}"))?;
                let table_schema = Arc::new(self.try_decode_schema(&table_schema)?);
                let condition = parse_physical_expr(
                    &self.try_decode_message(&condition)?,
                    ctx,
                    &table_schema,
                    self,
                )?;
                let operation = if let Some(s) = operation_json.as_ref() {
                    Some(
                        serde_json::from_str::<DeltaOperation>(s)
                            .map_err(|e| plan_datafusion_err!("{e}"))?,
                    )
                } else {
                    None
                };
                let partition_value_columns = partition_value_columns_json
                    .as_deref()
                    .map(serde_json::from_str::<Vec<(String, String)>>)
                    .transpose()
                    .map_err(|e| plan_datafusion_err!("{e}"))?;
                Ok(Arc::new(DeletionVectorWriterExec::new(
                    input,
                    table_url,
                    condition,
                    table_schema,
                    version,
                    partition_value_columns,
                    operation,
                )?))
            }
            NodeKind::DeletionVectorRowsWriter(gen::DeletionVectorRowsWriterExecNode {
                input,
                adds_input,
                table_url,
                path_column,
                row_index_column,
                version,
                operation_json,
                partition_value_columns_json,
            }) => {
                let input = self.try_decode_plan(&input, ctx)?;
                let adds_input = self.try_decode_plan(&adds_input, ctx)?;
                let table_url = Url::parse(&table_url)
                    .map_err(|e| plan_datafusion_err!("failed to parse table URL: {e}"))?;
                let operation = if let Some(s) = operation_json.as_ref() {
                    Some(
                        serde_json::from_str::<DeltaOperation>(s)
                            .map_err(|e| plan_datafusion_err!("{e}"))?,
                    )
                } else {
                    None
                };
                let partition_value_columns = partition_value_columns_json
                    .as_deref()
                    .map(serde_json::from_str::<Vec<(String, String)>>)
                    .transpose()
                    .map_err(|e| plan_datafusion_err!("{e}"))?;
                Ok(Arc::new(DeletionVectorRowsWriterExec::new(
                    input,
                    adds_input,
                    table_url,
                    path_column,
                    row_index_column,
                    version,
                    partition_value_columns,
                    operation,
                )?))
            }
            NodeKind::IcebergWriter(gen::IcebergWriterExecNode {
                input,
                table_url,
                partition_columns,
                sink_mode,
                table_exists,
                options,
                logical_input_schema,
                lakehouse_table_json,
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
                let partition_columns = partition_columns
                    .into_iter()
                    .map(|field| self.try_decode_catalog_partition_field(field))
                    .collect::<Result<Vec<_>>>()?;
                let mut options = if options.is_empty() {
                    IcebergWriterExecOptions::default()
                } else {
                    serde_json::from_str(&options).map_err(|e| {
                        plan_datafusion_err!("failed to decode Iceberg options: {e}")
                    })?
                };
                if let Some(lakehouse_table) =
                    self.try_decode_lakehouse_table(&lakehouse_table_json)?
                {
                    options.lakehouse_table = Some(lakehouse_table);
                }
                let logical_input_schema = if logical_input_schema.is_empty() {
                    None
                } else {
                    Some(Arc::new(self.try_decode_schema(&logical_input_schema)?))
                };

                Ok(Arc::new(IcebergWriterExec::new(
                    input,
                    table_url,
                    partition_columns,
                    sink_mode,
                    table_exists,
                    options,
                    logical_input_schema,
                )))
            }
            NodeKind::IcebergCommit(gen::IcebergCommitExecNode {
                input,
                table_url,
                lakehouse_table_json,
            }) => {
                let input = self.try_decode_plan(&input, ctx)?;
                let table_url = Url::parse(&table_url)
                    .map_err(|e| plan_datafusion_err!("failed to parse table URL: {e}"))?;
                let lakehouse_table = self.try_decode_lakehouse_table(&lakehouse_table_json)?;

                Ok(Arc::new(IcebergCommitExec::new(
                    input,
                    table_url,
                    lakehouse_table,
                )))
            }
            NodeKind::IcebergManifestScan(gen::IcebergManifestScanExecNode {
                table_url,
                snapshot_json,
            }) => {
                let snapshot: sail_iceberg::spec::Snapshot = serde_json::from_str(&snapshot_json)
                    .map_err(|e| {
                    plan_datafusion_err!("failed to decode Iceberg snapshot: {e}")
                })?;
                Ok(Arc::new(IcebergManifestScanExec::new(table_url, snapshot)))
            }
            NodeKind::IcebergDiscovery(gen::IcebergDiscoveryExecNode {
                input,
                table_url,
                snapshot_id,
                input_partition_scan,
            }) => {
                let input = self.try_decode_plan(&input, ctx)?;
                Ok(Arc::new(IcebergDiscoveryExec::new(
                    input,
                    table_url,
                    snapshot_id,
                    input_partition_scan,
                )?))
            }
            NodeKind::IcebergScanByDataFiles(gen::IcebergScanByDataFilesExecNode {
                input,
                table_url,
                output_schema,
            }) => {
                let input = self.try_decode_plan(&input, ctx)?;
                let output_schema = Arc::new(self.try_decode_schema(&output_schema)?);
                Ok(Arc::new(IcebergScanByDataFilesExec::new(
                    input,
                    table_url,
                    output_schema,
                )))
            }
            NodeKind::IcebergDeleteApply(gen::IcebergDeleteApplyExecNode {
                input,
                data_file_path,
                positional_deletes_json,
                equality_deletes_json,
                table_url,
                iceberg_schema_json,
            }) => {
                let input = self.try_decode_plan(&input, ctx)?;
                let positional_deletes: Vec<sail_iceberg::spec::delete_index::DeleteFileRef> =
                    serde_json::from_str(&positional_deletes_json).map_err(|e| {
                        plan_datafusion_err!("failed to decode positional delete refs: {e}")
                    })?;
                let equality_deletes: Vec<sail_iceberg::spec::delete_index::DeleteFileRef> =
                    serde_json::from_str(&equality_deletes_json).map_err(|e| {
                        plan_datafusion_err!("failed to decode equality delete refs: {e}")
                    })?;
                let iceberg_schema: sail_iceberg::spec::Schema =
                    serde_json::from_str(&iceberg_schema_json).map_err(|e| {
                        plan_datafusion_err!("failed to decode Iceberg schema: {e}")
                    })?;
                Ok(Arc::new(IcebergDeleteApplyExec::new(
                    input,
                    data_file_path,
                    positional_deletes,
                    equality_deletes,
                    table_url,
                    iceberg_schema,
                )))
            }
            NodeKind::PythonDataSource(gen::PythonDataSourceExecNode {
                pickled_reader,
                schema,
                partitions,
            }) => {
                let schema = Arc::new(self.try_decode_schema(&schema)?);
                let partitions = partitions
                    .into_iter()
                    .map(|p| InputPartition {
                        partition_id: p.partition_id as usize,
                        data: p.data,
                    })
                    .collect();
                // Note: executor is created lazily in execute() on the worker
                Ok(Arc::new(PythonDataSourceExec::new(
                    pickled_reader,
                    schema,
                    partitions,
                )))
            }
            NodeKind::PythonDataSourceWrite(gen::PythonDataSourceWriteExecNode {
                pickled_writer,
                schema,
                is_arrow,
                input,
            }) => {
                let schema = Arc::new(self.try_decode_schema(&schema)?);
                let input = self.try_decode_plan(&input, ctx)?;
                if schema.as_ref() != input.schema().as_ref() {
                    return plan_err!(
                        "PythonDataSourceWriteExec schema mismatch: encoded schema does not match input schema"
                    );
                }
                Ok(Arc::new(PythonDataSourceWriteExec::new(
                    input,
                    pickled_writer,
                    is_arrow,
                )))
            }
            NodeKind::PythonDataSourceWriteCommit(gen::PythonDataSourceWriteCommitExecNode {
                pickled_writer,
                expected_partitions,
                input,
            }) => {
                let input = self.try_decode_plan(&input, ctx)?;
                Ok(Arc::new(PythonDataSourceWriteCommitExec::new(
                    input,
                    pickled_writer,
                    expected_partitions as usize,
                )))
            }
            NodeKind::CatalogCommand(gen::CatalogCommandExecNode { schema, command }) => {
                let schema = Arc::new(self.try_decode_schema(&schema)?);
                let command: sail_catalog::command::CatalogCommand = serde_json::from_str(&command)
                    .map_err(|e| plan_datafusion_err!("failed to decode CatalogCommand: {e}"))?;
                Ok(Arc::new(CatalogCommandExec::new(command, schema)))
            }
            NodeKind::Barrier(gen::BarrierExecNode {
                preconditions,
                plan,
            }) => {
                let preconditions = preconditions
                    .into_iter()
                    .map(|i| self.try_decode_plan(&i, ctx))
                    .collect::<Result<_>>()?;
                let plan = self.try_decode_plan(&plan, ctx)?;
                Ok(Arc::new(BarrierExec::new(preconditions, plan)))
            }
            _ => plan_err!("unsupported physical plan node: {node_kind:?}"),
        }
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
        let node_kind = if let Some(range) = node.downcast_ref::<RangeExec>() {
            let schema = self.try_encode_schema(range.original_schema().as_ref())?;
            let projection = self.try_encode_projection(range.projection())?;
            NodeKind::Range(gen::RangeExecNode {
                start: range.range().start,
                end: range.range().end,
                step: range.range().step,
                num_partitions: range.num_partitions() as u64,
                schema,
                projection,
            })
        } else if let Some(show_string) = node.downcast_ref::<ShowStringExec>() {
            let schema = self.try_encode_schema(show_string.schema().as_ref())?;
            NodeKind::ShowString(gen::ShowStringExecNode {
                input: self.try_encode_plan(show_string.input().clone())?,
                names: show_string.names().to_vec(),
                limit: show_string.limit() as u64,
                style: self.try_encode_show_string_style(show_string.format().style())?,
                truncate: show_string.format().truncate() as u64,
                schema,
            })
        } else if let Some(stage_input) = node.downcast_ref::<StageInputExec<usize>>() {
            let eq_properties = self.try_encode_equivalence_properties(
                stage_input.properties().equivalence_properties(),
            )?;
            let partitioning =
                self.try_encode_partitioning(stage_input.properties().output_partitioning())?;
            let bounded = match stage_input.properties().boundedness {
                Boundedness::Bounded => true,
                Boundedness::Unbounded {
                    requires_infinite_memory: _,
                } => false,
            };
            NodeKind::StageInput(gen::StageInputExecNode {
                input: *stage_input.input() as u64,
                eq_properties: Some(eq_properties),
                partitioning,
                bounded,
            })
        } else if let Some(system_table) = node.downcast_ref::<SystemTableExec>() {
            let table = serde_json::to_string(&system_table.table())
                .map_err(|e| plan_datafusion_err!("{e}"))?;
            let projection = system_table.projection().map(|x| gen::PhysicalProjection {
                columns: x.iter().map(|c| *c as u64).collect(),
            });
            let filters = system_table
                .filters()
                .iter()
                .map(|expr| {
                    let expr = serialize_physical_expr(expr, self)?;
                    self.try_encode_message(expr)
                })
                .collect::<Result<_>>()?;
            let fetch = system_table.fetch().map(|f| f as u64);
            NodeKind::SystemTable(gen::SystemTableExecNode {
                table,
                projection,
                filters,
                fetch,
            })
        } else if let Some(schema_pivot) = node.downcast_ref::<SchemaPivotExec>() {
            let schema = self.try_encode_schema(schema_pivot.schema().as_ref())?;
            NodeKind::SchemaPivot(gen::SchemaPivotExecNode {
                input: self.try_encode_plan(schema_pivot.input().clone())?,
                names: schema_pivot.names().to_vec(),
                schema,
            })
        } else if let Some(map_partitions) = node.downcast_ref::<MapPartitionsExec>() {
            let udf = self.try_encode_stream_udf(map_partitions.udf().as_ref())?;
            let schema = self.try_encode_schema(map_partitions.schema().as_ref())?;
            NodeKind::MapPartitions(gen::MapPartitionsExecNode {
                input: self.try_encode_plan(map_partitions.input().clone())?,
                udf: Some(udf),
                schema,
            })
        } else if let Some(work_table) = node.downcast_ref::<WorkTableExec>() {
            let name = work_table.name().to_string();
            let schema = self.try_encode_schema(work_table.schema().as_ref())?;
            NodeKind::WorkTable(gen::WorkTableExecNode { name, schema })
        } else if let Some(recursive_query) = node.downcast_ref::<RecursiveQueryExec>() {
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
        } else if let Some(sort_merge_join) = node.downcast_ref::<SortMergeJoinExec>() {
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
        } else if let Some(partial_sort) = node.downcast_ref::<PartialSortExec>() {
            let expr = Some(self.try_encode_lex_ordering(partial_sort.expr())?);
            let input = self.try_encode_plan(partial_sort.input().clone())?;
            let common_prefix_length = partial_sort.common_prefix_length() as u64;
            NodeKind::PartialSort(gen::PartialSortExecNode {
                expr,
                input,
                common_prefix_length,
            })
        } else if let Some(data_source) = node.downcast_ref::<DataSourceExec>() {
            let source = data_source.data_source();
            if let Some(file_scan) = source.downcast_ref::<FileScanConfig>() {
                let file_source = file_scan.file_source();
                if let Some(text_source) = file_source.downcast_ref::<TextSource>() {
                    let base_config = self.try_encode_message(serialize_file_scan_config(
                        file_scan,
                        self,
                        &DefaultPhysicalProtoConverter {},
                    )?)?;
                    let file_compression_type =
                        self.try_encode_file_compression_type(file_scan.file_compression_type)?;
                    NodeKind::Text(gen::TextExecNode {
                        base_config,
                        file_compression_type,
                        whole_text: text_source.whole_text(),
                        line_sep: text_source.line_sep().map(|x| vec![x]),
                    })
                } else if let Some(binary_source) = file_source.downcast_ref::<BinarySource>() {
                    let base_config = self.try_encode_message(serialize_file_scan_config(
                        file_scan,
                        self,
                        &DefaultPhysicalProtoConverter {},
                    )?)?;
                    NodeKind::BinarySource(gen::BinarySourceExecNode {
                        base_config,
                        path_glob_filter: binary_source.path_glob_filter().cloned(),
                    })
                } else if file_source.is::<JsonSource>() {
                    // TODO: Check if we still need to have JsonSource: https://github.com/apache/datafusion/pull/14224
                    let base_config = self.try_encode_message(serialize_file_scan_config(
                        file_scan,
                        self,
                        &DefaultPhysicalProtoConverter {},
                    )?)?;
                    let file_compression_type =
                        self.try_encode_file_compression_type(file_scan.file_compression_type)?;
                    NodeKind::NdJson(gen::NdJsonExecNode {
                        base_config,
                        file_compression_type,
                    })
                } else if file_source.is::<ArrowSource>() {
                    // TODO: Check if we still need to have ArrowSource: https://github.com/apache/datafusion/pull/14224
                    let base_config = self.try_encode_message(serialize_file_scan_config(
                        file_scan,
                        self,
                        &DefaultPhysicalProtoConverter {},
                    )?)?;
                    NodeKind::Arrow(gen::ArrowExecNode { base_config })
                } else if file_source.is::<AvroSource>() {
                    let base_config = self.try_encode_message(serialize_file_scan_config(
                        file_scan,
                        self,
                        &DefaultPhysicalProtoConverter {},
                    )?)?;
                    NodeKind::Avro(gen::AvroExecNode { base_config })
                } else {
                    return plan_err!("unsupported data source node: {data_source:?}");
                }
            } else if let Some(memory) = source.downcast_ref::<MemorySourceConfig>() {
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
        } else if let Some(delta_writer_exec) = node.downcast_ref::<DeltaWriterExec>() {
            let input = self.try_encode_plan(delta_writer_exec.input().clone())?;
            let sink_mode = self.try_encode_physical_sink_mode(delta_writer_exec.sink_mode())?;
            NodeKind::DeltaWriter(Box::new(gen::DeltaWriterExecNode {
                input,
                table_url: delta_writer_exec.table_url().to_string(),
                options: serde_json::to_string(delta_writer_exec.options())
                    .map_err(|e| plan_datafusion_err!("{e}"))?,
                sink_schema: self.try_encode_schema(delta_writer_exec.sink_schema())?,
                partition_columns: delta_writer_exec.partition_columns().to_vec(),
                table_exists: delta_writer_exec.table_exists(),
                sink_mode: Some(sink_mode),
                metadata_configuration: delta_writer_exec.metadata_configuration().clone(),
                write_context: Some(
                    self.try_encode_delta_write_context(delta_writer_exec.write_context())?,
                ),
                lakehouse_table_json: self
                    .try_encode_lakehouse_table(delta_writer_exec.lakehouse_table())?,
            }))
        } else if let Some(delta_commit_exec) = node.downcast_ref::<DeltaCommitExec>() {
            let input = self.try_encode_plan(delta_commit_exec.input().clone())?;
            let sink_mode = self.try_encode_physical_sink_mode(delta_commit_exec.sink_mode())?;
            NodeKind::DeltaCommit(gen::DeltaCommitExecNode {
                input,
                table_url: delta_commit_exec.table_url().to_string(),
                partition_columns: delta_commit_exec.partition_columns().to_vec(),
                table_exists: delta_commit_exec.table_exists(),
                sink_schema: self.try_encode_schema(delta_commit_exec.sink_schema())?,
                sink_mode: Some(sink_mode),
                user_metadata: delta_commit_exec.user_metadata().map(str::to_owned),
                commit_context: Some(
                    self.try_encode_delta_commit_context(delta_commit_exec.commit_context())?,
                ),
                lakehouse_table_json: self
                    .try_encode_lakehouse_table(delta_commit_exec.lakehouse_table())?,
            })
        } else if let Some(delta_scan_by_adds_exec) = node.downcast_ref::<DeltaScanByAddsExec>() {
            let input = self.try_encode_plan(delta_scan_by_adds_exec.input().clone())?;
            let table_schema = self.try_encode_schema(delta_scan_by_adds_exec.table_schema())?;
            let output_schema = self.try_encode_schema(delta_scan_by_adds_exec.output_schema())?;
            let scan_config_json = serde_json::to_string(delta_scan_by_adds_exec.scan_config())
                .map_err(|e| plan_datafusion_err!("failed to encode Delta scan config: {e}"))?;
            let projection = delta_scan_by_adds_exec
                .projection()
                .map(|p| {
                    self.try_encode_projection(p)
                        .map(|columns| gen::PhysicalProjection { columns })
                })
                .transpose()
                .map_err(|_| plan_datafusion_err!("invalid projection for DeltaScanByAddsExec"))?;
            let limit = delta_scan_by_adds_exec
                .limit()
                .map(u64::try_from)
                .transpose()
                .map_err(|_| plan_datafusion_err!("invalid limit for DeltaScanByAddsExec"))?;
            let pushdown_filter = if let Some(pred) = delta_scan_by_adds_exec.pushdown_filter() {
                let predicate_node = serialize_physical_expr(pred, self)?;
                Some(self.try_encode_message(predicate_node)?)
            } else {
                None
            };
            let statistics =
                Some(self.try_encode_statistics(delta_scan_by_adds_exec.statistics())?);
            NodeKind::DeltaScanByAdds(gen::DeltaScanByAddsExecNode {
                input,
                table_url: delta_scan_by_adds_exec.table_url().to_string(),
                table_schema,
                output_schema: Some(output_schema),
                scan_config_json,
                projection,
                limit,
                pushdown_filter,
                version: delta_scan_by_adds_exec.version(),
                statistics,
                lakehouse_table_json: self
                    .try_encode_lakehouse_table(delta_scan_by_adds_exec.lakehouse_table())?,
            })
        } else if let Some(delta_discovery_exec) = node.downcast_ref::<DeltaDiscoveryExec>() {
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
        } else if let Some(delta_metadata_stats_exec) =
            node.downcast_ref::<DeltaMetadataStatsExec>()
        {
            NodeKind::DeltaMetadataStats(gen::DeltaMetadataStatsExecNode {
                input: self.try_encode_plan(delta_metadata_stats_exec.input().clone())?,
                stats_schema: self.try_encode_schema(delta_metadata_stats_exec.stats_schema())?,
            })
        } else if let Some(delta_remove_actions_exec) =
            node.downcast_ref::<DeltaRemoveActionsExec>()
        {
            let input = self.try_encode_plan(delta_remove_actions_exec.children()[0].clone())?;
            let partition_value_columns_json = delta_remove_actions_exec
                .partition_value_columns()
                .map(serde_json::to_string)
                .transpose()
                .map_err(|e| plan_datafusion_err!("{e}"))?;
            NodeKind::DeltaRemoveActions(gen::DeltaRemoveActionsExecNode {
                input,
                partition_value_columns_json,
            })
        } else if let Some(delta_log_replay_exec) = node.downcast_ref::<DeltaLogReplayExec>() {
            let children = delta_log_replay_exec.children();
            let (input, checkpoint_input, commits_input) = match children.as_slice() {
                [input] => (self.try_encode_plan((*input).clone())?, None, None),
                [checkpoint_input, commits_input] => (
                    Vec::new(),
                    Some(self.try_encode_plan((*checkpoint_input).clone())?),
                    Some(self.try_encode_plan((*commits_input).clone())?),
                ),
                _ => {
                    return plan_err!(
                        "DeltaLogReplayExec expects one child for sort replay or two children for hash replay"
                    )
                }
            };
            NodeKind::DeltaLogReplay(gen::DeltaLogReplayExecNode {
                input,
                table_url: delta_log_replay_exec.table_url().to_string(),
                version: delta_log_replay_exec.version(),
                partition_columns: delta_log_replay_exec.partition_columns().to_vec(),
                checkpoint_files: delta_log_replay_exec.checkpoint_files().to_vec(),
                commit_files: delta_log_replay_exec.commit_files().to_vec(),
                checkpoint_input,
                commits_input,
            })
        } else if let Some(console_sink) = node.downcast_ref::<ConsoleSinkExec>() {
            let input = self.try_encode_plan(console_sink.input().clone())?;
            NodeKind::ConsoleSink(gen::ConsoleSinkExecNode { input })
        } else if let Some(socket_source) = node.downcast_ref::<SocketSourceExec>() {
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
        } else if let Some(rate_source) = node.downcast_ref::<RateSourceExec>() {
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
        } else if let Some(data_sink) = node.downcast_ref::<DataSinkExec>() {
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
            if let Some(sink) = data_sink.sink().downcast_ref::<TextSink>() {
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
        } else if let Some(stream_collector) = node.downcast_ref::<StreamCollectorExec>() {
            let input = self.try_encode_plan(stream_collector.input().clone())?;
            NodeKind::StreamCollector(gen::StreamCollectorExecNode { input })
        } else if let Some(stream_limit) = node.downcast_ref::<StreamLimitExec>() {
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
        } else if let Some(stream_filter) = node.downcast_ref::<StreamFilterExec>() {
            let input = self.try_encode_plan(stream_filter.input().clone())?;
            let predicate =
                self.try_encode_message(serialize_physical_expr(stream_filter.predicate(), self)?)?;
            NodeKind::StreamFilter(gen::StreamFilterExecNode { input, predicate })
        } else if let Some(stream_source_adapter) = node.downcast_ref::<StreamSourceAdapterExec>() {
            let input = self.try_encode_plan(stream_source_adapter.input().clone())?;
            NodeKind::StreamSourceAdapter(gen::StreamSourceAdapterExecNode { input })
        } else if let Some(cardinality_check) = node.downcast_ref::<MergeCardinalityCheckExec>() {
            let input = self.try_encode_plan(cardinality_check.input().clone())?;
            NodeKind::MergeCardinalityCheck(gen::MergeCardinalityCheckExecNode {
                input,
                target_row_id_col: cardinality_check.target_row_id_col().to_string(),
                target_present_col: cardinality_check.target_present_col().to_string(),
                source_present_col: cardinality_check.source_present_col().to_string(),
            })
        } else if let Some(monotonic_id) = node.downcast_ref::<MonotonicIdExec>() {
            let input = self.try_encode_plan(monotonic_id.input().clone())?;
            let schema = self.try_encode_schema(monotonic_id.schema().as_ref())?;
            NodeKind::MonotonicId(gen::MonotonicIdExecNode {
                input,
                column_name: monotonic_id.column_name().to_string(),
                schema,
            })
        } else if let Some(spark_partition_id) = node.downcast_ref::<SparkPartitionIdExec>() {
            let input = self.try_encode_plan(spark_partition_id.input().clone())?;
            let schema = self.try_encode_schema(spark_partition_id.schema().as_ref())?;
            NodeKind::SparkPartitionId(gen::SparkPartitionIdExecNode {
                input,
                column_name: spark_partition_id.column_name().to_string(),
                schema,
            })
        } else if let Some(coalesce) = node.downcast_ref::<CoalesceExec>() {
            let input = self.try_encode_plan(coalesce.input().clone())?;
            NodeKind::Coalesce(gen::CoalesceExecNode {
                input,
                output_partitions: u64::try_from(coalesce.output_partitions())
                    .map_err(|e| plan_datafusion_err!("{e}"))?,
            })
        } else if let Some(relaxed_tz_cast) = node.downcast_ref::<RelaxedTzCastExec>() {
            let input = self.try_encode_plan(relaxed_tz_cast.input().clone())?;
            let schema = self.try_encode_schema(relaxed_tz_cast.schema().as_ref())?;
            NodeKind::RelaxedTzCast(gen::RelaxedTzCastExecNode { input, schema })
        } else if let Some(dv_writer_exec) = node.downcast_ref::<DeletionVectorWriterExec>() {
            let input = self.try_encode_plan(dv_writer_exec.input().clone())?;
            let condition_node = serialize_physical_expr(dv_writer_exec.condition(), self)?;
            let condition = self.try_encode_message(condition_node)?;
            let table_schema = self.try_encode_schema(dv_writer_exec.table_schema())?;
            let operation_json = if let Some(op) = dv_writer_exec.operation() {
                Some(serde_json::to_string(op).map_err(|e| plan_datafusion_err!("{e}"))?)
            } else {
                None
            };
            NodeKind::DeletionVectorWriter(gen::DeletionVectorWriterExecNode {
                input,
                table_url: dv_writer_exec.table_url().to_string(),
                condition,
                table_schema,
                version: dv_writer_exec.version(),
                operation_json,
                partition_value_columns_json: dv_writer_exec
                    .partition_value_columns()
                    .map(serde_json::to_string)
                    .transpose()
                    .map_err(|e| plan_datafusion_err!("{e}"))?,
            })
        } else if let Some(dv_rows_writer_exec) =
            node.downcast_ref::<DeletionVectorRowsWriterExec>()
        {
            let input = self.try_encode_plan(dv_rows_writer_exec.input().clone())?;
            let adds_input = self.try_encode_plan(dv_rows_writer_exec.adds_input().clone())?;
            let operation_json = if let Some(op) = dv_rows_writer_exec.operation() {
                Some(serde_json::to_string(op).map_err(|e| plan_datafusion_err!("{e}"))?)
            } else {
                None
            };
            NodeKind::DeletionVectorRowsWriter(gen::DeletionVectorRowsWriterExecNode {
                input,
                adds_input,
                table_url: dv_rows_writer_exec.table_url().to_string(),
                path_column: dv_rows_writer_exec.path_column().to_string(),
                row_index_column: dv_rows_writer_exec.row_index_column().to_string(),
                version: dv_rows_writer_exec.version(),
                operation_json,
                partition_value_columns_json: dv_rows_writer_exec
                    .partition_value_columns()
                    .map(serde_json::to_string)
                    .transpose()
                    .map_err(|e| plan_datafusion_err!("{e}"))?,
            })
        } else if let Some(iceberg_writer_exec) = node.downcast_ref::<IcebergWriterExec>() {
            let input = self.try_encode_plan(iceberg_writer_exec.input().clone())?;
            let sink_mode = self.try_encode_physical_sink_mode(iceberg_writer_exec.sink_mode())?;
            let options = serde_json::to_string(iceberg_writer_exec.options())
                .map_err(|e| plan_datafusion_err!("failed to encode Iceberg options: {e}"))?;
            let logical_input_schema = iceberg_writer_exec
                .logical_input_schema()
                .map(|schema| self.try_encode_schema(schema.as_ref()))
                .transpose()?
                .unwrap_or_default();
            NodeKind::IcebergWriter(gen::IcebergWriterExecNode {
                input,
                table_url: iceberg_writer_exec.table_url().to_string(),
                partition_columns: iceberg_writer_exec
                    .partition_columns()
                    .iter()
                    .map(Self::try_encode_catalog_partition_field)
                    .collect::<Result<Vec<_>>>()?,
                sink_mode: Some(sink_mode),
                table_exists: iceberg_writer_exec.table_exists(),
                options,
                logical_input_schema,
                lakehouse_table_json: self
                    .try_encode_lakehouse_table(iceberg_writer_exec.lakehouse_table())?,
            })
        } else if let Some(iceberg_commit_exec) = node.downcast_ref::<IcebergCommitExec>() {
            let input = self.try_encode_plan(iceberg_commit_exec.input().clone())?;
            NodeKind::IcebergCommit(gen::IcebergCommitExecNode {
                input,
                table_url: iceberg_commit_exec.table_url().to_string(),
                lakehouse_table_json: self
                    .try_encode_lakehouse_table(iceberg_commit_exec.lakehouse_table())?,
            })
        } else if let Some(manifest_scan) = node.downcast_ref::<IcebergManifestScanExec>() {
            let snapshot_json = serde_json::to_string(manifest_scan.snapshot())
                .map_err(|e| plan_datafusion_err!("failed to encode Iceberg snapshot: {e}"))?;
            NodeKind::IcebergManifestScan(gen::IcebergManifestScanExecNode {
                table_url: manifest_scan.table_url().to_string(),
                snapshot_json,
            })
        } else if let Some(discovery) = node.downcast_ref::<IcebergDiscoveryExec>() {
            let input = self.try_encode_plan(discovery.input().clone())?;
            NodeKind::IcebergDiscovery(gen::IcebergDiscoveryExecNode {
                input,
                table_url: discovery.table_url().to_string(),
                snapshot_id: discovery.snapshot_id(),
                input_partition_scan: discovery.input_partition_scan(),
            })
        } else if let Some(scan_by_files) = node.downcast_ref::<IcebergScanByDataFilesExec>() {
            let input = self.try_encode_plan(scan_by_files.input().clone())?;
            let output_schema = self.try_encode_schema(scan_by_files.output_schema().as_ref())?;
            NodeKind::IcebergScanByDataFiles(gen::IcebergScanByDataFilesExecNode {
                input,
                table_url: scan_by_files.table_url().to_string(),
                output_schema,
            })
        } else if let Some(delete_apply) = node.downcast_ref::<IcebergDeleteApplyExec>() {
            let input = self.try_encode_plan(delete_apply.input().clone())?;
            let positional_deletes_json = serde_json::to_string(delete_apply.positional_deletes())
                .map_err(|e| {
                    plan_datafusion_err!("failed to encode positional delete refs: {e}")
                })?;
            let equality_deletes_json = serde_json::to_string(delete_apply.equality_deletes())
                .map_err(|e| plan_datafusion_err!("failed to encode equality delete refs: {e}"))?;
            let iceberg_schema_json = serde_json::to_string(delete_apply.iceberg_schema())
                .map_err(|e| plan_datafusion_err!("failed to encode Iceberg schema: {e}"))?;
            NodeKind::IcebergDeleteApply(gen::IcebergDeleteApplyExecNode {
                input,
                data_file_path: delete_apply.data_file_path().to_string(),
                positional_deletes_json,
                equality_deletes_json,
                table_url: delete_apply.table_url().to_string(),
                iceberg_schema_json,
            })
        } else if let Some(python_exec) = node.downcast_ref::<PythonDataSourceExec>() {
            let schema = self.try_encode_schema(python_exec.schema().as_ref())?;
            let partitions = python_exec
                .partitions()
                .iter()
                .map(|p| gen::PythonDataSourceInputPartition {
                    partition_id: p.partition_id as u64,
                    data: p.data.clone(),
                })
                .collect();
            NodeKind::PythonDataSource(gen::PythonDataSourceExecNode {
                pickled_reader: python_exec.pickled_reader().to_vec(),
                schema,
                partitions,
            })
        } else if let Some(python_write_exec) = node.downcast_ref::<PythonDataSourceWriteExec>() {
            let schema = self.try_encode_schema(python_write_exec.input().schema().as_ref())?;
            let input = self.try_encode_plan(python_write_exec.input().clone())?;
            NodeKind::PythonDataSourceWrite(gen::PythonDataSourceWriteExecNode {
                pickled_writer: python_write_exec.pickled_writer().to_vec(),
                schema,
                is_arrow: python_write_exec.is_arrow(),
                input,
            })
        } else if let Some(python_commit_exec) =
            node.downcast_ref::<PythonDataSourceWriteCommitExec>()
        {
            let input = self.try_encode_plan(python_commit_exec.input().clone())?;
            NodeKind::PythonDataSourceWriteCommit(gen::PythonDataSourceWriteCommitExecNode {
                pickled_writer: python_commit_exec.pickled_writer().to_vec(),
                expected_partitions: python_commit_exec.expected_partitions() as u64,
                input,
            })
        } else if let Some(catalog_command_exec) = node.downcast_ref::<CatalogCommandExec>() {
            let schema = self.try_encode_schema(catalog_command_exec.schema().as_ref())?;
            let command = serde_json::to_string(catalog_command_exec.command())
                .map_err(|e| plan_datafusion_err!("failed to encode CatalogCommand: {e}"))?;
            NodeKind::CatalogCommand(gen::CatalogCommandExecNode { schema, command })
        } else if let Some(file_delete_exec) = node.downcast_ref::<FileDeleteExec>() {
            NodeKind::FileDelete(gen::FileDeleteExecNode {
                object_store_url: file_delete_exec.object_store_url().as_str().to_string(),
                path: file_delete_exec.path().to_string(),
            })
        } else if let Some(barrier_exec) = node.downcast_ref::<BarrierExec>() {
            let preconditions = barrier_exec
                .preconditions()
                .iter()
                .map(|child| self.try_encode_plan(child.clone()))
                .collect::<Result<_>>()?;
            let plan = self.try_encode_plan(barrier_exec.plan().clone())?;
            NodeKind::Barrier(gen::BarrierExecNode {
                preconditions,
                plan,
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
        // TODO: Implement custom registry to avoid codec for built-in functions.
        // The `match name` below has no session-registry fallback, so every
        // scalar UDF needs an explicit arm or distributed decode fails with
        // "could not find scalar function". DataFusion built-ins without an arm
        // (e.g. `array_length`, `cardinality` — what Spark `size` lowers to) thus
        // break ANY cluster query that uses them, including
        // `filter(arr, x -> size(filter(x, ...)) > 0)`. A registry fallback for
        // DF built-ins would fix this class at once (Spark* custom UDFs + HOFs
        // would still need their oneof/arm). This is the prerequisite for
        // distributing HOFs in aggregate/window nodes (see the TODO in
        // `WrapHigherOrderFunctions`).
        //
        // The fix needs NO proto change. datafusion-proto's from_proto.rs already
        // resolves a scalar UDF from the session registry FIRST when the encoded
        // extension buffer is empty: `ctx.udf(name).or_else(|_| codec.try_decode_udf(name, &[]))`.
        // Today `try_encode_udf` writes an `ExtendedScalarUdf` (UdfKind::Standard {})
        // for built-ins too, so the non-empty buffer forces this codec path instead.
        // Fix: in `try_encode_udf`, DON'T write a buffer for plain DataFusion
        // built-ins (leave it empty), so decode falls through to `ctx.udf(name)`
        // from the session registry (which already has array_length/cardinality/...).
        // Keep the explicit oneof/buffer only for Sail's Spark* custom UDFs and HOFs.
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
            UdfKind::XpathTyped(gen::XpathTypedUdf { name }) => {
                let kind = xpath_typed_name_to_kind(&name)?;
                let udf = XpathTyped::new(kind);
                return Ok(Arc::new(ScalarUDF::from(udf)));
            }
            UdfKind::SparkToXml(gen::SparkToXmlUdf { session_timezone }) => {
                let udf = SparkToXml::new(Arc::from(session_timezone));
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
            UdfKind::ArraysZip(gen::ArraysZipUdf { field_names }) => {
                let udf = ArraysZip::new(field_names);
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
            UdfKind::SparkTimestamp(gen::SparkTimestampUdf { timezone, is_try }) => {
                let udf = SparkTimestamp::try_new(timezone.map(Arc::from), is_try)?;
                return Ok(Arc::new(ScalarUDF::from(udf)));
            }
            UdfKind::SparkDate(gen::SparkDateUdf { is_try }) => {
                return Ok(Arc::new(ScalarUDF::from(SparkDate::new(is_try))));
            }
            UdfKind::SparkTime(gen::SparkTimeUdf { is_try }) => {
                return Ok(Arc::new(ScalarUDF::from(SparkTime::new(is_try))));
            }
            UdfKind::SparkFromCsv(gen::SparkFromCsvUdf { session_timezone }) => {
                let udf = SparkFromCSV::new(Arc::from(session_timezone));
                return Ok(Arc::new(ScalarUDF::from(udf)));
            }
            UdfKind::SparkToCsv(gen::SparkToCsvUdf { session_timezone }) => {
                let udf = SparkToCsv::new(Arc::from(session_timezone));
                return Ok(Arc::new(ScalarUDF::from(udf)));
            }
            UdfKind::SparkFromJson(gen::SparkFromJsonUdf { session_timezone }) => {
                let udf = SparkFromJson::new(Arc::from(session_timezone));
                return Ok(Arc::new(ScalarUDF::from(udf)));
            }
            UdfKind::SparkVariantGet(gen::SparkVariantGetUdf { safe }) => {
                return Ok(Arc::new(ScalarUDF::from(SparkVariantGet::new(safe))));
            }
            UdfKind::SparkNextDay(gen::SparkNextDayUdf { ansi_mode }) => {
                return Ok(Arc::new(ScalarUDF::from(SparkNextDay::new(ansi_mode))));
            }
            UdfKind::SparkWindowBuckets(gen::SparkWindowBucketsUdf {
                window_duration,
                slide_duration,
                start_time,
            }) => {
                return Ok(Arc::new(ScalarUDF::from(SparkWindowBuckets::new(
                    window_duration,
                    slide_duration,
                    start_time,
                ))));
            }
            UdfKind::SparkToNumber(gen::SparkToNumberUdf { safe }) => {
                return Ok(Arc::new(ScalarUDF::from(SparkToNumber::new(safe))));
            }
            UdfKind::SparkToChar(gen::SparkToCharUdf { ansi_mode }) => {
                return Ok(Arc::new(ScalarUDF::from(SparkToChar::new(ansi_mode))));
            }
            UdfKind::SparkAbs(gen::SparkAbsUdf { ansi_mode }) => {
                return Ok(Arc::new(ScalarUDF::from(SparkAbs::new(ansi_mode))));
            }
            UdfKind::SparkBin(gen::SparkBinUdf { ansi_mode }) => {
                return Ok(Arc::new(ScalarUDF::from(SparkBin::new(ansi_mode))));
            }
            UdfKind::SparkPmod(gen::SparkPmodUdf { ansi_mode }) => {
                return Ok(Arc::new(ScalarUDF::from(SparkPmod::new(ansi_mode))));
            }
            UdfKind::SparkNegative(gen::SparkNegativeUdf { ansi_mode }) => {
                return Ok(Arc::new(ScalarUDF::from(SparkNegative::new(ansi_mode))));
            }
            UdfKind::SparkMakeTimestampNtz(gen::SparkMakeTimestampNtzUdf { is_try }) => {
                return Ok(Arc::new(ScalarUDF::from(SparkMakeTimestampNtz::new(
                    is_try,
                ))));
            }
            UdfKind::ConvertTz(gen::ConvertTzUdf { classic }) => {
                return Ok(Arc::new(ScalarUDF::from(ConvertTz::new(classic))));
            }
            UdfKind::SparkParseJson(gen::SparkParseJsonUdf { safe }) => {
                return Ok(Arc::new(ScalarUDF::from(SparkParseJson::new(safe))));
            }
            UdfKind::SparkStructRename(gen::SparkStructRenameUdf { target_type }) => {
                let target_type = self.try_decode_data_type(&target_type)?;
                return Ok(Arc::new(ScalarUDF::from(SparkStructRename::new(
                    target_type,
                ))));
            }
        };
        match name {
            "array_item_with_position" => {
                Ok(Arc::new(ScalarUDF::from(ArrayItemWithPosition::new())))
            }
            "array_struct_field" => Ok(Arc::new(ScalarUDF::from(ArrayStructField::new()))),
            "array_min" => Ok(Arc::new(ScalarUDF::from(ArrayMin::new()))),
            "array_max" => Ok(Arc::new(ScalarUDF::from(ArrayMax::new()))),
            "array_intersect" | "list_intersect" => {
                Ok(Arc::new(ScalarUDF::from(ArrayIntersect::new())))
            }
            "spark_array_position" | "array_position" => {
                Ok(Arc::new(ScalarUDF::from(SparkArrayPosition::new())))
            }
            "spark_array_compact" => Ok(Arc::new(ScalarUDF::from(SparkArrayCompact::new()))),
            "bitmap_count" => Ok(Arc::new(ScalarUDF::from(BitmapCount::new()))),
            "format_string" => Ok(Arc::new(ScalarUDF::from(FormatStringFunc::new()))),
            "greatest" => Ok(Arc::new(ScalarUDF::from(GreatestFunc::new()))),
            "least" => Ok(Arc::new(ScalarUDF::from(LeastFunc::new()))),
            "levenshtein" => Ok(Arc::new(ScalarUDF::from(Levenshtein::new()))),
            "make_valid_utf8" => Ok(Arc::new(ScalarUDF::from(MakeValidUtf8::new()))),
            "map_entries" => Ok(Arc::new(ScalarUDF::from(SparkMapEntries::new()))),
            "map_from_arrays" => Ok(Arc::new(ScalarUDF::from(MapFromArrays::new()))),
            "map_from_entries" => Ok(Arc::new(ScalarUDF::from(MapFromEntries::new()))),
            "multi_expr" => Ok(Arc::new(ScalarUDF::from(MultiExpr::new()))),
            "raise_error" => Ok(Arc::new(ScalarUDF::from(RaiseError::new()))),
            "random_poisson" => Ok(Arc::new(ScalarUDF::from(RandPoisson::new()))),
            "randn" => Ok(Arc::new(ScalarUDF::from(Randn::new()))),
            "spark_cast_to_variant" => Ok(Arc::new(ScalarUDF::from(SparkCastToVariant::new()))),
            "is_variant_null" => Ok(Arc::new(ScalarUDF::from(SparkIsVariantNullUdf::new()))),
            "variant_to_json" => Ok(Arc::new(ScalarUDF::from(SparkVariantToJsonUdf::new()))),
            "spark_variant_explode" => Ok(Arc::new(ScalarUDF::from(SparkVariantExplodeUdf::new()))),
            "to_variant_object" => Ok(Arc::new(ScalarUDF::from(SparkToVariantObjectUdf::new()))),
            "schema_of_variant" => Ok(Arc::new(ScalarUDF::from(SparkSchemaOfVariantUdf::new()))),
            "random" | "rand" => Ok(Arc::new(ScalarUDF::from(Random::new()))),
            "randstr" => Ok(Arc::new(ScalarUDF::from(Randstr::new()))),
            "format_number" => Ok(Arc::new(ScalarUDF::from(FormatNumber::new()))),
            "soundex" => Ok(Arc::new(ScalarUDF::from(Soundex::new()))),
            "quote" => Ok(Arc::new(ScalarUDF::from(SparkQuote::new()))),
            "st_asbinary" => Ok(Arc::new(ScalarUDF::from(StAsBinary::new()))),
            "st_geomfromwkb" => Ok(Arc::new(ScalarUDF::from(StGeomFromWKB::new()))),
            "st_geogfromwkb" => Ok(Arc::new(ScalarUDF::from(StGeogFromWKB::new()))),
            "spark_array" | "spark_make_array" | "array" => {
                Ok(Arc::new(ScalarUDF::from(SparkArray::new())))
            }
            "spark_concat" | "concat" | "array_concat" => {
                Ok(Arc::new(ScalarUDF::from(SparkConcat::new())))
            }
            "spark_split" | "split" => Ok(Arc::new(ScalarUDF::from(SparkSplit::new()))),
            "regexp_extract" => Ok(Arc::new(ScalarUDF::from(SparkRegexpExtract::new()))),
            "regexp_extract_all" => Ok(Arc::new(ScalarUDF::from(SparkRegexpExtractAll::new()))),
            "sentences" => Ok(Arc::new(ScalarUDF::from(SparkSentences::new()))),
            "spark_hex" | "hex" => Ok(Arc::new(ScalarUDF::from(SparkHex::new()))),
            "spark_unhex" | "unhex" => Ok(Arc::new(ScalarUDF::from(SparkUnHex::new()))),
            "spark_murmur3_hash" | "hash" => Ok(Arc::new(ScalarUDF::from(SparkMurmur3Hash::new()))),
            "spark_reverse" | "reverse" => Ok(Arc::new(ScalarUDF::from(SparkReverse::new()))),
            "spark_xxhash64" | "xxhash64" => Ok(Arc::new(ScalarUDF::from(SparkXxhash64::new()))),
            "hll_sketch_estimate" => {
                Ok(Arc::new(ScalarUDF::from(HllSketchEstimateFunction::new())))
            }
            "hll_union" => Ok(Arc::new(ScalarUDF::from(HllUnionFunction::new()))),
            "theta_difference" => Ok(Arc::new(ScalarUDF::from(ThetaDifferenceFunction::new()))),
            "theta_intersection" => Ok(Arc::new(ScalarUDF::from(ThetaIntersectionFunction::new()))),
            "theta_sketch_estimate" => Ok(Arc::new(ScalarUDF::from(
                ThetaSketchEstimateFunction::new(),
            ))),
            "theta_union" => Ok(Arc::new(ScalarUDF::from(ThetaUnionFunction::new()))),
            "spark_sha1" | "sha" | "sha1" => Ok(Arc::new(ScalarUDF::from(SparkSha1::new()))),
            "crc32" => Ok(Arc::new(ScalarUDF::from(SparkCrc32::new()))),
            "overlay" => Ok(Arc::new(ScalarUDF::from(OverlayFunc::new()))),
            "rewrite_like_pattern" => Ok(Arc::new(ScalarUDF::from(RewriteLikePatternFunc::new()))),
            "json_length" | "json_len" => Ok(sail_function::scalar::json::json_length_udf()),
            "json_as_text" => Ok(sail_function::scalar::json::json_as_text_udf()),
            "json_object_keys" | "json_keys" => {
                Ok(sail_function::scalar::json::json_object_keys_udf())
            }
            "spark_schema_of_json" | "schema_of_json" => {
                Ok(Arc::new(ScalarUDF::from(SparkSchemaOfJson::new())))
            }
            "schema_of_csv" => Ok(Arc::new(ScalarUDF::from(SparkSchemaOfCsv::new()))),
            "xpath" => Ok(Arc::new(ScalarUDF::from(
                sail_function::scalar::xml::xpath::Xpath::new(),
            ))),
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
            "spark_year" | "year" => Ok(Arc::new(ScalarUDF::from(SparkYear::new()))),
            "spark_luhn_check" | "luhn_check" => {
                Ok(Arc::new(ScalarUDF::from(SparkLuhnCheck::new())))
            }
            "negate_duration" => Ok(Arc::new(ScalarUDF::from(NegateDuration::new()))),
            "spark_make_dt_interval" | "make_dt_interval" => {
                Ok(Arc::new(ScalarUDF::from(SparkMakeDtInterval::new())))
            }
            "spark_make_interval" | "make_interval" => {
                Ok(Arc::new(ScalarUDF::from(SparkMakeInterval::new())))
            }
            "spark_make_ym_interval" | "make_ym_interval" => {
                Ok(Arc::new(ScalarUDF::from(SparkMakeYmInterval::new())))
            }
            "spark_make_time" | "make_time" => Ok(Arc::new(ScalarUDF::from(SparkMakeTime::new()))),
            "date_trunc" => Ok(Arc::new(ScalarUDF::from(SparkDateTrunc::new()))),
            "spark_time_diff" | "time_diff" => Ok(Arc::new(ScalarUDF::from(SparkTimeDiff::new()))),
            "spark_time_trunc" | "time_trunc" => {
                Ok(Arc::new(ScalarUDF::from(SparkTimeTrunc::new())))
            }
            "spark_mask" | "mask" => Ok(Arc::new(ScalarUDF::from(SparkMask::new()))),
            "spark_concat_ws" | "concat_ws" => Ok(Arc::new(ScalarUDF::from(SparkConcatWs::new()))),
            "spark_sequence" | "sequence" => Ok(Arc::new(ScalarUDF::from(SparkSequence::new()))),
            "spark_shuffle" | "shuffle" => Ok(Arc::new(ScalarUDF::from(SparkShuffle::new()))),
            "spark_encode" | "encode" => Ok(Arc::new(ScalarUDF::from(SparkEncode::new()))),
            "spark_elt" | "elt" => Ok(Arc::new(ScalarUDF::from(SparkElt::new()))),
            "spark_decode" | "decode" => Ok(Arc::new(ScalarUDF::from(SparkDecode::new()))),
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
            "spark_to_json" | "to_json" => Ok(Arc::new(ScalarUDF::from(SparkToJson::new()))),
            "spark_try_subtract" | "try_subtract" => {
                Ok(Arc::new(ScalarUDF::from(SparkTrySubtract::new())))
            }
            "spark_uniform" | "uniform" => Ok(Arc::new(ScalarUDF::from(SparkUniform::new()))),
            "spark_width_bucket" | "width_bucket" => {
                Ok(Arc::new(ScalarUDF::from(SparkWidthBucket::new())))
            }
            "str_to_map" => Ok(Arc::new(ScalarUDF::from(StrToMap::new()))),
            "parse_url" => Ok(Arc::new(ScalarUDF::from(ParseUrl::new()))),
            "try_parse_url" | "spark_try_parse_url" => {
                Ok(Arc::new(ScalarUDF::from(SparkTryParseUrl::new())))
            }
            "try_url_decode" => Ok(Arc::new(ScalarUDF::from(TryUrlDecode::new()))),
            "url_decode" => Ok(Arc::new(ScalarUDF::from(UrlDecode::new()))),
            "url_encode" => Ok(Arc::new(ScalarUDF::from(UrlEncode::new()))),
            _ => plan_err!("could not find scalar function: {name}"),
        }
    }

    fn try_encode_udf(&self, node: &ScalarUDF, buf: &mut Vec<u8>) -> Result<()> {
        // TODO: Implement custom registry to avoid codec for built-in functions
        let node_inner = node.inner();
        let udf_kind: UdfKind = if node_inner.is::<ArrayItemWithPosition>()
            || node_inner.is::<ArrayStructField>()
            || node_inner.is::<ArrayMax>()
            || node_inner.is::<ArrayMin>()
            || node_inner.is::<ArrayIntersect>()
            || node_inner.is::<SparkArrayPosition>()
            || node_inner.is::<SparkArrayCompact>()
            || node_inner.is::<BitmapCount>()
            || node_inner.is::<FormatStringFunc>()
            || node_inner.is::<GreatestFunc>()
            || node_inner.is::<LeastFunc>()
            || node_inner.is::<FormatNumber>()
            || node_inner.is::<Levenshtein>()
            || node_inner.is::<Randstr>()
            || node_inner.is::<Soundex>()
            || node_inner.is::<SparkQuote>()
            || node_inner.is::<StAsBinary>()
            || node_inner.is::<StGeomFromWKB>()
            || node_inner.is::<StGeogFromWKB>()
            || node_inner.is::<MakeValidUtf8>()
            || node_inner.is::<SparkMapEntries>()
            || node_inner.is::<MapFromArrays>()
            || node_inner.is::<MapFromEntries>()
            || node_inner.is::<MultiExpr>()
            || node_inner.is::<NegateDuration>()
            || node_inner.is::<OverlayFunc>()
            || node_inner.is::<ParseUrl>()
            || node_inner.is::<RaiseError>()
            || node_inner.is::<Randn>()
            || node_inner.is::<Random>()
            || node_inner.is::<RandPoisson>()
            || node_inner.is::<RewriteLikePatternFunc>()
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
            || node_inner.is::<SparkDateTrunc>()
            || node_inner.is::<SparkDayTimeInterval>()
            || node_inner.is::<SparkDecode>()
            || node_inner.is::<SparkElt>()
            || node_inner.is::<SparkEncode>()
            || node_inner.is::<SparkExpm1>()
            || node_inner.is::<SparkFloor>()
            || node_inner.is::<SparkHex>()
            || node_inner.is::<SparkIntervalDiv>()
            || node_inner.is::<SparkCastToVariant>()
            || node_inner.is::<SparkIsVariantNullUdf>()
            || node_inner.is::<SparkVariantExplodeUdf>()
            || node_inner.is::<SparkToVariantObjectUdf>()
            || node_inner.is::<SparkSchemaOfVariantUdf>()
            || node_inner.is::<SparkLastDay>()
            || node_inner.is::<SparkYear>()
            || node_inner.is::<SparkLuhnCheck>()
            || node_inner.is::<SparkMakeDtInterval>()
            || node_inner.is::<SparkMakeInterval>()
            || node_inner.is::<SparkMakeTime>()
            || node_inner.is::<SparkTimeDiff>()
            || node_inner.is::<SparkTimeTrunc>()
            || node_inner.is::<SparkMakeYmInterval>()
            || node_inner.is::<SparkMask>()
            || node_inner.is::<SparkConcatWs>()
            || node_inner.is::<SparkMurmur3Hash>()
            || node_inner.is::<SparkRegexpExtract>()
            || node_inner.is::<SparkRegexpExtractAll>()
            || node_inner.is::<SparkReverse>()
            || node_inner.is::<SparkSequence>()
            || node_inner.is::<SparkSchemaOfCsv>()
            || node_inner.is::<SparkSchemaOfJson>()
            || node_inner.is::<SparkShuffle>()
            || node_inner.is::<SparkSha1>()
            || node_inner.is::<SparkSignum>()
            || node_inner.is::<SparkSentences>()
            || node_inner.is::<SparkSplit>()
            || node_inner.is::<SparkToBinary>()
            || node_inner.is::<SparkToChronoFmt>()
            || node_inner.is::<SparkToLargeUtf8>()
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
            || node_inner.is::<SparkTryToTimestamp>()
            || node_inner.is::<HllSketchEstimateFunction>()
            || node_inner.is::<HllUnionFunction>()
            || node_inner.is::<ThetaDifferenceFunction>()
            || node_inner.is::<ThetaIntersectionFunction>()
            || node_inner.is::<ThetaSketchEstimateFunction>()
            || node_inner.is::<ThetaUnionFunction>()
            || node_inner.is::<SparkUnbase64>()
            || node_inner.is::<SparkUniform>()
            || node_inner.is::<SparkUnHex>()
            || node_inner.is::<SparkVariantToJsonUdf>()
            || node_inner.is::<SparkVersion>()
            || node_inner.is::<SparkWidthBucket>()
            || node_inner.is::<SparkXxhash64>()
            || node_inner.is::<SparkYearMonthInterval>()
            || node_inner.is::<StrToMap>()
            || node_inner.is::<SparkToJson>()
            || node_inner.is::<TryUrlDecode>()
            || node_inner.is::<UrlDecode>()
            || node_inner.is::<UrlEncode>()
            || node_inner.is::<Xpath>()
            || node.name() == "json_as_text"
            || node.name() == "json_len"
            || node.name() == "json_length"
        {
            UdfKind::Standard(gen::StandardUdf {})
        } else if let Some(func) = node.inner().downcast_ref::<PySparkUDF>() {
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
        } else if let Some(func) = node.inner().downcast_ref::<PySparkCoGroupMapUDF>() {
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
        } else if let Some(func) = node.inner().downcast_ref::<DropStructField>() {
            let field_names = func.field_names().to_vec();
            UdfKind::DropStructField(gen::DropStructFieldUdf { field_names })
        } else if let Some(_func) = node.inner().downcast_ref::<Explode>() {
            let name = node.name().to_string();
            UdfKind::Explode(gen::ExplodeUdf { name })
        } else if let Some(_func) = node.inner().downcast_ref::<XpathTyped>() {
            let name = node.name().to_string();
            UdfKind::XpathTyped(gen::XpathTypedUdf { name })
        } else if let Some(func) = node.inner().downcast_ref::<SparkToXml>() {
            let session_timezone = func.session_timezone().to_string();
            UdfKind::SparkToXml(gen::SparkToXmlUdf { session_timezone })
        } else if let Some(func) = node.inner().downcast_ref::<SparkUnixTimestamp>() {
            let timezone = func.timezone().to_string();
            UdfKind::SparkUnixTimestamp(gen::SparkUnixTimestampUdf { timezone })
        } else if let Some(func) = node.inner().downcast_ref::<StructFunction>() {
            let field_names = func.field_names().to_vec();
            UdfKind::StructFunction(gen::StructFunctionUdf { field_names })
        } else if let Some(func) = node.inner().downcast_ref::<ArraysZip>() {
            let field_names = func.field_names().to_vec();
            UdfKind::ArraysZip(gen::ArraysZipUdf { field_names })
        } else if let Some(func) = node.inner().downcast_ref::<UpdateStructField>() {
            let field_names = func.field_names().to_vec();
            UdfKind::UpdateStructField(gen::UpdateStructFieldUdf { field_names })
        } else if let Some(func) = node.inner().downcast_ref::<TimestampNow>() {
            let timezone = func.timezone().to_string();
            let time_unit: gen_datafusion_common::TimeUnit = func.time_unit().into();
            let time_unit = time_unit.as_str_name().to_string();
            UdfKind::TimestampNow(gen::TimestampNowUdf {
                timezone,
                time_unit,
            })
        } else if let Some(func) = node.inner().downcast_ref::<SparkTimestamp>() {
            let timezone = func.timezone().map(|x| x.to_string());
            let is_try = func.is_try();
            UdfKind::SparkTimestamp(gen::SparkTimestampUdf { timezone, is_try })
        } else if let Some(func) = node.inner().downcast_ref::<SparkDate>() {
            let is_try = func.is_try();
            UdfKind::SparkDate(gen::SparkDateUdf { is_try })
        } else if let Some(func) = node.inner().downcast_ref::<SparkTime>() {
            let is_try = func.is_try();
            UdfKind::SparkTime(gen::SparkTimeUdf { is_try })
        } else if let Some(func) = node.inner().downcast_ref::<SparkVariantGet>() {
            let safe = func.safe();
            UdfKind::SparkVariantGet(gen::SparkVariantGetUdf { safe })
        } else if let Some(func) = node.inner().downcast_ref::<SparkParseJson>() {
            let safe = func.safe();
            UdfKind::SparkParseJson(gen::SparkParseJsonUdf { safe })
        } else if let Some(func) = node.inner().downcast_ref::<SparkFromCSV>() {
            let session_timezone = func.session_timezone().to_string();
            UdfKind::SparkFromCsv(gen::SparkFromCsvUdf { session_timezone })
        } else if let Some(func) = node.inner().downcast_ref::<SparkToCsv>() {
            let session_timezone = func.session_timezone().to_string();
            UdfKind::SparkToCsv(gen::SparkToCsvUdf { session_timezone })
        } else if let Some(func) = node.inner().downcast_ref::<SparkFromJson>() {
            let session_timezone = func.session_timezone().to_string();
            UdfKind::SparkFromJson(gen::SparkFromJsonUdf { session_timezone })
        } else if let Some(func) = node.inner().downcast_ref::<SparkNextDay>() {
            let ansi_mode = func.ansi_mode();
            UdfKind::SparkNextDay(gen::SparkNextDayUdf { ansi_mode })
        } else if let Some(func) = node.inner().downcast_ref::<SparkWindowBuckets>() {
            UdfKind::SparkWindowBuckets(gen::SparkWindowBucketsUdf {
                window_duration: func.window_duration(),
                slide_duration: func.slide_duration(),
                start_time: func.start_time(),
            })
        } else if let Some(func) = node.inner().downcast_ref::<SparkToNumber>() {
            let safe = func.safe();
            UdfKind::SparkToNumber(gen::SparkToNumberUdf { safe })
        } else if let Some(func) = node.inner().downcast_ref::<SparkToChar>() {
            let ansi_mode = func.ansi_mode();
            UdfKind::SparkToChar(gen::SparkToCharUdf { ansi_mode })
        } else if let Some(func) = node.inner().downcast_ref::<SparkAbs>() {
            let ansi_mode = func.ansi_mode();
            UdfKind::SparkAbs(gen::SparkAbsUdf { ansi_mode })
        } else if let Some(func) = node.inner().downcast_ref::<SparkBin>() {
            let ansi_mode = func.ansi_mode();
            UdfKind::SparkBin(gen::SparkBinUdf { ansi_mode })
        } else if let Some(func) = node.inner().downcast_ref::<SparkPmod>() {
            let ansi_mode = func.ansi_mode();
            UdfKind::SparkPmod(gen::SparkPmodUdf { ansi_mode })
        } else if let Some(func) = node.inner().downcast_ref::<SparkNegative>() {
            let ansi_mode = func.ansi_mode();
            UdfKind::SparkNegative(gen::SparkNegativeUdf { ansi_mode })
        } else if let Some(func) = node.inner().downcast_ref::<SparkMakeTimestampNtz>() {
            let is_try = func.is_try();
            UdfKind::SparkMakeTimestampNtz(gen::SparkMakeTimestampNtzUdf { is_try })
        } else if let Some(func) = node.inner().downcast_ref::<ConvertTz>() {
            let classic = func.classic();
            UdfKind::ConvertTz(gen::ConvertTzUdf { classic })
        } else if let Some(func) = node.inner().downcast_ref::<SparkStructRename>() {
            let target_type = self.try_encode_data_type(func.target_type())?;
            UdfKind::SparkStructRename(gen::SparkStructRenameUdf { target_type })
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
                "bitmap_and_agg" => Ok(Arc::new(AggregateUDF::from(BitmapAndAggFunction::new()))),
                "bitmap_construct_agg" => Ok(Arc::new(AggregateUDF::from(
                    BitmapConstructAggFunction::new(),
                ))),
                "bitmap_or_agg" => Ok(Arc::new(AggregateUDF::from(BitmapOrAggFunction::new()))),
                "count_min_sketch" => {
                    Ok(Arc::new(AggregateUDF::from(CountMinSketchFunction::new())))
                }
                "grouping_id" => Ok(Arc::new(AggregateUDF::from(GroupingIdFunction::new()))),
                "histogram_numeric" => Ok(Arc::new(AggregateUDF::from(
                    HistogramNumericFunction::new(),
                ))),
                "hll_sketch_agg" => Ok(Arc::new(AggregateUDF::from(HllSketchAggFunction::new()))),
                "hll_union_agg" => Ok(Arc::new(AggregateUDF::from(HllUnionAggFunction::new()))),
                "kurtosis" => Ok(Arc::new(AggregateUDF::from(KurtosisFunction::new()))),
                "max_by" => Ok(Arc::new(AggregateUDF::from(MaxByFunction::new()))),
                "min_by" => Ok(Arc::new(AggregateUDF::from(MinByFunction::new()))),
                "mode" => Ok(Arc::new(AggregateUDF::from(ModeFunction::new()))),
                "percentile" => Ok(Arc::new(AggregateUDF::from(PercentileFunction::new()))),
                "percentile_disc" => Ok(Arc::new(AggregateUDF::from(PercentileDisc::new()))),
                "product" => Ok(Arc::new(AggregateUDF::from(ProductFunction::new()))),
                "regr_avgx" => Ok(Arc::new(AggregateUDF::from(Regr::new(
                    RegrType::AvgX,
                    "regr_avgx",
                )))),
                "regr_avgy" => Ok(Arc::new(AggregateUDF::from(Regr::new(
                    RegrType::AvgY,
                    "regr_avgy",
                )))),
                "regr_count" => Ok(Arc::new(AggregateUDF::from(Regr::new(
                    RegrType::Count,
                    "regr_count",
                )))),
                "regr_intercept" => Ok(Arc::new(AggregateUDF::from(Regr::new(
                    RegrType::Intercept,
                    "regr_intercept",
                )))),
                "regr_r2" => Ok(Arc::new(AggregateUDF::from(Regr::new(
                    RegrType::R2,
                    "regr_r2",
                )))),
                "regr_slope" => Ok(Arc::new(AggregateUDF::from(Regr::new(
                    RegrType::Slope,
                    "regr_slope",
                )))),
                "regr_sxx" => Ok(Arc::new(AggregateUDF::from(Regr::new(
                    RegrType::Sxx,
                    "regr_sxx",
                )))),
                "regr_sxy" => Ok(Arc::new(AggregateUDF::from(Regr::new(
                    RegrType::Sxy,
                    "regr_sxy",
                )))),
                "regr_syy" => Ok(Arc::new(AggregateUDF::from(Regr::new(
                    RegrType::Syy,
                    "regr_syy",
                )))),
                "schema_of_variant_agg" => Ok(Arc::new(AggregateUDF::from(
                    SchemaOfVariantAggFunction::new(),
                ))),
                "skewness" => Ok(Arc::new(AggregateUDF::from(SkewnessFunc::new()))),
                "theta_intersection_agg" => Ok(Arc::new(AggregateUDF::from(
                    ThetaIntersectionAggFunction::new(),
                ))),
                "theta_sketch_agg" => {
                    Ok(Arc::new(AggregateUDF::from(ThetaSketchAggFunction::new())))
                }
                "theta_union_agg" => Ok(Arc::new(AggregateUDF::from(ThetaUnionAggFunction::new()))),
                "try_avg" => Ok(Arc::new(AggregateUDF::from(TryAvgFunction::new()))),
                "try_sum" => Ok(Arc::new(AggregateUDF::from(SparkTrySum::new()))),
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
                kind,
                actual_arg_count,
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
                let kind = self.try_decode_pyspark_group_agg_kind(kind)?;
                let actual_arg_count = actual_arg_count
                    .map(|c| c as usize)
                    .unwrap_or(input_types.len()); // backward compat: all inputs are real
                let udaf = PySparkGroupAggregateUDF::new(
                    kind,
                    name,
                    payload,
                    deterministic,
                    input_names,
                    input_types,
                    output_type,
                    Arc::new(config),
                    actual_arg_count,
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
                is_iter,
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
                    PySparkGroupMapMode { is_pandas, is_iter },
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
            None => plan_err!("ExtendedAggregateUdf: no UDF found for {name}"),
        }
    }

    fn try_encode_udaf(&self, node: &AggregateUDF, buf: &mut Vec<u8>) -> Result<()> {
        let udaf_kind = if node.inner().is::<BitmapAndAggFunction>()
            || node.inner().is::<BitmapConstructAggFunction>()
            || node.inner().is::<BitmapOrAggFunction>()
            || node.inner().is::<CountMinSketchFunction>()
            || node.inner().is::<GroupingIdFunction>()
            || node.inner().is::<HistogramNumericFunction>()
            || node.inner().is::<HllSketchAggFunction>()
            || node.inner().is::<HllUnionAggFunction>()
            || node.inner().is::<KurtosisFunction>()
            || node.inner().is::<MaxByFunction>()
            || node.inner().is::<MinByFunction>()
            || node.inner().is::<ModeFunction>()
            || node.inner().is::<PercentileFunction>()
            || node.inner().is::<PercentileDisc>()
            || node.inner().is::<ProductFunction>()
            || node.inner().is::<Regr>()
            || node.inner().is::<SchemaOfVariantAggFunction>()
            || node.inner().is::<SkewnessFunc>()
            || node.inner().is::<ThetaIntersectionAggFunction>()
            || node.inner().is::<ThetaSketchAggFunction>()
            || node.inner().is::<ThetaUnionAggFunction>()
            || node.inner().is::<TryAvgFunction>()
            || node.inner().is::<SparkTrySum>()
        {
            UdafKind::Standard(gen::StandardUdaf {})
        } else if let Some(func) = node.inner().downcast_ref::<PySparkGroupAggregateUDF>() {
            let input_types = func
                .input_types()
                .iter()
                .map(|x| self.try_encode_data_type(x))
                .collect::<Result<Vec<_>>>()?;
            let output_type = self.try_encode_data_type(func.output_type())?;
            let config = self.try_encode_pyspark_udf_config(func.config())?;
            let kind = self.try_encode_pyspark_group_agg_kind(func.kind())?;
            UdafKind::PySparkGroupAgg(gen::PySparkGroupAggUdaf {
                name: func.name().to_string(),
                payload: func.payload().to_vec(),
                deterministic: func.deterministic(),
                input_names: func.input_names().to_vec(),
                input_types,
                output_type,
                config: Some(config),
                kind,
                actual_arg_count: Some(func.actual_arg_count() as u64),
            })
        } else if let Some(func) = node.inner().downcast_ref::<PySparkGroupMapUDF>() {
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
                is_iter: func.is_iter(),
                config: Some(config),
            })
        } else if let Some(func) = node.inner().downcast_ref::<PySparkBatchCollectorUDF>() {
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

    fn try_decode_udwf(&self, name: &str, buf: &[u8]) -> Result<Arc<WindowUDF>> {
        let udwf = ExtendedWindowUdf::decode(buf)
            .map_err(|e| plan_datafusion_err!("failed to decode udwf: {e}"))?;
        let ExtendedWindowUdf { udwf_kind } = udwf;
        match udwf_kind {
            Some(UdwfKind::Standard(gen::StandardUdwf {})) => match name {
                "ntile" => Ok(Arc::new(WindowUDF::from(SparkNtile::new()))),
                _ => plan_err!("Could not find Window Function: {name}"),
            },
            None => plan_err!("ExtendedWindowUdf: no UDWF found for {name}"),
        }
    }

    fn try_encode_udwf(&self, node: &WindowUDF, buf: &mut Vec<u8>) -> Result<()> {
        if !node.inner().is::<SparkNtile>() {
            return Ok(());
        }
        let node = ExtendedWindowUdf {
            udwf_kind: Some(UdwfKind::Standard(gen::StandardUdwf {})),
        };
        node.encode(buf)
            .map_err(|e| plan_datafusion_err!("failed to encode udwf: {e}"))
    }

    fn try_decode_expr(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn PhysicalExpr>],
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let node = ExtendedPhysicalExprNode::decode(buf)
            .map_err(|e| plan_datafusion_err!("failed to decode physical expr: {e}"))?;
        let expr_kind = node
            .expr_kind
            .ok_or_else(|| plan_datafusion_err!("missing physical expr node"))?;
        match expr_kind {
            ExprKind::SchemaEvolutionCast(node) => {
                let (input, input_field, target_field) = self.try_decode_cast_column_expr(
                    node,
                    inputs,
                    "SchemaEvolutionCastColumnExpr",
                )?;
                Ok(Arc::new(SchemaEvolutionCastColumnExpr::new(
                    input,
                    input_field,
                    target_field,
                    None,
                )))
            }
            ExprKind::HigherOrderFunction(node) => {
                let fun = Self::try_decode_higher_order_udf(node.udf)?;
                let input_schema = self.try_decode_schema(&node.input_schema)?;
                // The planner's `ConfigOptions` are not serialized. This is safe
                // because `filter` does not read config (its lambda parameters and
                // result type are config-independent). Revisit when adding a
                // higher-order function whose behavior depends on `ConfigOptions`
                // (e.g. ANSI mode) — that config would need to be carried here.
                let inner = HigherOrderFunctionExpr::try_new_with_schema(
                    fun,
                    inputs.to_vec(),
                    &input_schema,
                    Arc::new(ConfigOptions::default()),
                )?;
                Ok(Arc::new(DistributedHigherOrderExpr::new(
                    Arc::new(inner),
                    Arc::new(input_schema),
                )))
            }
            ExprKind::Lambda(node) => {
                if inputs.len() != 1 {
                    return plan_err!("LambdaExpr expects exactly one input, got {}", inputs.len());
                }
                Ok(Arc::new(LambdaExpr::try_new(
                    node.params,
                    Arc::clone(&inputs[0]),
                )?))
            }
            ExprKind::LambdaVariable(node) => {
                let schema = self.try_decode_schema(&node.field)?;
                let field = schema
                    .fields()
                    .first()
                    .ok_or_else(|| plan_datafusion_err!("LambdaVariable missing field"))?
                    .clone();
                let index = usize::try_from(node.index).map_err(|_| {
                    plan_datafusion_err!(
                        "LambdaVariable index {} does not fit in usize",
                        node.index
                    )
                })?;
                Ok(Arc::new(LambdaVariable::new(index, field)))
            }
        }
    }

    fn try_encode_expr(&self, node: &Arc<dyn PhysicalExpr>, buf: &mut Vec<u8>) -> Result<()> {
        let expr_kind = if let Some(cast) = node.downcast_ref::<SchemaEvolutionCastColumnExpr>() {
            let node = self.try_encode_cast_column_expr(
                cast.input_field().as_ref(),
                cast.target_field().as_ref(),
            )?;
            ExprKind::SchemaEvolutionCast(node)
        } else if let Some(w) = node.downcast_ref::<DistributedHigherOrderExpr>() {
            let hof = w
                .inner()
                .downcast_ref::<HigherOrderFunctionExpr>()
                .ok_or_else(|| {
                    plan_datafusion_err!(
                        "DistributedHigherOrderExpr inner is not a HigherOrderFunctionExpr"
                    )
                })?;
            let input_schema = self.try_encode_schema(w.input_schema().as_ref())?;
            ExprKind::HigherOrderFunction(HigherOrderFunctionExprNode {
                input_schema,
                udf: Some(Self::try_encode_higher_order_udf(hof)?),
            })
        } else if let Some(lambda) = node.downcast_ref::<LambdaExpr>() {
            ExprKind::Lambda(LambdaExprNode {
                params: lambda.params().to_vec(),
            })
        } else if let Some(var) = node.downcast_ref::<LambdaVariable>() {
            let index = var.index() as u64;
            let field = self.try_encode_schema(&Schema::new(vec![var.field().as_ref().clone()]))?;
            ExprKind::LambdaVariable(LambdaVariableExprNode { index, field })
        } else if let Some(hof) = node.downcast_ref::<HigherOrderFunctionExpr>() {
            // An unwrapped higher-order function reached serialization, which means
            // it appeared in a plan node that `WrapHigherOrderFunctions` does not
            // cover (it handles `ProjectionExec`, `FilterExec` and `SortExec`).
            // Fail with a clear message (naming the function) instead of the
            // generic "unsupported extension" error.
            return plan_err!(
                "higher-order function `{}` in an unsupported plan node cannot be \
                 serialized for distributed execution (covered nodes: projection, \
                 filter, sort)",
                hof.name()
            );
        } else {
            return plan_err!("unsupported physical expr extension");
        };

        let node = ExtendedPhysicalExprNode {
            expr_kind: Some(expr_kind),
        };
        node.encode(buf)
            .map_err(|e| plan_datafusion_err!("failed to encode physical expr: {e}"))
    }
}

impl RemoteExecutionCodec {
    /// Encodes the concrete higher-order UDF of `hof` using a one-variant-per-function
    /// `oneof` (the same pattern as `ExtendedScalarUdf`). Add a branch here for each
    /// new higher-order function (`transform`, `exists`, `forall`, ...).
    fn try_encode_higher_order_udf(hof: &HigherOrderFunctionExpr) -> Result<gen::HigherOrderUdf> {
        let udf_inner = hof.fun().inner().as_ref() as &dyn std::any::Any;
        let udf_kind = if let Some(filter) = udf_inner.downcast_ref::<SparkArrayFilter>() {
            HigherOrderUdfKind::Filter(gen::SparkArrayFilterUdf {
                index_first: filter.is_index_first(),
            })
        } else {
            return plan_err!("unsupported higher-order function: {}", hof.name());
        };
        Ok(gen::HigherOrderUdf {
            udf_kind: Some(udf_kind),
        })
    }

    /// Rebuilds the higher-order UDF from its `oneof` encoding. The inverse of
    /// [`Self::try_encode_higher_order_udf`].
    fn try_decode_higher_order_udf(
        udf: Option<gen::HigherOrderUdf>,
    ) -> Result<Arc<HigherOrderUDF>> {
        let udf_kind = udf
            .and_then(|udf| udf.udf_kind)
            .ok_or_else(|| plan_datafusion_err!("missing higher-order function UDF"))?;
        Ok(match udf_kind {
            HigherOrderUdfKind::Filter(gen::SparkArrayFilterUdf { index_first }) => {
                if index_first {
                    Arc::new(HigherOrderUDF::new_from_impl(
                        SparkArrayFilter::new_index_first(),
                    ))
                } else {
                    Arc::new(HigherOrderUDF::new_from_impl(SparkArrayFilter::new()))
                }
            }
        })
    }

    #[expect(clippy::type_complexity)]
    fn try_decode_cast_column_expr(
        &self,
        node: CastColumnExprNode,
        inputs: &[Arc<dyn PhysicalExpr>],
        expr_name: &str,
    ) -> Result<(Arc<dyn PhysicalExpr>, Arc<Field>, Arc<Field>)> {
        let CastColumnExprNode {
            input_schema,
            target_schema,
        } = node;
        if inputs.len() != 1 {
            return plan_err!(
                "{expr_name} expects exactly one input, got {}",
                inputs.len()
            );
        }

        let input_schema = self.try_decode_schema(&input_schema)?;
        let target_schema = self.try_decode_schema(&target_schema)?;

        let input_field = input_schema
            .fields()
            .first()
            .ok_or_else(|| plan_datafusion_err!("{expr_name} missing input field"))?
            .as_ref()
            .clone();
        let target_field = target_schema
            .fields()
            .first()
            .ok_or_else(|| plan_datafusion_err!("{expr_name} missing target field"))?
            .as_ref()
            .clone();

        Ok((
            inputs[0].clone(),
            Arc::new(input_field),
            Arc::new(target_field),
        ))
    }

    fn try_encode_cast_column_expr(
        &self,
        input_field: &Field,
        target_field: &Field,
    ) -> Result<CastColumnExprNode> {
        let input_schema = Schema::new(vec![input_field.clone()]);
        let input_schema = self.try_encode_schema(&input_schema)?;
        let target_schema = Schema::new(vec![target_field.clone()]);
        let target_schema = self.try_encode_schema(&target_schema)?;
        Ok(CastColumnExprNode {
            input_schema,
            target_schema,
        })
    }

    fn try_decode_physical_sink_mode(
        &self,
        proto_mode: gen::PhysicalSinkMode,
        _schema: &Schema,
        _ctx: &TaskContext,
    ) -> Result<PhysicalSinkMode> {
        let gen::PhysicalSinkMode { mode } = proto_mode;
        match mode {
            Some(gen::physical_sink_mode::Mode::Append(gen::AppendMode {})) => {
                Ok(PhysicalSinkMode::Append)
            }
            Some(gen::physical_sink_mode::Mode::Overwrite(gen::OverwriteMode {})) => {
                Ok(PhysicalSinkMode::Overwrite)
            }
            Some(gen::physical_sink_mode::Mode::OverwriteIf(gen::OverwriteIfMode { source })) => {
                Ok(PhysicalSinkMode::OverwriteIf {
                    condition: None,
                    source,
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
            PhysicalSinkMode::OverwriteIf { source, .. } => {
                gen::physical_sink_mode::Mode::OverwriteIf(gen::OverwriteIfMode {
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

    fn try_decode_delta_snapshot_context(
        &self,
        context: gen::DeltaSnapshotContext,
    ) -> Result<DeltaSnapshotContext> {
        Ok(DeltaSnapshotContext {
            version: context.version,
            protocol: self.try_decode_json(&context.protocol_json, "Delta protocol")?,
            metadata: self.try_decode_json(&context.metadata_json, "Delta metadata")?,
            txns: self.try_decode_json(&context.txns_json, "Delta transactions")?,
            domain_metadata: self
                .try_decode_json(&context.domain_metadata_json, "Delta domain metadata")?,
            commit_timestamps: self
                .try_decode_json(&context.commit_timestamps_json, "Delta commit timestamps")?,
        })
    }

    fn try_encode_delta_snapshot_context(
        &self,
        context: &DeltaSnapshotContext,
    ) -> Result<gen::DeltaSnapshotContext> {
        Ok(gen::DeltaSnapshotContext {
            version: context.version,
            protocol_json: self.try_encode_json(&context.protocol, "Delta protocol")?,
            metadata_json: self.try_encode_json(&context.metadata, "Delta metadata")?,
            txns_json: self.try_encode_json(&context.txns, "Delta transactions")?,
            domain_metadata_json: self
                .try_encode_json(&context.domain_metadata, "Delta domain metadata")?,
            commit_timestamps_json: self
                .try_encode_json(&context.commit_timestamps, "Delta commit timestamps")?,
        })
    }

    fn try_decode_delta_commit_context(
        &self,
        context: gen::DeltaCommitContext,
    ) -> Result<DeltaCommitContext> {
        let base_snapshot = context
            .base_snapshot
            .map(|context| self.try_decode_delta_snapshot_context(context))
            .transpose()?;
        Ok(DeltaCommitContext { base_snapshot })
    }

    fn try_encode_delta_commit_context(
        &self,
        context: &DeltaCommitContext,
    ) -> Result<gen::DeltaCommitContext> {
        let base_snapshot = context
            .base_snapshot
            .as_ref()
            .map(|context| self.try_encode_delta_snapshot_context(context))
            .transpose()?;
        Ok(gen::DeltaCommitContext { base_snapshot })
    }

    fn try_decode_delta_write_context(
        &self,
        context: gen::DeltaWriteContext,
    ) -> Result<DeltaWriteContext> {
        let commit_context = match context.commit_context {
            Some(context) => self.try_decode_delta_commit_context(context)?,
            None => return plan_err!("Missing commit_context for DeltaWriteContext"),
        };
        let initial_actions = context
            .initial_actions_json
            .iter()
            .map(|action| self.try_decode_json::<Action>(action, "Delta initial action"))
            .collect::<Result<Vec<_>>>()?;
        let schema_actions = context
            .schema_actions_json
            .iter()
            .map(|action| self.try_decode_json::<Action>(action, "Delta schema action"))
            .collect::<Result<Vec<_>>>()?;
        let operation = context
            .operation_json
            .as_deref()
            .map(|operation| self.try_decode_json::<DeltaOperation>(operation, "Delta operation"))
            .transpose()?;
        let logical_kernel_for_mapping = context
            .logical_kernel_for_mapping_json
            .as_deref()
            .map(|schema| self.try_decode_json::<StructType>(schema, "Delta logical schema"))
            .transpose()?;

        Ok(DeltaWriteContext {
            commit_context,
            final_schema: self.try_decode_json(&context.final_schema_json, "Delta final schema")?,
            effective_column_mapping_mode: self
                .try_decode_delta_column_mapping_mode(context.effective_column_mapping_mode)?,
            initial_actions,
            schema_actions,
            operation,
            logical_kernel_for_mapping,
            physical_partition_columns: context.physical_partition_columns,
        })
    }

    fn try_encode_delta_write_context(
        &self,
        context: &DeltaWriteContext,
    ) -> Result<gen::DeltaWriteContext> {
        let initial_actions_json = context
            .initial_actions
            .iter()
            .map(|action| self.try_encode_json(action, "Delta initial action"))
            .collect::<Result<Vec<_>>>()?;
        let schema_actions_json = context
            .schema_actions
            .iter()
            .map(|action| self.try_encode_json(action, "Delta schema action"))
            .collect::<Result<Vec<_>>>()?;
        let operation_json = context
            .operation
            .as_ref()
            .map(|operation| self.try_encode_json(operation, "Delta operation"))
            .transpose()?;
        let logical_kernel_for_mapping_json = context
            .logical_kernel_for_mapping
            .as_ref()
            .map(|schema| self.try_encode_json(schema, "Delta logical schema"))
            .transpose()?;

        Ok(gen::DeltaWriteContext {
            commit_context: Some(self.try_encode_delta_commit_context(&context.commit_context)?),
            final_schema_json: self.try_encode_json(&context.final_schema, "Delta final schema")?,
            effective_column_mapping_mode: Self::try_encode_delta_column_mapping_mode(
                context.effective_column_mapping_mode,
            ),
            initial_actions_json,
            schema_actions_json,
            operation_json,
            logical_kernel_for_mapping_json,
            physical_partition_columns: context.physical_partition_columns.clone(),
        })
    }

    fn try_decode_delta_column_mapping_mode(&self, mode: i32) -> Result<ColumnMappingMode> {
        match gen::DeltaColumnMappingMode::try_from(mode)
            .map_err(|_| plan_datafusion_err!("invalid Delta column mapping mode"))?
        {
            gen::DeltaColumnMappingMode::None => Ok(ColumnMappingMode::None),
            gen::DeltaColumnMappingMode::Name => Ok(ColumnMappingMode::Name),
            gen::DeltaColumnMappingMode::Id => Ok(ColumnMappingMode::Id),
        }
    }

    fn try_encode_delta_column_mapping_mode(mode: ColumnMappingMode) -> i32 {
        (match mode {
            ColumnMappingMode::None => gen::DeltaColumnMappingMode::None,
            ColumnMappingMode::Name => gen::DeltaColumnMappingMode::Name,
            ColumnMappingMode::Id => gen::DeltaColumnMappingMode::Id,
        }) as i32
    }

    fn try_decode_json<T: DeserializeOwned>(&self, value: &str, description: &str) -> Result<T> {
        serde_json::from_str(value)
            .map_err(|e| plan_datafusion_err!("failed to decode {description}: {e}"))
    }

    fn try_encode_json<T: Serialize>(&self, value: &T, description: &str) -> Result<String> {
        serde_json::to_string(value)
            .map_err(|e| plan_datafusion_err!("failed to encode {description}: {e}"))
    }

    fn try_decode_lakehouse_table(&self, value: &str) -> Result<Option<LakehouseExecutionContext>> {
        if value.is_empty() {
            Ok(None)
        } else {
            self.try_decode_json(value, "lakehouse execution context")
                .map(Some)
        }
    }

    fn try_encode_lakehouse_table(
        &self,
        value: Option<&LakehouseExecutionContext>,
    ) -> Result<String> {
        value
            .map(|value| self.try_encode_json(value, "lakehouse execution context"))
            .unwrap_or_else(|| Ok(String::new()))
    }

    fn try_decode_catalog_partition_field(
        &self,
        field: gen::CatalogPartitionFieldNode,
    ) -> Result<CatalogPartitionField> {
        let transform_kind = gen::PartitionTransformKind::try_from(field.transform_kind)
            .map_err(|_| plan_datafusion_err!("invalid partition transform kind"))?;
        let transform = match transform_kind {
            gen::PartitionTransformKind::Unspecified | gen::PartitionTransformKind::Identity => {
                None
            }
            gen::PartitionTransformKind::Year => Some(PartitionTransform::Year),
            gen::PartitionTransformKind::Month => Some(PartitionTransform::Month),
            gen::PartitionTransformKind::Day => Some(PartitionTransform::Day),
            gen::PartitionTransformKind::Hour => Some(PartitionTransform::Hour),
            gen::PartitionTransformKind::Bucket => {
                Some(PartitionTransform::Bucket(field.transform_value))
            }
            gen::PartitionTransformKind::Truncate => {
                Some(PartitionTransform::Truncate(field.transform_value))
            }
        };
        Ok(CatalogPartitionField {
            column: field.column,
            transform,
        })
    }

    fn try_encode_catalog_partition_field(
        field: &CatalogPartitionField,
    ) -> Result<gen::CatalogPartitionFieldNode> {
        let (transform_kind, transform_value) = match field.transform {
            None => (gen::PartitionTransformKind::Unspecified as i32, 0),
            Some(PartitionTransform::Identity) => (gen::PartitionTransformKind::Identity as i32, 0),
            Some(PartitionTransform::Year) => (gen::PartitionTransformKind::Year as i32, 0),
            Some(PartitionTransform::Month) => (gen::PartitionTransformKind::Month as i32, 0),
            Some(PartitionTransform::Day) => (gen::PartitionTransformKind::Day as i32, 0),
            Some(PartitionTransform::Hour) => (gen::PartitionTransformKind::Hour as i32, 0),
            Some(PartitionTransform::Bucket(n)) => (gen::PartitionTransformKind::Bucket as i32, n),
            Some(PartitionTransform::Truncate(w)) => {
                (gen::PartitionTransformKind::Truncate as i32, w)
            }
        };
        Ok(gen::CatalogPartitionFieldNode {
            column: field.column.clone(),
            transform_kind,
            transform_value,
        })
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
        let udf = udf as &dyn Any;
        let stream_udf_kind = if let Some(func) = udf.downcast_ref::<PySparkMapIterUDF>() {
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
        } else if let Some(func) = udf.downcast_ref::<PySparkUDTF>() {
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
            parse_physical_sort_exprs(
                &lex_ordering,
                &PhysicalPlanDecodeContext::new(ctx, self),
                schema,
                &DefaultPhysicalProtoConverter {},
            )
            .map_err(|e| plan_datafusion_err!("failed to decode lex ordering: {e}"))?,
        );
        match lex_ordering {
            Some(lex_ordering) => Ok(lex_ordering),
            None => plan_err!("failed to decode lex ordering: invalid sort expressions"),
        }
    }

    fn try_encode_lex_ordering(&self, lex_ordering: &LexOrdering) -> Result<gen::LexOrdering> {
        let lex_ordering = serialize_physical_sort_exprs(
            lex_ordering.to_vec(),
            self,
            &DefaultPhysicalProtoConverter {},
        )?;
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

    fn try_decode_constraint(&self, constraint: &gen::Constraint) -> Result<Constraint> {
        let gen::Constraint { kind: Some(kind) } = constraint else {
            return plan_err!("missing constraint kind");
        };
        match kind {
            gen::constraint::Kind::PrimaryKey(gen::PrimaryKeyConstraint { indices }) => {
                let indices = indices.iter().map(|x| *x as usize).collect();
                Ok(Constraint::PrimaryKey(indices))
            }
            gen::constraint::Kind::Unique(gen::UniqueConstraint { indices }) => {
                let indices = indices.iter().map(|x| *x as usize).collect();
                Ok(Constraint::Unique(indices))
            }
        }
    }

    fn try_encode_constraint(&self, constraint: &Constraint) -> Result<gen::Constraint> {
        let kind = match constraint {
            Constraint::PrimaryKey(indices) => {
                let indices = indices.iter().map(|x| *x as u64).collect();
                gen::constraint::Kind::PrimaryKey(gen::PrimaryKeyConstraint { indices })
            }
            Constraint::Unique(indices) => {
                let indices = indices.iter().map(|x| *x as u64).collect();
                gen::constraint::Kind::Unique(gen::UniqueConstraint { indices })
            }
        };
        Ok(gen::Constraint { kind: Some(kind) })
    }

    fn try_decode_equivalence_class(
        &self,
        class: gen::EquivalenceClass,
        schema: &Schema,
        ctx: &TaskContext,
    ) -> Result<EquivalenceClass> {
        let gen::EquivalenceClass { exprs } = class;
        let exprs = exprs
            .iter()
            .map(|expr| parse_physical_expr(&self.try_decode_message(expr)?, ctx, schema, self))
            .collect::<Result<Vec<_>>>()?;
        // The constants are set by the equivalence properties, so we do nothing here.
        Ok(EquivalenceClass::new(exprs))
    }

    fn try_encode_equivalence_class(
        &self,
        class: &EquivalenceClass,
    ) -> Result<gen::EquivalenceClass> {
        let exprs = class
            .iter()
            .map(|expr| {
                let expr = serialize_physical_expr(expr, self)?;
                self.try_encode_message(expr)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(gen::EquivalenceClass { exprs })
    }

    fn try_decode_constant_expression(
        &self,
        const_expr: &gen::ConstantExpr,
        schema: &Schema,
        ctx: &TaskContext,
    ) -> Result<ConstExpr> {
        let gen::ConstantExpr {
            expr,
            across_partitions,
        } = const_expr;
        let expr = parse_physical_expr(&self.try_decode_message(expr)?, ctx, schema, self)?;
        let across_partitions = match across_partitions {
            Some(x) => self.try_decode_constant_across_partitions(x)?,
            None => return plan_err!("missing constant expression across partitions"),
        };
        Ok(ConstExpr::new(expr, across_partitions))
    }

    fn try_encode_constant_expression(&self, const_expr: &ConstExpr) -> Result<gen::ConstantExpr> {
        let expr = serialize_physical_expr(&const_expr.expr, self)?;
        let expr = self.try_encode_message(expr)?;
        let across_partitions =
            self.try_encode_constant_across_partitions(&const_expr.across_partitions)?;
        Ok(gen::ConstantExpr {
            expr,
            across_partitions: Some(across_partitions),
        })
    }

    fn try_decode_constant_across_partitions(
        &self,
        constant: &gen::ConstantAcrossPartitions,
    ) -> Result<AcrossPartitions> {
        let gen::ConstantAcrossPartitions { kind: Some(kind) } = constant else {
            return plan_err!("missing constant across partitions kind");
        };
        match kind {
            gen::constant_across_partitions::Kind::Uniform(gen::UniformConstant { value }) => {
                let value = value
                    .as_ref()
                    .map(|x| self.try_decode_scalar_value(x))
                    .transpose()?;
                Ok(AcrossPartitions::Uniform(value))
            }
            gen::constant_across_partitions::Kind::Heterogeneous(gen::HeterogeneousConstant {}) => {
                Ok(AcrossPartitions::Heterogeneous)
            }
        }
    }

    fn try_encode_constant_across_partitions(
        &self,
        constant: &AcrossPartitions,
    ) -> Result<gen::ConstantAcrossPartitions> {
        let kind = match constant {
            AcrossPartitions::Uniform(value) => {
                let value = value
                    .as_ref()
                    .map(|x| self.try_encode_scalar_value(x))
                    .transpose()?;
                gen::constant_across_partitions::Kind::Uniform(gen::UniformConstant { value })
            }
            AcrossPartitions::Heterogeneous => {
                gen::constant_across_partitions::Kind::Heterogeneous(gen::HeterogeneousConstant {})
            }
        };
        Ok(gen::ConstantAcrossPartitions { kind: Some(kind) })
    }

    fn try_decode_equivalence_group(
        &self,
        eq_group: &gen::EquivalenceGroup,
        schema: &Schema,
        ctx: &TaskContext,
    ) -> Result<EquivalenceGroup> {
        let gen::EquivalenceGroup { classes } = eq_group;
        let classes = classes
            .iter()
            .map(|class| self.try_decode_equivalence_class(class.clone(), schema, ctx))
            .collect::<Result<Vec<_>>>()?;
        Ok(EquivalenceGroup::new(classes))
    }

    fn try_encode_equivalence_group(
        &self,
        eq_group: &EquivalenceGroup,
    ) -> Result<gen::EquivalenceGroup> {
        let classes = eq_group
            .iter()
            .map(|class| self.try_encode_equivalence_class(class))
            .collect::<Result<Vec<_>>>()?;
        Ok(gen::EquivalenceGroup { classes })
    }

    fn try_decode_equivalence_properties(
        &self,
        eq_properties: &gen::EquivalenceProperties,
        ctx: &TaskContext,
    ) -> Result<EquivalenceProperties> {
        let gen::EquivalenceProperties {
            eq_group,
            constants,
            orderings,
            constraints,
            schema,
        } = eq_properties;
        let schema = self.try_decode_schema(schema)?;
        let eq_group = match eq_group {
            Some(x) => self.try_decode_equivalence_group(x, &schema, ctx)?,
            None => return plan_err!("missing equivalence group"),
        };
        let constants = constants
            .iter()
            .map(|x| self.try_decode_constant_expression(x, &schema, ctx))
            .collect::<Result<Vec<_>>>()?;
        let orderings = self.try_decode_lex_orderings(orderings, &schema, ctx)?;
        let constraints = constraints
            .iter()
            .map(|x| self.try_decode_constraint(x))
            .collect::<Result<Vec<_>>>()?;
        let mut eq_properties =
            EquivalenceProperties::new_with_orderings(Arc::new(schema), orderings)
                .with_constraints(Constraints::new_unverified(constraints));
        eq_properties.add_equivalence_group(eq_group)?;
        eq_properties.add_constants(constants)?;
        Ok(eq_properties)
    }

    fn try_encode_equivalence_properties(
        &self,
        eq_properties: &EquivalenceProperties,
    ) -> Result<gen::EquivalenceProperties> {
        let schema = self.try_encode_schema(eq_properties.schema().as_ref())?;
        let eq_group = self.try_encode_equivalence_group(eq_properties.eq_group())?;
        let constants = eq_properties
            .constants()
            .iter()
            .map(|x| self.try_encode_constant_expression(x))
            .collect::<Result<Vec<_>>>()?;
        let orderings = self.try_encode_lex_orderings(eq_properties.oeq_class())?;
        let constraints = eq_properties
            .constraints()
            .iter()
            .map(|x| self.try_encode_constraint(x))
            .collect::<Result<Vec<_>>>()?;
        Ok(gen::EquivalenceProperties {
            eq_group: Some(eq_group),
            constants,
            orderings,
            constraints,
            schema,
        })
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
            // Spark 4.0 Arrow-native scalar UDF kinds
            gen::PySparkUdfKind::ScalarArrow => PySparkUdfKind::ScalarArrow,
            gen::PySparkUdfKind::ScalarArrowIter => PySparkUdfKind::ScalarArrowIter,
        };
        Ok(kind)
    }

    fn try_encode_pyspark_udf_kind(&self, kind: PySparkUdfKind) -> Result<i32> {
        let kind = match kind {
            PySparkUdfKind::Batch => gen::PySparkUdfKind::Batch,
            PySparkUdfKind::ArrowBatch => gen::PySparkUdfKind::ArrowBatch,
            PySparkUdfKind::ScalarPandas => gen::PySparkUdfKind::ScalarPandas,
            PySparkUdfKind::ScalarPandasIter => gen::PySparkUdfKind::ScalarPandasIter,
            PySparkUdfKind::ScalarArrow => gen::PySparkUdfKind::ScalarArrow,
            PySparkUdfKind::ScalarArrowIter => gen::PySparkUdfKind::ScalarArrowIter,
        };
        Ok(kind as i32)
    }

    // Decode/encode grouped aggregate UDF kind (Pandas vs Arrow)
    fn try_decode_pyspark_group_agg_kind(&self, kind: i32) -> Result<PySparkGroupAggKind> {
        let kind = gen::PySparkGroupAggKind::try_from(kind)
            .map_err(|e| plan_datafusion_err!("failed to decode pyspark group agg kind: {e}"))?;
        let kind = match kind {
            gen::PySparkGroupAggKind::Pandas => PySparkGroupAggKind::Pandas,
            gen::PySparkGroupAggKind::Arrow => PySparkGroupAggKind::Arrow,
        };
        Ok(kind)
    }

    fn try_encode_pyspark_group_agg_kind(&self, kind: PySparkGroupAggKind) -> Result<i32> {
        let kind = match kind {
            PySparkGroupAggKind::Pandas => gen::PySparkGroupAggKind::Pandas,
            PySparkGroupAggKind::Arrow => gen::PySparkGroupAggKind::Arrow,
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
            python_udf_pandas_conversion_enabled: config.python_udf_pandas_conversion_enabled,
            python_udtf_pandas_conversion_enabled: config.python_udtf_pandas_conversion_enabled,
            python_udf_pandas_int_to_decimal_coercion_enabled: config
                .python_udf_pandas_int_to_decimal_coercion_enabled,
            binary_as_bytes: config.binary_as_bytes,
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
            python_udf_pandas_conversion_enabled: config.python_udf_pandas_conversion_enabled,
            python_udtf_pandas_conversion_enabled: config.python_udtf_pandas_conversion_enabled,
            python_udf_pandas_int_to_decimal_coercion_enabled: config
                .python_udf_pandas_int_to_decimal_coercion_enabled,
            binary_as_bytes: config.binary_as_bytes,
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
        parse_protobuf_partitioning(
            Some(&partitioning),
            &PhysicalPlanDecodeContext::new(ctx, self),
            schema,
            &DefaultPhysicalProtoConverter {},
        )?
        .ok_or_else(|| plan_datafusion_err!("no partitioning found"))
    }

    fn try_encode_partitioning(&self, partitioning: &Partitioning) -> Result<Vec<u8>> {
        let partitioning =
            serialize_partitioning(partitioning, self, &DefaultPhysicalProtoConverter {})?;
        self.try_encode_message(partitioning)
    }

    fn try_decode_data_type(&self, buf: &[u8]) -> Result<DataType> {
        let arrow_type = self.try_decode_message::<gen_datafusion_common::ArrowType>(buf)?;
        Ok((&arrow_type).try_into()?)
    }

    fn try_encode_data_type(&self, data_type: &DataType) -> Result<Vec<u8>> {
        self.try_encode_message::<gen_datafusion_common::ArrowType>(data_type.try_into()?)
    }

    fn try_decode_scalar_value(&self, buf: &[u8]) -> Result<ScalarValue> {
        let value = self.try_decode_message::<gen_datafusion_common::ScalarValue>(buf)?;
        Ok((&value).try_into()?)
    }

    fn try_encode_scalar_value(&self, value: &ScalarValue) -> Result<Vec<u8>> {
        self.try_encode_message::<gen_datafusion_common::ScalarValue>(value.try_into()?)
    }

    fn try_decode_schema(&self, buf: &[u8]) -> Result<Schema> {
        let schema = self.try_decode_message::<gen_datafusion_common::Schema>(buf)?;
        Ok((&schema).try_into()?)
    }

    fn try_encode_schema(&self, schema: &Schema) -> Result<Vec<u8>> {
        self.try_encode_message::<gen_datafusion_common::Schema>(schema.try_into()?)
    }

    fn try_decode_statistics(&self, buf: &[u8]) -> Result<Statistics> {
        let statistics = self.try_decode_message::<gen_datafusion_common::Statistics>(buf)?;
        (&statistics).try_into()
    }

    fn try_encode_statistics(&self, statistics: &Statistics) -> Result<Vec<u8>> {
        self.try_encode_message::<gen_datafusion_common::Statistics>(statistics.into())
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

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip_udf(udf: ScalarUDF) -> Result<Arc<ScalarUDF>> {
        let codec = RemoteExecutionCodec;
        let name = udf.name().to_string();
        let mut buf = vec![];
        codec.try_encode_udf(&udf, &mut buf)?;
        codec.try_decode_udf(&name, &buf)
    }

    #[test]
    fn test_round_trip_spark_variant_explode_helper_udf() -> Result<()> {
        let decoded = round_trip_udf(ScalarUDF::from(SparkVariantExplodeUdf::new()))?;

        assert!(decoded
            .inner()
            .downcast_ref::<SparkVariantExplodeUdf>()
            .is_some());
        assert_eq!(decoded.name(), "spark_variant_explode");

        Ok(())
    }

    /// Builds a wrapped `filter(arr, v -> v > 2)` physical expression over a
    /// single `List<Int32>` column "arr", plus the input schema and a sample
    /// `[[1, 2, 3]]` list array for evaluation. Shared by the expr- and
    /// plan-level distributed roundtrip tests.
    fn build_wrapped_filter() -> Result<(
        Arc<dyn PhysicalExpr>,
        datafusion::arrow::datatypes::SchemaRef,
        datafusion::arrow::array::ListArray,
    )> {
        use std::collections::HashMap;

        use datafusion::arrow::array::{Array, Int32Array, ListArray};
        use datafusion::arrow::buffer::OffsetBuffer;
        use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
        use datafusion::common::DFSchema;
        use datafusion::logical_expr::execution_props::ExecutionProps;
        use datafusion::logical_expr::expr::{HigherOrderFunction, LambdaVariable};
        use datafusion::logical_expr::{col, lambda, lit, Expr, HigherOrderUDF};
        use datafusion::physical_expr::create_physical_expr;
        use sail_function::scalar::array::spark_array_filter::SparkArrayFilter;
        use sail_physical_plan::higher_order::wrap_distributed_higher_order;

        let list_field = Arc::new(Field::new_list_field(DataType::Int32, true));
        let list = ListArray::new(
            list_field,
            OffsetBuffer::<i32>::from_lengths(vec![3]),
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            None,
        );

        let schema = Schema::new(vec![Field::new("arr", list.data_type().clone(), true)]);
        let dfschema = DFSchema::from_unqualified_fields(
            vec![Field::new("arr", list.data_type().clone(), true)].into(),
            HashMap::new(),
        )?;

        let body = Expr::LambdaVariable(LambdaVariable::new(
            "v".to_string(),
            Some(Arc::new(Field::new("v", DataType::Int32, true))),
        ))
        .gt(lit(2i32));

        let func = Arc::new(HigherOrderUDF::new_from_impl(SparkArrayFilter::new()));
        let logical = Expr::HigherOrderFunction(HigherOrderFunction::new(
            func,
            vec![col("arr"), lambda(["v"], body)],
        ));
        let physical = create_physical_expr(&logical, &dfschema, &ExecutionProps::new())?;

        let schema_ref: SchemaRef = Arc::new(schema);
        let wrapped = wrap_distributed_higher_order(physical, &schema_ref)?;
        Ok((wrapped, schema_ref, list))
    }

    #[test]
    fn test_round_trip_distributed_filter_higher_order_expr() -> Result<()> {
        use datafusion::arrow::array::RecordBatch;

        let (wrapped, schema_ref, list) = build_wrapped_filter()?;
        assert!(wrapped
            .downcast_ref::<DistributedHigherOrderExpr>()
            .is_some());

        // Serialize -> encode bytes -> decode bytes (prove the wire path).
        let codec = RemoteExecutionCodec;
        let proto = serialize_physical_expr(&wrapped, &codec)?;
        let bytes = proto.encode_to_vec();
        let proto2 = datafusion_proto::protobuf::PhysicalExprNode::decode(bytes.as_slice())
            .map_err(|e| plan_datafusion_err!("failed to decode PhysicalExprNode: {e}"))?;

        // Parse back into a physical expr; must be a DistributedHigherOrderExpr.
        let ctx = TaskContext::default();
        let decoded = parse_physical_expr(&proto2, &ctx, &schema_ref, &codec)?;
        assert!(decoded
            .downcast_ref::<DistributedHigherOrderExpr>()
            .is_some());

        // Both exprs must evaluate identically.
        let batch = RecordBatch::try_new(Arc::clone(&schema_ref), vec![Arc::new(list)])?;
        let original_result = wrapped.evaluate(&batch)?.into_array(1)?;
        let decoded_result = decoded.evaluate(&batch)?.into_array(1)?;
        assert_eq!(&original_result, &decoded_result);

        Ok(())
    }

    #[test]
    fn test_round_trip_distributed_filter_in_projection_plan() -> Result<()> {
        use datafusion::arrow::array::RecordBatch;
        use datafusion::physical_expr::projection::ProjectionExpr;
        use datafusion::physical_plan::empty::EmptyExec;
        use datafusion::physical_plan::projection::ProjectionExec;

        // End-to-end at the PLAN level: a ProjectionExec carrying the wrapped
        // higher-order function must survive try_encode_plan -> bytes ->
        // try_decode_plan and still evaluate correctly. This exercises the path
        // datafusion-proto uses to serialize a real plan node (not just the
        // bare expression).
        let (wrapped, schema_ref, list) = build_wrapped_filter()?;
        let original_result = Arc::clone(&wrapped)
            .evaluate(&RecordBatch::try_new(
                Arc::clone(&schema_ref),
                vec![Arc::new(list.clone())],
            )?)?
            .into_array(1)?;

        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::clone(&schema_ref)));
        let projection = ProjectionExec::try_new(
            vec![ProjectionExpr {
                expr: wrapped,
                alias: "result".to_string(),
            }],
            input,
        )?;

        let codec = RemoteExecutionCodec;
        let bytes = codec.try_encode_plan(Arc::new(projection))?;
        let ctx = TaskContext::default();
        let decoded = codec.try_decode_plan(&bytes, &ctx)?;

        let decoded_proj = decoded
            .downcast_ref::<ProjectionExec>()
            .ok_or_else(|| plan_datafusion_err!("decoded plan is not a ProjectionExec"))?;
        let decoded_expr = &decoded_proj.expr()[0].expr;
        assert!(decoded_expr
            .downcast_ref::<DistributedHigherOrderExpr>()
            .is_some());

        // The wrapped HOF inside the decoded plan still evaluates to [[3]].
        let batch = RecordBatch::try_new(schema_ref, vec![Arc::new(list)])?;
        let decoded_result = decoded_expr.evaluate(&batch)?.into_array(1)?;
        assert_eq!(&original_result, &decoded_result);

        Ok(())
    }

    /// `filter(arr, v -> v > threshold)` where the lambda captures an OUTER
    /// column (`threshold`). Proves the captured `Column(threshold)` and the
    /// two-column input schema survive encode/decode.
    #[test]
    fn test_round_trip_distributed_filter_with_capture() -> Result<()> {
        use std::collections::HashMap;

        use datafusion::arrow::array::{Array, Int32Array, ListArray, RecordBatch};
        use datafusion::arrow::buffer::OffsetBuffer;
        use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
        use datafusion::common::DFSchema;
        use datafusion::logical_expr::execution_props::ExecutionProps;
        use datafusion::logical_expr::expr::{HigherOrderFunction, LambdaVariable};
        use datafusion::logical_expr::{col, lambda, Expr, HigherOrderUDF};
        use datafusion::physical_expr::create_physical_expr;
        use sail_function::scalar::array::spark_array_filter::SparkArrayFilter;
        use sail_physical_plan::higher_order::wrap_distributed_higher_order;

        let list_field = Arc::new(Field::new_list_field(DataType::Int32, true));
        let list = ListArray::new(
            list_field,
            OffsetBuffer::<i32>::from_lengths(vec![5]),
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            None,
        );
        let threshold = Int32Array::from(vec![2]);

        let fields = vec![
            Field::new("arr", list.data_type().clone(), true),
            Field::new("threshold", DataType::Int32, true),
        ];
        let schema = Schema::new(fields.clone());
        let dfschema = DFSchema::from_unqualified_fields(fields.into(), HashMap::new())?;

        let v_var = Expr::LambdaVariable(LambdaVariable::new(
            "v".to_string(),
            Some(Arc::new(Field::new("v", DataType::Int32, true))),
        ));
        let body = v_var.gt(col("threshold"));
        let func = Arc::new(HigherOrderUDF::new_from_impl(SparkArrayFilter::new()));
        let logical = Expr::HigherOrderFunction(HigherOrderFunction::new(
            func,
            vec![col("arr"), lambda(["v"], body)],
        ));
        let physical = create_physical_expr(&logical, &dfschema, &ExecutionProps::new())?;

        let schema_ref: SchemaRef = Arc::new(schema);
        let wrapped = wrap_distributed_higher_order(physical, &schema_ref)?;
        assert!(wrapped
            .downcast_ref::<DistributedHigherOrderExpr>()
            .is_some());

        let codec = RemoteExecutionCodec;
        let proto = serialize_physical_expr(&wrapped, &codec)?;
        let bytes = proto.encode_to_vec();
        let proto2 = datafusion_proto::protobuf::PhysicalExprNode::decode(bytes.as_slice())
            .map_err(|e| plan_datafusion_err!("failed to decode PhysicalExprNode: {e}"))?;

        let ctx = TaskContext::default();
        let decoded = parse_physical_expr(&proto2, &ctx, &schema_ref, &codec)?;
        assert!(decoded
            .downcast_ref::<DistributedHigherOrderExpr>()
            .is_some());

        let batch = RecordBatch::try_new(
            Arc::clone(&schema_ref),
            vec![Arc::new(list), Arc::new(threshold)],
        )?;
        let original_result = wrapped.evaluate(&batch)?.into_array(1)?;
        let decoded_result = decoded.evaluate(&batch)?.into_array(1)?;
        assert_eq!(&original_result, &decoded_result);

        Ok(())
    }

    /// Index-first `filter(arr, i -> i = 0)` built directly from
    /// `SparkArrayFilter::new_index_first()`, mirroring the planner rewrite.
    /// Proves the `index_first` flag survives encode/decode (otherwise the
    /// decoded UDF would rebuild the non-index-first variant).
    #[test]
    fn test_round_trip_distributed_filter_index_first() -> Result<()> {
        use std::collections::HashMap;

        use datafusion::arrow::array::{Array, Int32Array, ListArray, RecordBatch};
        use datafusion::arrow::buffer::OffsetBuffer;
        use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
        use datafusion::common::DFSchema;
        use datafusion::logical_expr::execution_props::ExecutionProps;
        use datafusion::logical_expr::expr::{HigherOrderFunction, LambdaVariable};
        use datafusion::logical_expr::{col, lambda, lit, Expr, HigherOrderUDF};
        use datafusion::physical_expr::{create_physical_expr, HigherOrderFunctionExpr};
        use sail_function::scalar::array::spark_array_filter::SparkArrayFilter;
        use sail_physical_plan::higher_order::wrap_distributed_higher_order;

        let list_field = Arc::new(Field::new_list_field(DataType::Int32, true));
        let list = ListArray::new(
            list_field,
            OffsetBuffer::<i32>::from_lengths(vec![3]),
            Arc::new(Int32Array::from(vec![10, 20, 30])),
            None,
        );

        let fields = vec![Field::new("arr", list.data_type().clone(), true)];
        let schema = Schema::new(fields.clone());
        let dfschema = DFSchema::from_unqualified_fields(fields.into(), HashMap::new())?;

        let i_var = Expr::LambdaVariable(LambdaVariable::new(
            "i".to_string(),
            Some(Arc::new(Field::new("i", DataType::Int32, false))),
        ));
        // (i) -> i = 0, the rewritten index-first predicate.
        let body = i_var.eq(lit(0i32));
        let func = Arc::new(HigherOrderUDF::new_from_impl(
            SparkArrayFilter::new_index_first(),
        ));
        let logical = Expr::HigherOrderFunction(HigherOrderFunction::new(
            func,
            vec![col("arr"), lambda(["i"], body)],
        ));
        let physical = create_physical_expr(&logical, &dfschema, &ExecutionProps::new())?;

        let schema_ref: SchemaRef = Arc::new(schema);
        let wrapped = wrap_distributed_higher_order(physical, &schema_ref)?;
        assert!(wrapped
            .downcast_ref::<DistributedHigherOrderExpr>()
            .is_some());

        let codec = RemoteExecutionCodec;
        let proto = serialize_physical_expr(&wrapped, &codec)?;
        let bytes = proto.encode_to_vec();
        let proto2 = datafusion_proto::protobuf::PhysicalExprNode::decode(bytes.as_slice())
            .map_err(|e| plan_datafusion_err!("failed to decode PhysicalExprNode: {e}"))?;

        let ctx = TaskContext::default();
        let decoded = parse_physical_expr(&proto2, &ctx, &schema_ref, &codec)?;
        let decoded_hof = decoded
            .downcast_ref::<DistributedHigherOrderExpr>()
            .ok_or_else(|| plan_datafusion_err!("decoded is not a DistributedHigherOrderExpr"))?;

        // Strongest check: the decoded inner UDF is still index-first.
        let inner_hof = decoded_hof
            .inner()
            .downcast_ref::<HigherOrderFunctionExpr>()
            .ok_or_else(|| plan_datafusion_err!("inner is not a HigherOrderFunctionExpr"))?;
        let udf_any = inner_hof.fun().inner().as_ref() as &dyn std::any::Any;
        let filter = udf_any
            .downcast_ref::<SparkArrayFilter>()
            .ok_or_else(|| plan_datafusion_err!("inner UDF is not a SparkArrayFilter"))?;
        assert!(filter.is_index_first());

        let batch = RecordBatch::try_new(Arc::clone(&schema_ref), vec![Arc::new(list)])?;
        let original_result = wrapped.evaluate(&batch)?.into_array(1)?;
        let decoded_result = decoded.evaluate(&batch)?.into_array(1)?;
        assert_eq!(&original_result, &decoded_result);

        Ok(())
    }

    /// Two nested `filter` HOFs: the inner filter sits in the OUTER filter's
    /// ARRAY-ARGUMENT position, i.e. `filter(filter(arr, x -> x > 1), v -> v >
    /// 2)` over a `List<Int32>` column. Because the inner filter is a non-lambda
    /// argument of the outer filter, this exercises the BASE-schema recursion
    /// branch in `wrap_distributed_higher_order` (the `else` arm that recurses
    /// with the same schema), NOT the extended-schema branch. The
    /// extended-schema (lambda-body) branch is covered by
    /// `test_round_trip_distributed_filter_nested_in_lambda_body`. The decoded
    /// tree must contain TWO nested `DistributedHigherOrderExpr` wrappers and
    /// evaluate identically.
    ///
    /// Note: the original "nested inside a lambda body via `cardinality(...)`"
    /// formulation could not be used because `cardinality` (a
    /// `datafusion-functions-nested` scalar UDF) is not registered in the Sail
    /// codec's scalar-function deserialization table, so it fails to round-trip
    /// with `ExtendedScalarUdf: no UDF found for cardinality`. That is a
    /// pre-existing scalar-UDF registration gap, unrelated to the higher-order
    /// roundtrip under test, so we nest two `filter`s directly instead.
    #[test]
    fn test_round_trip_distributed_filter_nested() -> Result<()> {
        use std::collections::HashMap;

        use datafusion::arrow::array::{Array, Int32Array, ListArray, RecordBatch};
        use datafusion::arrow::buffer::OffsetBuffer;
        use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
        use datafusion::common::tree_node::TreeNode;
        use datafusion::common::DFSchema;
        use datafusion::logical_expr::execution_props::ExecutionProps;
        use datafusion::logical_expr::expr::{HigherOrderFunction, LambdaVariable};
        use datafusion::logical_expr::{col, lambda, lit, Expr, HigherOrderUDF};
        use datafusion::physical_expr::create_physical_expr;
        use sail_function::scalar::array::spark_array_filter::SparkArrayFilter;
        use sail_physical_plan::higher_order::wrap_distributed_higher_order;

        // arr = [[1, 2, 3, 4, 5]] : one row, a single int array.
        let list_field = Arc::new(Field::new_list_field(DataType::Int32, true));
        let list = ListArray::new(
            list_field,
            OffsetBuffer::<i32>::from_lengths(vec![5]),
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            None,
        );

        let fields = vec![Field::new("arr", list.data_type().clone(), true)];
        let schema = Schema::new(fields.clone());
        let dfschema = DFSchema::from_unqualified_fields(fields.into(), HashMap::new())?;

        let x_var = Expr::LambdaVariable(LambdaVariable::new(
            "x".to_string(),
            Some(Arc::new(Field::new("x", DataType::Int32, true))),
        ));
        let v_var = Expr::LambdaVariable(LambdaVariable::new(
            "v".to_string(),
            Some(Arc::new(Field::new("v", DataType::Int32, true))),
        ));

        // Inner filter sits in the OUTER filter's array-argument position.
        let inner_filter = Expr::HigherOrderFunction(HigherOrderFunction::new(
            Arc::new(HigherOrderUDF::new_from_impl(SparkArrayFilter::new())),
            vec![col("arr"), lambda(["x"], x_var.gt(lit(1i32)))],
        ));
        let logical = Expr::HigherOrderFunction(HigherOrderFunction::new(
            Arc::new(HigherOrderUDF::new_from_impl(SparkArrayFilter::new())),
            vec![inner_filter, lambda(["v"], v_var.gt(lit(2i32)))],
        ));
        let physical = create_physical_expr(&logical, &dfschema, &ExecutionProps::new())?;

        let schema_ref: SchemaRef = Arc::new(schema);
        let wrapped = wrap_distributed_higher_order(physical, &schema_ref)?;
        assert!(wrapped
            .downcast_ref::<DistributedHigherOrderExpr>()
            .is_some());

        let codec = RemoteExecutionCodec;
        let proto = serialize_physical_expr(&wrapped, &codec)?;
        let bytes = proto.encode_to_vec();
        let proto2 = datafusion_proto::protobuf::PhysicalExprNode::decode(bytes.as_slice())
            .map_err(|e| plan_datafusion_err!("failed to decode PhysicalExprNode: {e}"))?;

        let ctx = TaskContext::default();
        let decoded = parse_physical_expr(&proto2, &ctx, &schema_ref, &codec)?;
        assert!(decoded
            .downcast_ref::<DistributedHigherOrderExpr>()
            .is_some());

        // The decoded tree must contain a NESTED wrapper inside the lambda body.
        let nested_count = std::cell::Cell::new(0usize);
        Arc::clone(&decoded).apply(|node| {
            if node.is::<DistributedHigherOrderExpr>() {
                nested_count.set(nested_count.get() + 1);
            }
            Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
        })?;
        assert!(
            nested_count.get() >= 2,
            "expected at least 2 DistributedHigherOrderExpr nodes, found {}",
            nested_count.get()
        );

        let batch = RecordBatch::try_new(Arc::clone(&schema_ref), vec![Arc::new(list)])?;
        let original_result = wrapped.evaluate(&batch)?.into_array(1)?;
        let decoded_result = decoded.evaluate(&batch)?.into_array(1)?;
        assert_eq!(&original_result, &decoded_result);

        Ok(())
    }

    /// A `filter` HOF nested INSIDE the outer lambda's BODY:
    /// `filter(arr2d, a -> filter(a, y -> y > 1) IS NOT NULL)` over a single
    /// `List<List<Int32>>` column. Because the inner filter appears in the
    /// outer lambda's body (not in an array-argument position), this exercises
    /// the EXTENDED-schema branch of `wrap_distributed_higher_order`: the inner
    /// wrapper must serialize `input_schema = base_schema + the outer lambda's
    /// `a` parameter field`, and its lambda variables carry indices beyond the
    /// base schema width. The base-schema branch is covered by
    /// `test_round_trip_distributed_filter_nested`.
    ///
    /// The outer lambda body uses only natively-serialized exprs (`IS NOT NULL`
    /// over the inner filter's list result) so no unregistered scalar UDF is
    /// needed.
    #[test]
    fn test_round_trip_distributed_filter_nested_in_lambda_body() -> Result<()> {
        use std::collections::HashMap;

        use datafusion::arrow::array::{Array, Int32Array, ListArray, RecordBatch};
        use datafusion::arrow::buffer::OffsetBuffer;
        use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
        use datafusion::common::tree_node::TreeNode;
        use datafusion::common::DFSchema;
        use datafusion::logical_expr::execution_props::ExecutionProps;
        use datafusion::logical_expr::expr::{HigherOrderFunction, LambdaVariable};
        use datafusion::logical_expr::{col, lambda, lit, Expr, HigherOrderUDF};
        use datafusion::physical_expr::create_physical_expr;
        use sail_function::scalar::array::spark_array_filter::SparkArrayFilter;
        use sail_physical_plan::higher_order::wrap_distributed_higher_order;

        // arr2d = [ [[1, 2], [3]] ] : one outer row holding two inner int
        // arrays, [1, 2] and [3].
        let inner_int_field = Arc::new(Field::new_list_field(DataType::Int32, true));
        let inner_lists = ListArray::new(
            Arc::clone(&inner_int_field),
            OffsetBuffer::<i32>::from_lengths(vec![2, 1]),
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            None,
        );
        let inner_list_dt = inner_lists.data_type().clone();
        let outer_field = Arc::new(Field::new_list_field(inner_list_dt.clone(), true));
        let arr2d = ListArray::new(
            outer_field,
            OffsetBuffer::<i32>::from_lengths(vec![2]),
            Arc::new(inner_lists),
            None,
        );

        let fields = vec![Field::new("arr2d", arr2d.data_type().clone(), true)];
        let schema = Schema::new(fields.clone());
        let dfschema = DFSchema::from_unqualified_fields(fields.into(), HashMap::new())?;

        let filter_udf = || Arc::new(HigherOrderUDF::new_from_impl(SparkArrayFilter::new()));

        // Outer lambda element param `a` is one inner array: List<Int32>.
        let a_var = Expr::LambdaVariable(LambdaVariable::new(
            "a".to_string(),
            Some(Arc::new(Field::new("a", inner_list_dt, true))),
        ));
        // Inner lambda variable `y` ranges over the Int32 elements of `a`.
        let y_var = Expr::LambdaVariable(LambdaVariable::new(
            "y".to_string(),
            Some(Arc::new(Field::new("y", DataType::Int32, true))),
        ));
        // Inner HOF lives inside the outer lambda body.
        let inner = Expr::HigherOrderFunction(HigherOrderFunction::new(
            filter_udf(),
            vec![a_var, lambda(["y"], y_var.gt(lit(1i32)))],
        ));
        let outer = Expr::HigherOrderFunction(HigherOrderFunction::new(
            filter_udf(),
            vec![col("arr2d"), lambda(["a"], inner.is_not_null())],
        ));
        let physical = create_physical_expr(&outer, &dfschema, &ExecutionProps::new())?;

        let schema_ref: SchemaRef = Arc::new(schema);
        let wrapped = wrap_distributed_higher_order(physical, &schema_ref)?;
        assert!(wrapped
            .downcast_ref::<DistributedHigherOrderExpr>()
            .is_some());

        let codec = RemoteExecutionCodec;
        let proto = serialize_physical_expr(&wrapped, &codec)?;
        let bytes = proto.encode_to_vec();
        let proto2 = datafusion_proto::protobuf::PhysicalExprNode::decode(bytes.as_slice())
            .map_err(|e| plan_datafusion_err!("failed to decode PhysicalExprNode: {e}"))?;

        let ctx = TaskContext::default();
        let decoded = parse_physical_expr(&proto2, &ctx, &schema_ref, &codec)?;
        assert!(decoded
            .downcast_ref::<DistributedHigherOrderExpr>()
            .is_some());

        // Extended-schema proof: collect every wrapper's carried input-schema
        // width. The inner wrapper carries base + the `a` param, so its field
        // count must exceed the outer (base-schema) wrapper's.
        let widths = std::cell::RefCell::new(Vec::<usize>::new());
        Arc::clone(&decoded).apply(|node| {
            if let Some(hof) = node.downcast_ref::<DistributedHigherOrderExpr>() {
                widths.borrow_mut().push(hof.input_schema().fields().len());
            }
            Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
        })?;
        let widths = widths.into_inner();
        assert!(
            widths.len() >= 2,
            "expected at least 2 DistributedHigherOrderExpr nodes, found {}",
            widths.len()
        );
        let min = widths.iter().min().copied().unwrap_or_default();
        let max = widths.iter().max().copied().unwrap_or_default();
        assert!(
            max > min,
            "expected an inner wrapper with an EXTENDED schema (wider than the \
             base schema); wrapper schema widths were {widths:?}"
        );

        let batch = RecordBatch::try_new(Arc::clone(&schema_ref), vec![Arc::new(arr2d)])?;
        let original_result = wrapped.evaluate(&batch)?.into_array(1)?;
        let decoded_result = decoded.evaluate(&batch)?.into_array(1)?;
        assert_eq!(&original_result, &decoded_result);

        Ok(())
    }
}
