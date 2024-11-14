use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::common::{not_impl_err, plan_datafusion_err, plan_err, Result};
use datafusion::execution::FunctionRegistry;
use datafusion::functions::string::overlay::OverlayFunc;
use datafusion::logical_expr::{AggregateUDF, AggregateUDFImpl, ScalarUDF, Volatility};
use datafusion::physical_expr::expressions::UnKnownColumn;
use datafusion::physical_expr::{LexOrdering, PhysicalExpr};
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::values::ValuesExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion::prelude::SessionContext;
use datafusion_proto::generated::datafusion_common as gen_datafusion_common;
use datafusion_proto::physical_plan::from_proto::{
    parse_physical_sort_exprs, parse_protobuf_partitioning,
};
use datafusion_proto::physical_plan::to_proto::{
    serialize_partitioning, serialize_physical_sort_exprs,
};
use datafusion_proto::physical_plan::{AsExecutionPlan, PhysicalExtensionCodec};
use datafusion_proto::protobuf::{PhysicalPlanNode, PhysicalSortExprNode};
use prost::bytes::BytesMut;
use prost::Message;
use sail_common::spec::PySparkUdfType;
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
use sail_plan::extension::function::spark_array::SparkArray;
use sail_plan::extension::function::spark_concat::SparkConcat;
use sail_plan::extension::function::spark_hex_unhex::{SparkHex, SparkUnHex};
use sail_plan::extension::function::spark_murmur3_hash::SparkMurmur3Hash;
use sail_plan::extension::function::spark_reverse::SparkReverse;
use sail_plan::extension::function::spark_unix_timestamp::SparkUnixTimestamp;
use sail_plan::extension::function::spark_xxhash64::SparkXxhash64;
use sail_plan::extension::function::struct_function::StructFunction;
use sail_plan::extension::function::unix_timestamp_now::UnixTimestampNow;
use sail_plan::extension::function::update_struct_field::UpdateStructField;
use sail_plan::extension::logical::{Range, ShowStringFormat, ShowStringStyle};
use sail_plan::extension::physical::{RangeExec, SchemaPivotExec, ShowStringExec};
use sail_python_udf::udf::pyspark_udaf::PySparkAggregateUDF;
use sail_python_udf::udf::pyspark_udf::PySparkUDF;

use crate::plan::gen::extended_aggregate_udf::UdafKind;
use crate::plan::gen::extended_physical_plan_node::NodeKind;
use crate::plan::gen::extended_scalar_udf::UdfKind;
use crate::plan::gen::{ExtendedAggregateUdf, ExtendedPhysicalPlanNode, ExtendedScalarUdf};
use crate::plan::{gen, ShuffleReadExec, ShuffleWriteExec};
use crate::stream::{TaskReadLocation, TaskWriteLocation};

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
            NodeKind::SchemaPivot(gen::SchemaPivotExecNode {
                input,
                names,
                schema,
            }) => {
                let schema = self.try_decode_message::<gen_datafusion_common::Schema>(&schema)?;
                Ok(Arc::new(SchemaPivotExec::new(
                    self.try_decode_plan(&input, registry)?,
                    names,
                    Arc::new((&schema).try_into()?),
                )))
            }
            NodeKind::ShuffleRead(gen::ShuffleReadExecNode {
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
                    .map(|x| self.try_decode_task_read_location_list(x))
                    .collect::<Result<_>>()?;
                let node = ShuffleReadExec::new(stage as usize, schema, partitioning);
                let node = node.with_locations(locations);
                Ok(Arc::new(node))
            }
            NodeKind::ShuffleWrite(gen::ShuffleWriteExecNode {
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
                    .map(|x| self.try_decode_task_write_location_list(x))
                    .collect::<Result<_>>()?;
                let node = ShuffleWriteExec::new(stage as usize, plan, partitioning);
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
                let schema = self.try_decode_message::<gen_datafusion_common::Schema>(&schema)?;
                let schema = Arc::new((&schema).try_into()?);
                let partitions = partitions
                    .into_iter()
                    .map(|x| read_record_batches(&x))
                    .collect::<Result<Vec<_>>>()?;
                let projection =
                    projection.map(|x| x.columns.into_iter().map(|c| c as usize).collect());
                let mut lex_ordering: Vec<LexOrdering> = vec![];
                for sort in &sort_information {
                    let sort_values: Vec<PhysicalSortExprNode> = sort
                        .values
                        .iter()
                        .map(|x| self.try_decode_message(x))
                        .collect::<Result<_>>()?;
                    let sort_expr =
                        parse_physical_sort_exprs(&sort_values, registry, &schema, self)?;
                    lex_ordering.push(sort_expr);
                }
                Ok(Arc::new(
                    MemoryExec::try_new(&partitions, schema, projection)?
                        .with_show_sizes(show_sizes)
                        .try_with_sort_information(lex_ordering)?,
                ))
            }
            NodeKind::Values(gen::ValuesExecNode { data, schema }) => {
                let schema = self.try_decode_message::<gen_datafusion_common::Schema>(&schema)?;
                let schema = Arc::new((&schema).try_into()?);
                let data = read_record_batches(&data)?;
                Ok(Arc::new(ValuesExec::try_new_from_batches(schema, data)?))
            }
            NodeKind::NdJson(gen::NdJsonExecNode {
                base_config: _,
                file_compression_type: _,
            }) => {
                Err(plan_datafusion_err!("NdJsonExec is not supported"))
                // let base_config: FileScanConfig = ;
                // let file_compression_type: FileCompressionType = ;
                // Ok(Arc::new(NdJsonExec::new(
                //     base_config,
                //     file_compression_type,
                // )?))
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
        } else if let Some(schema_pivot) = node.as_any().downcast_ref::<SchemaPivotExec>() {
            let schema = self.try_encode_message::<gen_datafusion_common::Schema>(
                schema_pivot.schema().as_ref().try_into()?,
            )?;
            NodeKind::SchemaPivot(gen::SchemaPivotExecNode {
                input: self.try_encode_plan(schema_pivot.input().clone())?,
                names: schema_pivot.names().to_vec(),
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
                .map(|x| self.try_encode_task_read_location_list(x))
                .collect::<Result<_>>()?;
            NodeKind::ShuffleRead(gen::ShuffleReadExecNode {
                stage: shuffle_read.stage() as u64,
                schema,
                partitioning,
                locations,
            })
        } else if let Some(shuffle_write) = node.as_any().downcast_ref::<ShuffleWriteExec>() {
            let partitioning =
                self.try_encode_partitioning(shuffle_write.shuffle_partitioning())?;
            let locations = shuffle_write
                .locations()
                .iter()
                .map(|x| self.try_encode_task_write_location_list(x))
                .collect::<Result<_>>()?;
            NodeKind::ShuffleWrite(gen::ShuffleWriteExecNode {
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
            let mut sort_information = vec![];
            for sort in memory.sort_information() {
                let sort = serialize_physical_sort_exprs(sort.to_vec(), self)?;
                let sort = sort
                    .into_iter()
                    .map(|x| self.try_encode_message(x))
                    .collect::<Result<_>>()?;
                let sort = gen::SortInformation { values: sort };
                sort_information.push(sort)
            }
            NodeKind::Memory(gen::MemoryExecNode {
                partitions,
                schema,
                projection,
                show_sizes: memory.show_sizes(),
                sort_information,
            })
        } else if let Some(values) = node.as_any().downcast_ref::<ValuesExec>() {
            let data = write_record_batches(&values.data(), &values.schema())?;
            let schema = self.try_encode_message::<gen_datafusion_common::Schema>(
                values.schema().as_ref().try_into()?,
            )?;
            NodeKind::Values(gen::ValuesExecNode { data, schema })
        } else if let Some(values) = node.as_any().downcast_ref::<ValuesExec>() {
            let data = write_record_batches(&values.data(), &values.schema())?;
            let schema = self.try_encode_message::<gen_datafusion_common::Schema>(
                values.schema().as_ref().try_into()?,
            )?;
            NodeKind::Values(gen::ValuesExecNode { data, schema })
        } else {
            return plan_err!("unsupported physical plan node: {node:?}");
        };
        let node = ExtendedPhysicalPlanNode {
            node_kind: Some(node_kind),
        };
        node.encode(buf)
            .map_err(|e| plan_datafusion_err!("failed to encode plan: {e:?}"))
    }

    fn try_decode_udf(&self, name: &str, buf: &[u8]) -> Result<Arc<ScalarUDF>> {
        let udf = ExtendedScalarUdf::decode(buf)
            .map_err(|e| plan_datafusion_err!("failed to decode udf: {e:?}"))?;
        let ExtendedScalarUdf { udf_kind } = udf;
        let udf_kind = match udf_kind {
            Some(x) => x,
            None => return plan_err!("ExtendedScalarUdf: no UDF found for {name}"),
        };
        let auxiliary_field = match udf_kind {
            UdfKind::WithOneAuxiliaryField(gen::WithOneAuxiliaryFieldUdf { auxiliary_field }) => {
                let auxiliary_field = match auxiliary_field {
                    Some(auxiliary_field) => auxiliary_field,
                    None => return plan_err!("no auxiliary field found for {name}"),
                };
                match auxiliary_field.auxiliary_field {
                    Some(auxiliary_field) => Some(auxiliary_field),
                    None => return plan_err!("no auxiliary field found for {name}"),
                }
            }
            UdfKind::Standard(gen::StandardUdf {}) => None,
            UdfKind::PySpark(gen::PySparkUdf {
                function_name,
                deterministic,
                input_types,
                eval_type,
                output_type,
                python_bytes,
            }) => {
                let input_types: Vec<DataType> = input_types
                    .iter()
                    .map(|x| {
                        let arrow_type =
                            self.try_decode_message::<gen_datafusion_common::ArrowType>(x)?;
                        let data_type: DataType = (&arrow_type).try_into()?;
                        Ok(data_type)
                    })
                    .collect::<Result<_>>()?;
                let eval_type = PySparkUdfType::try_from(eval_type)
                    .map_err(|e| plan_datafusion_err!("failed to decode eval_type: {e:?}"))?;
                let output_type =
                    self.try_decode_message::<gen_datafusion_common::ArrowType>(&output_type)?;
                let output_type: DataType = (&output_type).try_into()?;
                let udf = PySparkUDF::new(
                    function_name.to_owned(),
                    deterministic,
                    eval_type,
                    input_types,
                    output_type,
                    python_bytes,
                    false,
                );
                return Ok(Arc::new(ScalarUDF::from(udf)));
            }
        };
        match name {
            "array_item_with_position" => {
                Ok(Arc::new(ScalarUDF::from(ArrayItemWithPosition::new())))
            }
            "array_empty_to_null" => Ok(Arc::new(ScalarUDF::from(ArrayEmptyToNull::new()))),
            "map_to_array" => {
                let nullable = match auxiliary_field {
                    Some(gen::auxiliary_field::AuxiliaryField::AuxiliaryFieldSingle(single)) => {
                        self.try_decode_message::<bool>(&single.auxiliary_field)?
                    }
                    Some(gen::auxiliary_field::AuxiliaryField::AuxiliaryFieldRepeated(_)) => {
                        return plan_err!(
                            "Expected single auxiliary field, but found repeated for {name}"
                        )
                    }
                    None => return plan_err!("no auxiliary field found for {name}"),
                };
                Ok(Arc::new(ScalarUDF::from(MapToArray::new(nullable))))
            }
            "array_min" => Ok(Arc::new(ScalarUDF::from(ArrayMin::new()))),
            "array_max" => Ok(Arc::new(ScalarUDF::from(ArrayMax::new()))),
            "drop_struct_field" => {
                let field_names = match auxiliary_field {
                    Some(gen::auxiliary_field::AuxiliaryField::AuxiliaryFieldSingle(_)) => {
                        return plan_err!(
                            "Expected repeated auxiliary field, but found single for {name}"
                        )
                    }
                    Some(gen::auxiliary_field::AuxiliaryField::AuxiliaryFieldRepeated(
                        repeated,
                    )) => repeated
                        .auxiliary_field
                        .iter()
                        .map(|x| self.try_decode_message::<String>(x))
                        .collect::<Result<_>>()?,
                    None => return plan_err!("no auxiliary field found for {name}"),
                };
                Ok(Arc::new(ScalarUDF::from(DropStructField::new(field_names))))
            }
            "explode" | "explode_outer" | "posexplode" | "posexplode_outer" => {
                let kind = match auxiliary_field {
                    Some(gen::auxiliary_field::AuxiliaryField::AuxiliaryFieldSingle(single)) => {
                        self.try_decode_message::<String>(&single.auxiliary_field)?
                    }
                    Some(gen::auxiliary_field::AuxiliaryField::AuxiliaryFieldRepeated(_)) => {
                        return plan_err!(
                            "Expected single auxiliary field, but found repeated for {name}"
                        )
                    }
                    None => return plan_err!("no auxiliary field found for {name}"),
                };
                let kind = explode_name_to_kind(&kind)?;
                Ok(Arc::new(ScalarUDF::from(Explode::new(kind))))
            }
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
            "spark_unix_timestamp" | "unix_timestamp" | "unix_timestamp_now" => {
                match auxiliary_field {
                    Some(gen::auxiliary_field::AuxiliaryField::AuxiliaryFieldSingle(single)) => {
                        let timezone =
                            self.try_decode_message::<String>(&single.auxiliary_field)?;
                        let timezone: Arc<str> = timezone.into();
                        Ok(Arc::new(ScalarUDF::from(SparkUnixTimestamp::new(timezone))))
                    }
                    Some(gen::auxiliary_field::AuxiliaryField::AuxiliaryFieldRepeated(_)) => {
                        plan_err!("Expected single auxiliary field, but found repeated for {name}")
                    }
                    None => Ok(Arc::new(ScalarUDF::from(UnixTimestampNow::new()))),
                }
            }
            "spark_xxhash64" | "xxhash64" => Ok(Arc::new(ScalarUDF::from(SparkXxhash64::new()))),
            "struct" => {
                let field_names = match auxiliary_field {
                    Some(gen::auxiliary_field::AuxiliaryField::AuxiliaryFieldSingle(_)) => {
                        return plan_err!(
                            "Expected repeated auxiliary field, but found single for {name}"
                        )
                    }
                    Some(gen::auxiliary_field::AuxiliaryField::AuxiliaryFieldRepeated(
                        repeated,
                    )) => repeated
                        .auxiliary_field
                        .iter()
                        .map(|x| self.try_decode_message::<String>(x))
                        .collect::<Result<_>>()?,
                    None => return plan_err!("no auxiliary field found for {name}"),
                };
                Ok(Arc::new(ScalarUDF::from(StructFunction::new(field_names))))
            }
            "update_struct_field" => {
                let field_names = match auxiliary_field {
                    Some(gen::auxiliary_field::AuxiliaryField::AuxiliaryFieldSingle(_)) => {
                        return plan_err!(
                            "Expected repeated auxiliary field, but found single for {name}"
                        )
                    }
                    Some(gen::auxiliary_field::AuxiliaryField::AuxiliaryFieldRepeated(
                        repeated,
                    )) => repeated
                        .auxiliary_field
                        .iter()
                        .map(|x| self.try_decode_message::<String>(x))
                        .collect::<Result<_>>()?,
                    None => return plan_err!("no auxiliary field found for {name}"),
                };
                Ok(Arc::new(ScalarUDF::from(UpdateStructField::new(
                    field_names,
                ))))
            }
            "overlay" => Ok(Arc::new(ScalarUDF::from(OverlayFunc::new()))),
            "json_length" | "json_len" => Ok(datafusion_functions_json::udfs::json_length_udf()),
            "json_as_text" => Ok(datafusion_functions_json::udfs::json_as_text_udf()),
            _ => plan_err!("Could not find Scalar Function: {name}"),
        }
    }

    fn try_encode_udf(&self, node: &ScalarUDF, buf: &mut Vec<u8>) -> Result<()> {
        let udf_kind = if let Some(_func) = node
            .inner()
            .as_any()
            .downcast_ref::<ArrayItemWithPosition>()
        {
            UdfKind::Standard(gen::StandardUdf {})
        } else if let Some(_func) = node.inner().as_any().downcast_ref::<ArrayEmptyToNull>() {
            UdfKind::Standard(gen::StandardUdf {})
        } else if let Some(func) = node.inner().as_any().downcast_ref::<MapToArray>() {
            let nullable = self.try_encode_message::<bool>(func.nullable())?;
            UdfKind::WithOneAuxiliaryField(gen::WithOneAuxiliaryFieldUdf {
                auxiliary_field: Some(gen::AuxiliaryField {
                    auxiliary_field: Some(
                        gen::auxiliary_field::AuxiliaryField::AuxiliaryFieldSingle(
                            gen::AuxiliaryFieldSingle {
                                auxiliary_field: nullable,
                            },
                        ),
                    ),
                }),
            })
        } else if let Some(_func) = node.inner().as_any().downcast_ref::<ArrayMin>() {
            UdfKind::Standard(gen::StandardUdf {})
        } else if let Some(_func) = node.inner().as_any().downcast_ref::<ArrayMax>() {
            UdfKind::Standard(gen::StandardUdf {})
        } else if let Some(func) = node.inner().as_any().downcast_ref::<DropStructField>() {
            let field_names = func
                .field_names()
                .iter()
                .map(|x| self.try_encode_message::<String>(x.to_string()))
                .collect::<Result<_>>()?;
            UdfKind::WithOneAuxiliaryField(gen::WithOneAuxiliaryFieldUdf {
                auxiliary_field: Some(gen::AuxiliaryField {
                    auxiliary_field: Some(
                        gen::auxiliary_field::AuxiliaryField::AuxiliaryFieldRepeated(
                            gen::AuxiliaryFieldRepeated {
                                auxiliary_field: field_names,
                            },
                        ),
                    ),
                }),
            })
        } else if let Some(_func) = node.inner().as_any().downcast_ref::<Explode>() {
            let name = self.try_encode_message::<String>(node.name().to_string())?;
            UdfKind::WithOneAuxiliaryField(gen::WithOneAuxiliaryFieldUdf {
                auxiliary_field: Some(gen::AuxiliaryField {
                    auxiliary_field: Some(
                        gen::auxiliary_field::AuxiliaryField::AuxiliaryFieldSingle(
                            gen::AuxiliaryFieldSingle {
                                auxiliary_field: name,
                            },
                        ),
                    ),
                }),
            })
        } else if let Some(_func) = node.inner().as_any().downcast_ref::<Greatest>() {
            UdfKind::Standard(gen::StandardUdf {})
        } else if let Some(_func) = node.inner().as_any().downcast_ref::<Least>() {
            UdfKind::Standard(gen::StandardUdf {})
        } else if let Some(_func) = node.inner().as_any().downcast_ref::<Levenshtein>() {
            UdfKind::Standard(gen::StandardUdf {})
        } else if let Some(_func) = node.inner().as_any().downcast_ref::<MapFunction>() {
            UdfKind::Standard(gen::StandardUdf {})
        } else if let Some(_func) = node.inner().as_any().downcast_ref::<MultiExpr>() {
            UdfKind::Standard(gen::StandardUdf {})
        } else if let Some(_func) = node.inner().as_any().downcast_ref::<RaiseError>() {
            UdfKind::Standard(gen::StandardUdf {})
        } else if let Some(_func) = node.inner().as_any().downcast_ref::<Randn>() {
            UdfKind::Standard(gen::StandardUdf {})
        } else if let Some(_func) = node.inner().as_any().downcast_ref::<Random>() {
            UdfKind::Standard(gen::StandardUdf {})
        } else if let Some(_func) = node.inner().as_any().downcast_ref::<Size>() {
            UdfKind::Standard(gen::StandardUdf {})
        } else if let Some(_func) = node.inner().as_any().downcast_ref::<SparkArray>() {
            UdfKind::Standard(gen::StandardUdf {})
        } else if let Some(_func) = node.inner().as_any().downcast_ref::<SparkConcat>() {
            UdfKind::Standard(gen::StandardUdf {})
        } else if let Some(_func) = node.inner().as_any().downcast_ref::<SparkHex>() {
            UdfKind::Standard(gen::StandardUdf {})
        } else if let Some(_func) = node.inner().as_any().downcast_ref::<SparkUnHex>() {
            UdfKind::Standard(gen::StandardUdf {})
        } else if let Some(_func) = node.inner().as_any().downcast_ref::<SparkMurmur3Hash>() {
            UdfKind::Standard(gen::StandardUdf {})
        } else if let Some(_func) = node.inner().as_any().downcast_ref::<SparkReverse>() {
            UdfKind::Standard(gen::StandardUdf {})
        } else if let Some(func) = node.inner().as_any().downcast_ref::<SparkUnixTimestamp>() {
            let timezone = self.try_encode_message::<String>(func.timezone().to_string())?;
            UdfKind::WithOneAuxiliaryField(gen::WithOneAuxiliaryFieldUdf {
                auxiliary_field: Some(gen::AuxiliaryField {
                    auxiliary_field: Some(
                        gen::auxiliary_field::AuxiliaryField::AuxiliaryFieldSingle(
                            gen::AuxiliaryFieldSingle {
                                auxiliary_field: timezone,
                            },
                        ),
                    ),
                }),
            })
        } else if let Some(_func) = node.inner().as_any().downcast_ref::<SparkXxhash64>() {
            UdfKind::Standard(gen::StandardUdf {})
        } else if let Some(func) = node.inner().as_any().downcast_ref::<StructFunction>() {
            let field_names = func
                .field_names()
                .iter()
                .map(|x| self.try_encode_message::<String>(x.to_string()))
                .collect::<Result<_>>()?;
            UdfKind::WithOneAuxiliaryField(gen::WithOneAuxiliaryFieldUdf {
                auxiliary_field: Some(gen::AuxiliaryField {
                    auxiliary_field: Some(
                        gen::auxiliary_field::AuxiliaryField::AuxiliaryFieldRepeated(
                            gen::AuxiliaryFieldRepeated {
                                auxiliary_field: field_names,
                            },
                        ),
                    ),
                }),
            })
        } else if let Some(_func) = node.inner().as_any().downcast_ref::<UnixTimestampNow>() {
            UdfKind::Standard(gen::StandardUdf {})
        } else if let Some(func) = node.inner().as_any().downcast_ref::<UpdateStructField>() {
            let field_names = func
                .field_names()
                .iter()
                .map(|x| self.try_encode_message::<String>(x.to_string()))
                .collect::<Result<_>>()?;
            UdfKind::WithOneAuxiliaryField(gen::WithOneAuxiliaryFieldUdf {
                auxiliary_field: Some(gen::AuxiliaryField {
                    auxiliary_field: Some(
                        gen::auxiliary_field::AuxiliaryField::AuxiliaryFieldRepeated(
                            gen::AuxiliaryFieldRepeated {
                                auxiliary_field: field_names,
                            },
                        ),
                    ),
                }),
            })
        } else if let Some(func) = node.inner().as_any().downcast_ref::<PySparkUDF>() {
            let input_types = func
                .input_types()
                .iter()
                .map(|x| {
                    let input_type =
                        self.try_encode_message::<gen_datafusion_common::ArrowType>(x.try_into()?)?;
                    Ok(input_type)
                })
                .collect::<Result<_>>()?;
            let output_type = self.try_encode_message::<gen_datafusion_common::ArrowType>(
                func.output_type().try_into()?,
            )?;
            UdfKind::PySpark(gen::PySparkUdf {
                function_name: func.function_name().to_string(),
                deterministic: func.deterministic(),
                eval_type: (*func.eval_type()).into(),
                input_types,
                output_type,
                python_bytes: func.python_bytes().to_vec(),
            })
        } else if let Some(_func) = node.inner().as_any().downcast_ref::<OverlayFunc>() {
            UdfKind::Standard(gen::StandardUdf {})
        } else if node.name() == "json_length"
            || node.name() == "json_len"
            || node.name() == "json_as_text"
        {
            UdfKind::Standard(gen::StandardUdf {})
        } else {
            return Ok(());
        };
        let node = ExtendedScalarUdf {
            udf_kind: Some(udf_kind),
        };
        node.encode(buf)
            .map_err(|e| plan_datafusion_err!("failed to encode udf: {e:?}"))
    }

    fn try_decode_udaf(&self, name: &str, buf: &[u8]) -> Result<Arc<AggregateUDF>> {
        let udaf = ExtendedAggregateUdf::decode(buf)
            .map_err(|e| plan_datafusion_err!("failed to decode udf: {e:?}"))?;
        let ExtendedAggregateUdf { udaf_kind } = udaf;
        match udaf_kind {
            Some(UdafKind::PySparkAgg(gen::PySparkUdaf {
                function_name,
                deterministic,
                input_types,
                output_type,
                python_bytes,
            })) => {
                let input_types: Vec<DataType> = input_types
                    .iter()
                    .map(|x| {
                        let arrow_type =
                            self.try_decode_message::<gen_datafusion_common::ArrowType>(x)?;
                        let data_type: DataType = (&arrow_type).try_into()?;
                        Ok(data_type)
                    })
                    .collect::<Result<_>>()?;
                let output_type =
                    self.try_decode_message::<gen_datafusion_common::ArrowType>(&output_type)?;
                let output_type: DataType = (&output_type).try_into()?;
                let udaf = PySparkAggregateUDF::new(
                    function_name.to_owned(),
                    deterministic,
                    input_types,
                    output_type,
                    python_bytes,
                    false,
                );
                Ok(Arc::new(AggregateUDF::from(udaf)))
            }
            Some(UdafKind::Standard(gen::StandardUdaf {})) => match name {
                "kurtosis" => Ok(Arc::new(AggregateUDF::from(KurtosisFunction::new()))),
                "max_by" => Ok(Arc::new(AggregateUDF::from(MaxByFunction::new()))),
                "min_by" => Ok(Arc::new(AggregateUDF::from(MinByFunction::new()))),
                "mode" => Ok(Arc::new(AggregateUDF::from(ModeFunction::new()))),
                "skewness" => Ok(Arc::new(AggregateUDF::from(SkewnessFunc::new()))),
                _ => plan_err!("Could not find Aggregate Function: {name}"),
            },
            None => plan_err!("ExtendedScalarUdf: no UDF found for {name}"),
        }
    }

    fn try_encode_udaf(&self, node: &AggregateUDF, buf: &mut Vec<u8>) -> Result<()> {
        let udaf_kind = if let Some(func) =
            node.inner().as_any().downcast_ref::<PySparkAggregateUDF>()
        {
            let input_types = func
                .input_types()
                .iter()
                .map(|x| {
                    let input_type =
                        self.try_encode_message::<gen_datafusion_common::ArrowType>(x.try_into()?)?;
                    Ok(input_type)
                })
                .collect::<Result<_>>()?;
            let output_type = self.try_encode_message::<gen_datafusion_common::ArrowType>(
                func.output_type().try_into()?,
            )?;
            let deterministic = matches!(func.signature().volatility, Volatility::Immutable);
            UdafKind::PySparkAgg(gen::PySparkUdaf {
                function_name: func.function_name().to_string(),
                deterministic,
                input_types,
                output_type,
                python_bytes: func.python_bytes().to_vec(),
            })
        } else if let Some(_func) = node.inner().as_any().downcast_ref::<KurtosisFunction>() {
            UdafKind::Standard(gen::StandardUdaf {})
        } else if let Some(_func) = node.inner().as_any().downcast_ref::<MaxByFunction>() {
            UdafKind::Standard(gen::StandardUdaf {})
        } else if let Some(_func) = node.inner().as_any().downcast_ref::<MinByFunction>() {
            UdafKind::Standard(gen::StandardUdaf {})
        } else if let Some(_func) = node.inner().as_any().downcast_ref::<ModeFunction>() {
            UdafKind::Standard(gen::StandardUdaf {})
        } else if let Some(_func) = node.inner().as_any().downcast_ref::<SkewnessFunc>() {
            UdafKind::Standard(gen::StandardUdaf {})
        } else {
            return Ok(());
        };
        let node = ExtendedAggregateUdf {
            udaf_kind: Some(udaf_kind),
        };
        node.encode(buf)
            .map_err(|e| plan_datafusion_err!("failed to encode udaf: {e:?}"))
    }

    fn try_decode_expr(
        &self,
        _buf: &[u8],
        _inputs: &[Arc<dyn PhysicalExpr>],
    ) -> Result<Arc<dyn PhysicalExpr>> {
        not_impl_err!("PhysicalExtensionCodec is not provided")
    }

    fn try_encode_expr(&self, _node: &Arc<dyn PhysicalExpr>, _buf: &mut Vec<u8>) -> Result<()> {
        not_impl_err!("PhysicalExtensionCodec is not provided")
        // let expr_kind = if let Some(expr) = node.as_any().downcast_ref::<UnKnownColumn>() {
        //     return not_impl_err!("Unsupported physical expr: {node:?}");
        // } else {
        //     return not_impl_err!("Unsupported physical expr: {node:?}");
        // };
        // node.encode(buf)
        //     .map_err(|e| plan_datafusion_err!("failed to encode udaf: {e:?}"))
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
            Some(gen::task_write_location::Location::Memory(gen::TaskWriteLocationMemory {
                channel,
            })) => TaskWriteLocation::Memory {
                channel: channel.into(),
            },
            Some(gen::task_write_location::Location::Disk(gen::TaskWriteLocationDisk {
                channel,
            })) => TaskWriteLocation::Disk {
                channel: channel.into(),
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
            TaskWriteLocation::Memory { channel } => gen::TaskWriteLocation {
                location: Some(gen::task_write_location::Location::Memory(
                    gen::TaskWriteLocationMemory {
                        channel: channel.clone().into(),
                    },
                )),
            },
            TaskWriteLocation::Disk { channel } => gen::TaskWriteLocation {
                location: Some(gen::task_write_location::Location::Disk(
                    gen::TaskWriteLocationDisk {
                        channel: channel.clone().into(),
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
