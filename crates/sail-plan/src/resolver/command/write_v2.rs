use datafusion_expr::LogicalPlan;
use sail_catalog::provider::{CatalogPartitionField, PartitionTransform};
use sail_common::spec;
use sail_common_datafusion::utils::items::ItemTaker;

use crate::error::{PlanError, PlanResult};
use crate::resolver::command::write::{
    WriteColumnMatch, WriteMode, WritePlanBuilder, WriteTableAction, WriteTarget,
};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    /// Resolves the write operation for the Spark DataFrameWriter v2 API.
    pub(super) async fn resolve_command_write_to(
        &self,
        write_to: spec::WriteTo,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        use spec::WriteToMode;

        let spec::WriteTo {
            input,
            provider,
            table,
            mode,
            partitioning_columns,
            clustering_columns,
            options,
            table_properties,
        } = write_to;

        let input = self.resolve_write_input(*input, state).await?;
        let partition_by = self.resolve_write_partition_by_expressions(partitioning_columns)?;
        let cluster_by = self.resolve_write_cluster_by_columns(clustering_columns)?;

        let mut builder = WritePlanBuilder::new()
            .with_partition_by(partition_by)
            .with_cluster_by(cluster_by)
            .with_options(options)
            .with_table_properties(table_properties);

        if let Some(provider) = provider {
            builder = builder.with_format(provider);
        }

        match mode {
            WriteToMode::Append => {
                builder = builder
                    .with_target(WriteTarget::ExistingTable {
                        table,
                        column_match: WriteColumnMatch::ByName,
                    })
                    .with_mode(WriteMode::Append);
            }
            WriteToMode::Create => {
                builder = builder
                    .with_target(WriteTarget::NewTable {
                        table,
                        action: WriteTableAction::Create,
                    })
                    .with_mode(WriteMode::Overwrite);
            }
            WriteToMode::CreateOrReplace => {
                builder = builder
                    .with_target(WriteTarget::NewTable {
                        table,
                        action: WriteTableAction::CreateOrReplace,
                    })
                    .with_mode(WriteMode::Overwrite);
            }
            WriteToMode::Overwrite { condition } => {
                builder = builder
                    .with_target(WriteTarget::ExistingTable {
                        table,
                        column_match: WriteColumnMatch::ByName,
                    })
                    .with_mode(WriteMode::OverwriteIf {
                        condition: Box::new(spec::ExprWithSource {
                            expr: *condition,
                            source: None,
                        }),
                    });
            }
            WriteToMode::OverwritePartitions => {
                builder = builder
                    .with_target(WriteTarget::ExistingTable {
                        table,
                        column_match: WriteColumnMatch::ByName,
                    })
                    .with_mode(WriteMode::OverwritePartitions);
            }
            WriteToMode::Replace => {
                builder = builder
                    .with_target(WriteTarget::NewTable {
                        table,
                        action: WriteTableAction::Replace,
                    })
                    .with_mode(WriteMode::Overwrite);
            }
        };
        self.resolve_write_with_builder(input, builder, state).await
    }

    fn resolve_write_partition_by_expressions(
        &self,
        partition_by: Vec<spec::Expr>,
    ) -> PlanResult<Vec<CatalogPartitionField>> {
        partition_by
            .into_iter()
            .map(|x| match x {
                spec::Expr::UnresolvedAttribute {
                    name,
                    plan_id: None,
                    is_metadata_column: false,
                } => {
                    let name: Vec<String> = name.into();
                    Ok(CatalogPartitionField {
                        column: name.one()?,
                        transform: None,
                    })
                }
                spec::Expr::UnresolvedFunction(f) => self.resolve_partition_transform_function(f),
                _ => Err(PlanError::invalid(
                    "partitioning column must be a column reference or transform function",
                )),
            })
            .collect()
    }

    fn resolve_partition_transform_function(
        &self,
        func: spec::UnresolvedFunction,
    ) -> PlanResult<CatalogPartitionField> {
        let function_name: Vec<String> = func.function_name.into();
        let function_name = function_name.one()?;
        let function_name_lower = function_name.to_lowercase();

        match function_name_lower.as_str() {
            "years" | "months" | "days" | "hours" => {
                let transform = match function_name_lower.as_str() {
                    "years" => PartitionTransform::Year,
                    "months" => PartitionTransform::Month,
                    "days" => PartitionTransform::Day,
                    "hours" => PartitionTransform::Hour,
                    _ => unreachable!(),
                };
                let column = self.extract_partition_column_from_args(&func.arguments, 0)?;
                Ok(CatalogPartitionField {
                    column,
                    transform: Some(transform),
                })
            }
            "bucket" => {
                let num_buckets =
                    self.extract_partition_int_arg(&func.arguments, 0, "bucket count")?;
                let column = self.extract_partition_column_from_args(&func.arguments, 1)?;
                Ok(CatalogPartitionField {
                    column,
                    transform: Some(PartitionTransform::Bucket(num_buckets)),
                })
            }
            "truncate" => {
                let width = self.extract_partition_int_arg(&func.arguments, 0, "truncate width")?;
                let column = self.extract_partition_column_from_args(&func.arguments, 1)?;
                Ok(CatalogPartitionField {
                    column,
                    transform: Some(PartitionTransform::Truncate(width)),
                })
            }
            _ => Err(PlanError::invalid(format!(
                "unsupported partition transform function: {function_name}"
            ))),
        }
    }

    fn extract_partition_column_from_args(
        &self,
        args: &[spec::Expr],
        index: usize,
    ) -> PlanResult<String> {
        let arg = args.get(index).ok_or_else(|| {
            PlanError::invalid(format!(
                "partition transform function requires argument at index {index}"
            ))
        })?;
        match arg {
            spec::Expr::UnresolvedAttribute {
                name,
                plan_id: None,
                is_metadata_column: false,
            } => {
                let name: Vec<String> = name.clone().into();
                Ok(name.one()?)
            }
            _ => Err(PlanError::invalid(
                "partition transform function argument must be a column reference",
            )),
        }
    }

    fn extract_partition_int_arg(
        &self,
        args: &[spec::Expr],
        index: usize,
        description: &str,
    ) -> PlanResult<u32> {
        let arg = args.get(index).ok_or_else(|| {
            PlanError::invalid(format!(
                "partition transform function requires {description} at index {index}"
            ))
        })?;
        match arg {
            spec::Expr::Literal(lit) => match lit {
                spec::Literal::Int8 { value: Some(v) } => u32::try_from(*v).map_err(|_| {
                    PlanError::invalid(format!("{description} must be a positive integer"))
                }),
                spec::Literal::Int16 { value: Some(v) } => u32::try_from(*v).map_err(|_| {
                    PlanError::invalid(format!("{description} must be a positive integer"))
                }),
                spec::Literal::Int32 { value: Some(v) } => u32::try_from(*v).map_err(|_| {
                    PlanError::invalid(format!("{description} must be a positive integer"))
                }),
                spec::Literal::Int64 { value: Some(v) } => u32::try_from(*v).map_err(|_| {
                    PlanError::invalid(format!("{description} must be a positive integer"))
                }),
                spec::Literal::UInt8 { value: Some(v) } => Ok(u32::from(*v)),
                spec::Literal::UInt16 { value: Some(v) } => Ok(u32::from(*v)),
                spec::Literal::UInt32 { value: Some(v) } => Ok(*v),
                spec::Literal::UInt64 { value: Some(v) } => u32::try_from(*v).map_err(|_| {
                    PlanError::invalid(format!("{description} must fit in a 32-bit integer"))
                }),
                _ => Err(PlanError::invalid(format!(
                    "{description} must be an integer literal"
                ))),
            },
            _ => Err(PlanError::invalid(format!(
                "{description} must be an integer literal"
            ))),
        }
    }
}
