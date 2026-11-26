// SPDX-License-Identifier: Apache-2.0

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion_common::nested_struct::{
    requires_nested_struct_cast, validate_data_type_compatibility,
};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{DFSchema, Result};
use datafusion_expr::expr::Cast;
use datafusion_expr::expr_rewriter::NamePreserver;
use datafusion_expr::utils::merge_schema;
use datafusion_expr::{Expr, ExprSchemable, LogicalPlan, ScalarUDF, lit};
use sail_function::scalar::r#struct::struct_rename::StructRename;

/// Let a cast tighten nested nullability when the actual values meet the target contract.
pub fn rewrite_nested_nullability_casts(plan: LogicalPlan) -> Result<LogicalPlan> {
    plan.transform_up_with_subqueries(rewrite_plan_casts)
        .map(|result| result.data)
}

pub(crate) fn rewrite_plan_casts(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let schema = match &plan {
        LogicalPlan::TableScan(scan) => {
            DFSchema::try_from_qualified_schema(scan.table_name.clone(), &scan.source.schema())?
        }
        _ => merge_schema(&plan.inputs()),
    };
    let names = NamePreserver::new(&plan);
    plan.map_expressions(|expr| {
        let name = names.save(&expr);
        expr.transform_up(|expr| {
            let Expr::Cast(Cast { expr: input, field }) = &expr else {
                return Ok(Transformed::no(expr));
            };
            let target = field.data_type();
            if !target.is_nested() {
                return Ok(Transformed::no(expr));
            }
            let Ok(source) = input.get_type(&schema) else {
                return Ok(Transformed::no(expr));
            };
            if !requires_nested_struct_cast(&source, target)
                || source == *target
                || !same_storage_type(&source, target)
                || validate_data_type_compatibility(field.name(), &source, target).is_ok()
            {
                return Ok(Transformed::no(expr));
            }

            // The checked array constructors apply the target fields to the visible values.
            let udf = ScalarUDF::from(StructRename::new(target.clone()));
            let input = udf.call(vec![input.as_ref().clone(), lit(true)]);
            Ok(Transformed::yes(Expr::Cast(Cast::new_from_field(
                Box::new(input),
                field.clone(),
            ))))
        })
        .map(|result| result.update_data(|expr| name.restore(expr)))
    })
}

fn same_storage_type(source: &DataType, target: &DataType) -> bool {
    let compatible_field =
        |source: &Field, target: &Field| same_storage_type(source.data_type(), target.data_type());
    match (source, target) {
        (DataType::Struct(source), DataType::Struct(target)) => {
            source.len() == target.len()
                && source.iter().zip(target).all(|(source, target)| {
                    source.name() == target.name() && compatible_field(source, target)
                })
        }
        (DataType::List(source), DataType::List(target))
        | (DataType::LargeList(source), DataType::LargeList(target)) => {
            compatible_field(source, target)
        }
        (DataType::FixedSizeList(source, a), DataType::FixedSizeList(target, b)) => {
            a == b && compatible_field(source, target)
        }
        (DataType::Map(source, a), DataType::Map(target, b)) => {
            a == b && compatible_field(source, target)
        }
        _ => source == target,
    }
}
