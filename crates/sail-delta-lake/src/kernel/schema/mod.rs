// use delta_kernel::expressions::{Expression, JunctionPredicateOp, Predicate, Scalar};
// use delta_kernel::schema::StructType;
// use deltalake::errors::{DeltaResult, DeltaTableError};
// use deltalake::partitions::PartitionFilter;
// use deltalake::PartitionValue;

// pub(crate) fn to_kernel_predicate(
//     filters: &[PartitionFilter],
//     table_schema: &StructType,
// ) -> DeltaResult<Predicate> {
//     let predicates = filters
//         .iter()
//         .map(|filter| filter_to_kernel_predicate(filter, table_schema))
//         .collect::<DeltaResult<Vec<_>>>()?;
//     Ok(Predicate::junction(JunctionPredicateOp::And, predicates))
// }

// fn filter_to_kernel_predicate(
//     filter: &PartitionFilter,
//     table_schema: &StructType,
// ) -> DeltaResult<Predicate> {
//     let Some(field) = table_schema.field(&filter.key) else {
//         return Err(DeltaTableError::SchemaMismatch {
//             msg: format!("Field '{}' is not a root table field.", filter.key),
//         });
//     };
//     let Some(dt) = field.data_type().as_primitive_opt() else {
//         return Err(DeltaTableError::SchemaMismatch {
//             msg: format!("Field '{}' is not a primitive type", field.name()),
//         });
//     };

//     let column = Expression::column([field.name()]);
//     Ok(match &filter.value {
//         // NOTE: In SQL NULL is not equal to anything, including itself. However when specifying partition filters
//         // we have allowed to equality against null. So here we have to handle null values explicitly by using
//         // is_null and is_not_null methods directly.
//         PartitionValue::Equal(raw) => {
//             let scalar = dt.parse_scalar(raw)?;
//             if scalar.is_null() {
//                 column.is_null()
//             } else {
//                 column.eq(scalar)
//             }
//         }
//         PartitionValue::NotEqual(raw) => {
//             let scalar = dt.parse_scalar(raw)?;
//             if scalar.is_null() {
//                 column.is_not_null()
//             } else {
//                 column.ne(scalar)
//             }
//         }
//         PartitionValue::LessThan(raw) => column.lt(dt.parse_scalar(raw)?),
//         PartitionValue::LessThanOrEqual(raw) => column.le(dt.parse_scalar(raw)?),
//         PartitionValue::GreaterThan(raw) => column.gt(dt.parse_scalar(raw)?),
//         PartitionValue::GreaterThanOrEqual(raw) => column.ge(dt.parse_scalar(raw)?),
//         op @ PartitionValue::In(raw_values) | op @ PartitionValue::NotIn(raw_values) => {
//             let values = raw_values
//                 .iter()
//                 .map(|v| dt.parse_scalar(v))
//                 .collect::<Result<Vec<_>, _>>()?;
//             let (expr, operator): (Box<dyn Fn(Scalar) -> Predicate>, _) = match op {
//                 PartitionValue::In(_) => {
//                     (Box::new(|v| column.clone().eq(v)), JunctionPredicateOp::Or)
//                 }
//                 PartitionValue::NotIn(_) => {
//                     (Box::new(|v| column.clone().ne(v)), JunctionPredicateOp::And)
//                 }
//                 _ => unreachable!(),
//             };
//             let predicates = values.into_iter().map(expr).collect::<Vec<_>>();
//             Predicate::junction(operator, predicates)
//         }
//     })
// }
