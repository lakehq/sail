// SPDX-License-Identifier: Apache-2.0

// SPDX-SnippetBegin
// SPDX-License-Identifier: Apache-2.0
// SPDX-SnippetComment: Credit: https://github.com/apache/datafusion/blob/90b55e38b5de8e74a17e24bf7ea7259cbcdab022/datafusion/functions/src/utils.rs
/// $FUNC ScalarUDFImpl to test
/// $ARGS arguments (vec) to pass to function
/// $EXPECTED a Result<ColumnarValue>
/// $EXPECTED_TYPE is the expected value type
/// $EXPECTED_DATA_TYPE is the expected result type
/// $ARRAY_TYPE is the column type after function applied
/// $CONFIG_OPTIONS config options to pass to function
macro_rules! test_function {
($FUNC:expr, $ARGS:expr, $EXPECTED:expr, $EXPECTED_TYPE:ty, $EXPECTED_DATA_TYPE:expr, $ARRAY_TYPE:ident, $CONFIG_OPTIONS:expr) => {
    let expected: Result<Option<$EXPECTED_TYPE>> = $EXPECTED;
    let func = $FUNC;

    let data_array = $ARGS.iter().map(|arg| arg.data_type()).collect::<Vec<_>>();
    let cardinality = $ARGS
        .iter()
        .fold(Option::<usize>::None, |acc, arg| match arg {
            ColumnarValue::Scalar(_) => acc,
            ColumnarValue::Array(a) => Some(a.len()),
        })
        .unwrap_or(1);

        let scalar_arguments = $ARGS.iter().map(|arg| match arg {
            ColumnarValue::Scalar(scalar) => Some(scalar.clone()),
            ColumnarValue::Array(_) => None,
        }).collect::<Vec<_>>();
        let scalar_arguments_refs = scalar_arguments.iter().map(|arg| arg.as_ref()).collect::<Vec<_>>();

        let nullables = $ARGS.iter().map(|arg| match arg {
            ColumnarValue::Scalar(scalar) => scalar.is_null(),
            ColumnarValue::Array(a) => a.null_count() > 0,
        }).collect::<Vec<_>>();

        let field_array = data_array.into_iter().zip(nullables).enumerate()
            .map(|(idx, (data_type, nullable))| arrow::datatypes::Field::new(format!("field_{idx}"), data_type, nullable))
        .map(std::sync::Arc::new)
        .collect::<Vec<_>>();

    let return_field = func.return_field_from_args(datafusion_expr::ReturnFieldArgs {
        arg_fields: &field_array,
        scalar_arguments: &scalar_arguments_refs,
    });
        let arg_fields = $ARGS.iter()
        .enumerate()
            .map(|(idx, arg)| arrow::datatypes::Field::new(format!("f_{idx}"), arg.data_type(), true).into())
        .collect::<Vec<_>>();

    match expected {
        Ok(expected) => {
            assert_eq!(return_field.is_ok(), true);
            let return_field = return_field.unwrap();
            let return_type = return_field.data_type();
            assert_eq!(return_type, &$EXPECTED_DATA_TYPE);

                let result = func.invoke_with_args(datafusion_expr::ScalarFunctionArgs{
                args: $ARGS,
                arg_fields,
                number_rows: cardinality,
                return_field,
                    config_options: $CONFIG_OPTIONS
            });
                assert_eq!(result.is_ok(), true, "function returned an error: {}", result.unwrap_err());

                let result = result.unwrap().to_array(cardinality).expect("Failed to convert to array");
                let result = result.as_any().downcast_ref::<$ARRAY_TYPE>().expect("Failed to convert to type");
            assert_eq!(result.data_type(), &$EXPECTED_DATA_TYPE);

            // value is correct
            match expected {
                Some(v) => assert_eq!(result.value(0), v),
                None => assert!(result.is_null(0)),
            };
        }
        Err(expected_error) => {
            if let Ok(return_field) = return_field {
                // invoke is expected error - cannot use .expect_err() due to Debug not being implemented
                match func.invoke_with_args(datafusion_expr::ScalarFunctionArgs {
                    args: $ARGS,
                    arg_fields,
                    number_rows: cardinality,
                    return_field,
                    config_options: $CONFIG_OPTIONS,
                }) {
                    Ok(_) => assert!(false, "expected error"),
                    Err(error) => {
                        assert!(expected_error
                            .strip_backtrace()
                            .starts_with(&error.strip_backtrace()));
                    }
                }
            } else if let Err(error) = return_field {
                datafusion_common::assert_contains!(
                    expected_error.strip_backtrace(),
                    error.strip_backtrace()
                );
            }
        }
    };
};

    ($FUNC:expr, $ARGS:expr, $EXPECTED:expr, $EXPECTED_TYPE:ty, $EXPECTED_DATA_TYPE:expr, $ARRAY_TYPE:ident) => {
        test_function!(
            $FUNC,
            $ARGS,
            $EXPECTED,
            $EXPECTED_TYPE,
            $EXPECTED_DATA_TYPE,
            $ARRAY_TYPE,
            std::sync::Arc::new(datafusion_common::config::ConfigOptions::default())
        )
    };
}
// SPDX-SnippetEnd

pub(in crate::scalar::string) use test_function;
