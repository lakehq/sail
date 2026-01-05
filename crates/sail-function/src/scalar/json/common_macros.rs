// https://github.com/datafusion-contrib/datafusion-functions-json/blob/78c5abbf7222510ff221517f5d2e3c344969da98/src/common_macros.rs
// Copyright datafusion-functions-json contributors
// Portions Copyright (2026) LakeSail, Inc.
// Modified in 2026 by LakeSail, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/// Creates external API `ScalarUDF` for an array UDF. Specifically, creates
///
/// Creates a singleton `ScalarUDF` of the `$udf_impl` function named `$expr_fn_name _udf` and a
/// function named `$expr_fn_name _udf` which returns that function.
///
/// This is used to ensure creating the list of `ScalarUDF` only happens once.
///
/// # Arguments
/// * `udf_impl`: name of the [`ScalarUDFImpl`]
/// * `expr_fn_name`: name of the `expr_fn` function to be created
/// * `arg`: 0 or more named arguments for the function
/// * `doc`: documentation string for the function
///
/// Copied mostly from, `/datafusion/functions-array/src/macros.rs`.
///
/// [`ScalarUDFImpl`]: datafusion_expr::ScalarUDFImpl
macro_rules! make_udf_function {
    ($udf_impl:ty, $expr_fn_name:ident, $($arg:ident)*, $doc:expr) => {
        paste::paste! {
            #[doc = $doc]
            #[must_use] pub fn $expr_fn_name($($arg: datafusion::logical_expr::Expr),*) -> datafusion::logical_expr::Expr {
                datafusion::logical_expr::Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
                    [< $expr_fn_name _udf >](),
                    vec![$($arg),*],
                ))
            }

            /// Singleton instance of [`$udf_impl`], ensures the UDF is only created once
            /// named for example `STATIC_JSON_OBJ_CONTAINS`
            static [< STATIC_ $expr_fn_name:upper >]: std::sync::OnceLock<std::sync::Arc<datafusion::logical_expr::ScalarUDF>> =
                std::sync::OnceLock::new();

            /// `ScalarFunction` that returns a [`ScalarUDF`] for [`$udf_impl`]
            ///
            /// [`ScalarUDF`]: datafusion::logical_expr::ScalarUDF
            pub fn [< $expr_fn_name _udf >]() -> std::sync::Arc<datafusion::logical_expr::ScalarUDF> {
                [< STATIC_ $expr_fn_name:upper >]
                    .get_or_init(|| {
                        std::sync::Arc::new(datafusion::logical_expr::ScalarUDF::new_from_impl(
                            <$udf_impl>::default(),
                        ))
                    })
                    .clone()
            }
        }
    };
}

pub(crate) use make_udf_function;
