use std::sync::Arc;

use datafusion_expr::ScalarUDF;

/// Backend kernels are handles only; argument and result typing stays in native analysis.
pub(crate) fn default_scalar_kernel(name: &str) -> Option<Arc<ScalarUDF>> {
    static FUNCTIONS: std::sync::LazyLock<std::collections::HashMap<String, Arc<ScalarUDF>>> =
        std::sync::LazyLock::new(|| {
            let mut functions = std::collections::HashMap::new();
            for function in datafusion::functions::all_default_functions()
                .into_iter()
                .chain(datafusion::functions_nested::all_default_nested_functions())
            {
                functions.insert(function.name().to_string(), Arc::clone(&function));
                for alias in function.aliases() {
                    functions.insert(alias.clone(), Arc::clone(&function));
                }
            }
            functions
        });
    sail_function::scalar::jev::JevKind::from_name(name)
        .map(|kind| Arc::new(sail_function::scalar::jev::jev_udf(kind)))
        .or_else(|| FUNCTIONS.get(name).cloned())
}
