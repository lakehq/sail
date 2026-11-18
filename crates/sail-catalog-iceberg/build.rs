// SPDX-License-Identifier: Apache-2.0

use sail_build_scripts::openapi::generator::{OpenApiConfig, generate_openapi_client};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=build.rs");

    generate_openapi_client(
        "spec/iceberg-rest-catalog.yaml",
        std::path::Path::new(&std::env::var("OUT_DIR")?).join("iceberg_rest_catalog_gen.rs"),
        OpenApiConfig {
            excluded_operations: ["getToken".to_owned()].into_iter().collect(),
            excluded_schemas: ["ReportMetricsRequest".to_owned(), "Snapshot".to_owned()]
                .into_iter()
                .collect(),
            raw_path_parameters: ["prefix".to_owned()].into_iter().collect(),
        },
    )?;

    Ok(())
}
