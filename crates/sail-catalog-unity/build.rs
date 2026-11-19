// SPDX-License-Identifier: Apache-2.0

use sail_build_scripts::openapi::generator::{OpenApiConfig, generate_openapi_client};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=build.rs");

    generate_openapi_client(
        "spec/unity-catalog-all.yaml",
        std::path::Path::new(&std::env::var("OUT_DIR")?).join("unity_catalog_gen.rs"),
        OpenApiConfig {
            excluded_operations: [].into_iter().collect(),
            excluded_schemas: ["ColumnTypeName".to_owned()].into_iter().collect(),
            raw_path_parameters: Default::default(),
        },
    )?;

    generate_openapi_client(
        "spec/unity-catalog-delta.yaml",
        std::path::Path::new(&std::env::var("OUT_DIR")?).join("unity_catalog_delta_gen.rs"),
        OpenApiConfig {
            excluded_operations: [
                "createStagingTable",
                "createTable",
                "deleteTable",
                "tableExists",
                "reportMetrics",
                "getStagingTableCredentials",
                "renameTable",
                "getTemporaryPathCredentials",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect(),
            excluded_schemas: ["DeltaDataType", "DeltaLoadTableResponse"]
                .into_iter()
                .map(str::to_owned)
                .collect(),
            raw_path_parameters: Default::default(),
        },
    )?;

    Ok(())
}
