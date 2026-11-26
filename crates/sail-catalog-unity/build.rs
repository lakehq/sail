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

    Ok(())
}
