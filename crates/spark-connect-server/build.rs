use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);
    tonic_build::configure()
        .file_descriptor_set_path(out_dir.join("spark_connect_descriptor.bin"))
        .build_server(true)
        .compile(
            &[
                "proto/spark/connect/base.proto",
                "proto/spark/connect/catalog.proto",
                "proto/spark/connect/commands.proto",
                "proto/spark/connect/common.proto",
                "proto/spark/connect/expressions.proto",
                "proto/spark/connect/relations.proto",
                "proto/spark/connect/types.proto",
            ],
            &["proto"],
        )?;
    Ok(())
}
