use std::path::PathBuf;

fn build_proto() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);
    let descriptor_path = out_dir.join("sail_descriptor.bin");
    tonic_build::configure()
        .file_descriptor_set_path(&descriptor_path)
        .compile_well_known_types(true)
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &["proto/sail/driver.proto", "proto/sail/worker.proto"],
            &["proto"],
        )?;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=build.rs");
    build_proto()?;
    Ok(())
}
