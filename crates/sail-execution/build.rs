use std::path::PathBuf;

fn build_proto(service: &str, files: &[&str]) -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);
    let descriptor_path = out_dir.join(format!("sail_{service}_descriptor.bin"));
    let protos = files
        .iter()
        .map(|file| format!("proto/sail/{service}/{file}"))
        .collect::<Vec<_>>();
    tonic_build::configure()
        .file_descriptor_set_path(&descriptor_path)
        .compile_well_known_types(true)
        .build_server(true)
        .build_client(true)
        .compile_protos(&protos, &["proto"])?;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=build.rs");
    build_proto("driver", &["service.proto"])?;
    build_proto("worker", &["service.proto"])?;
    Ok(())
}
