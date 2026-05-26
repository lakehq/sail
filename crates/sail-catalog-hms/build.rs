use std::path::PathBuf;

fn build_hms_thrift() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=thrift/hms.thrift");

    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);

    volo_build::Builder::thrift()
        .add_service("thrift/hms.thrift")
        .out_dir(out_dir)
        .split_generated_files(false)
        .write()?;

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    build_hms_thrift()
}
