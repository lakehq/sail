fn build_hms_thrift() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=thrift/hms.thrift");

    // We use the default output directory and filename for volo build.
    volo_build::Builder::thrift()
        .add_service("thrift/hive_metastore.thrift")
        .split_generated_files(true)
        .write()?;

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    build_hms_thrift()
}
