use sail_build_scripts::data_source::options::build_data_source_options;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=build.rs");
    build_data_source_options("Binary", "binary")?;
    build_data_source_options("Csv", "csv")?;
    build_data_source_options("Json", "json")?;
    build_data_source_options("Parquet", "parquet")?;
    build_data_source_options("Text", "text")?;
    build_data_source_options("Delta", "delta")?;
    build_data_source_options("Iceberg", "iceberg")?;
    build_data_source_options("Socket", "socket")?;
    build_data_source_options("Rate", "rate")?;
    Ok(())
}
