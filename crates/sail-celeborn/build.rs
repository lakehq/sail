fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The transport Protocol Buffers file is copied from the Celeborn project.
    // See `common/src/main/proto/TransportMessages.proto` for the original file.
    println!("cargo:rerun-if-changed=proto/celeborn/transport.proto");
    tonic_prost_build::configure()
        .include_file("celeborn.rs")
        .compile_protos(&["proto/celeborn/transport.proto"], &["proto/celeborn"])?;
    Ok(())
}
