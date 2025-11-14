struct ProtoBuilder<'a> {
    package: &'a str,
    files: &'a [&'a str],
}

impl<'a> ProtoBuilder<'a> {
    fn new(package: &'a str, files: &'a [&'a str]) -> Self {
        Self { package, files }
    }

    fn build(self) -> Result<(), Box<dyn std::error::Error>> {
        let protos = self
            .files
            .iter()
            .map(|file| format!("proto/sail/{}/{}", self.package, file))
            .collect::<Vec<_>>();

        let proto_paths = protos.iter().map(|s| s.as_str()).collect::<Vec<_>>();

        tonic_prost_build::configure()
            .compile_well_known_types(true)
            .compile_protos(&proto_paths, &["proto"])?;

        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=build.rs");
    ProtoBuilder::new("streaming", &["marker.proto"]).build()?;
    Ok(())
}
