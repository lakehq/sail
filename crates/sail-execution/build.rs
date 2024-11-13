use std::path::PathBuf;

use prost_build::Config;

struct ProtoBuilder<'a> {
    package: &'a str,
    files: &'a [&'a str],
    skip_debug: Option<&'a [&'a str]>,
    with_service: bool,
}

impl<'a> ProtoBuilder<'a> {
    fn new(package: &'a str, files: &'a [&'a str]) -> Self {
        Self {
            package,
            files,
            skip_debug: None,
            with_service: false,
        }
    }

    fn skip_debug(mut self, skip_debug: &'a [&'a str]) -> Self {
        self.skip_debug = Some(skip_debug);
        self
    }

    fn with_service(mut self) -> Self {
        self.with_service = true;
        self
    }

    fn build(self) -> Result<(), Box<dyn std::error::Error>> {
        let protos = self
            .files
            .iter()
            .map(|file| format!("proto/sail/{}/{}", self.package, file))
            .collect::<Vec<_>>();

        let builder = tonic_build::configure();

        let builder = if self.with_service {
            let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);
            let descriptor_path = out_dir.join(format!("sail_{}_descriptor.bin", self.package));
            builder
                .file_descriptor_set_path(&descriptor_path)
                .build_server(true)
                .build_client(true)
        } else {
            builder
        };

        let mut config = Config::new();
        if let Some(skip_debug) = self.skip_debug {
            config.skip_debug(skip_debug);
        }

        builder
            .compile_well_known_types(true)
            .compile_protos_with_config(config, &protos, &["proto"])?;

        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=build.rs");
    ProtoBuilder::new("plan", &["physical.proto"]).build()?;
    ProtoBuilder::new("driver", &["service.proto"])
        .with_service()
        .build()?;
    ProtoBuilder::new("worker", &["service.proto"])
        .skip_debug(&["sail.worker.RunTaskRequest"])
        .with_service()
        .build()?;
    Ok(())
}
