fn build_unity_catalog() -> Result<(), Box<dyn std::error::Error>> {
    let src = "spec/unity-catalog-all.yaml";
    println!("cargo:rerun-if-changed={src}");

    let file = std::fs::File::open(src)?;
    let spec = serde_yaml::from_reader(file)?;

    let mut settings = progenitor::GenerationSettings::new();
    settings.with_interface(progenitor::InterfaceStyle::Builder);
    let mut generator = progenitor::Generator::new(&settings);
    let tokens = generator.generate_tokens(&spec)?;
    let ast = syn::parse2(tokens)?;
    let content = prettyplease::unparse(&ast);

    let mut out_file = std::path::Path::new(&std::env::var("OUT_DIR")?).to_path_buf();
    out_file.push("unity_catalog.rs");

    std::fs::write(out_file, content)?;

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=build.rs");
    build_unity_catalog()?;
    Ok(())
}
