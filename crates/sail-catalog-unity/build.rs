// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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

    let content = content
        .replace(
            "pub fn new(baseurl: &str) -> Self {",
            "pub fn new(baseurl: &str) -> Result<Self, reqwest::Error> {",
        )
        .replace(
            "Self::new_with_client(baseurl, client.build().unwrap())",
            "Ok(Self::new_with_client(baseurl, client.build()?))",
        )
        // We need to prevent the code examples from being treated as ignored tests.
        // These code snippets do not compile when running `cargo test -- --ignored`.
        .replace("```ignore", "```notrust");

    std::fs::write(out_file, content)?;

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=build.rs");
    build_unity_catalog()?;
    Ok(())
}
