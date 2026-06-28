use std::collections::HashSet;
use std::path::{Path, PathBuf};

use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FunctionMetadataEntry {
    name: String,
    #[serde(default)]
    signatures: Vec<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    examples: Option<String>,
    #[serde(default)]
    note: Option<String>,
    #[serde(default)]
    since: Option<String>,
    #[serde(default)]
    class_name: Option<String>,
}

impl FunctionMetadataEntry {
    fn validate(&self) -> Result<(), Box<dyn std::error::Error>> {
        if self.name.is_empty() {
            return Err("function metadata entry has an empty name".into());
        }
        if self.name != self.name.to_ascii_lowercase() {
            return Err(format!("function metadata name must be lowercase: {}", self.name).into());
        }
        if self.signatures.iter().any(|signature| signature.is_empty()) {
            return Err(format!("function metadata has an empty signature: {}", self.name).into());
        }
        if self.examples.as_deref().is_some_and(str::is_empty) {
            return Err(format!("function metadata has empty examples: {}", self.name).into());
        }
        if self.note.as_deref().is_some_and(str::is_empty) {
            return Err(format!("function metadata has an empty note: {}", self.name).into());
        }
        if self.since.as_deref().is_some_and(str::is_empty) {
            return Err(
                format!("function metadata has an empty since value: {}", self.name).into(),
            );
        }
        Ok(())
    }
}

fn rust_string(value: &str) -> String {
    format!("{value:?}")
}

fn rust_option_string(value: Option<&str>) -> String {
    match value {
        Some(value) => format!("Some({})", rust_string(value)),
        None => "None".to_string(),
    }
}

fn load_function_metadata(
    path: &Path,
) -> Result<Vec<FunctionMetadataEntry>, Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed={}", path.display());
    let content = std::fs::read_to_string(path)?;
    let entries: Vec<FunctionMetadataEntry> = serde_yaml::from_str(&content)?;

    let mut seen = HashSet::new();
    let mut previous = None;
    for entry in &entries {
        entry.validate()?;
        if !seen.insert(entry.name.as_str()) {
            return Err(format!("duplicate function metadata entry: {}", entry.name).into());
        }
        if previous.is_some_and(|previous| previous > entry.name.as_str()) {
            return Err("function metadata entries must be sorted by name".into());
        }
        previous = Some(entry.name.as_str());
    }
    Ok(entries)
}

fn build_function_metadata(
    entries: &[FunctionMetadataEntry],
    out_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut output = String::new();
    output
        .push_str("pub(crate) const BUILT_IN_FUNCTION_METADATA: &[BuiltInFunctionMetadata] = &[\n");
    for entry in entries {
        let name = rust_string(&entry.name);
        let signatures = entry
            .signatures
            .iter()
            .map(|signature| rust_string(signature))
            .collect::<Vec<_>>()
            .join(", ");
        let description = rust_option_string(entry.description.as_deref());
        let examples = rust_option_string(entry.examples.as_deref());
        let note = rust_option_string(entry.note.as_deref());
        let since = rust_option_string(entry.since.as_deref());
        let class_name = rust_string(entry.class_name.as_deref().unwrap_or(""));
        output.push_str(&format!(
            "    BuiltInFunctionMetadata {{ name: {name}, signatures: &[{signatures}], description: {description}, examples: {examples}, note: {note}, since: {since}, class_name: {class_name} }},\n"
        ));
    }
    output.push_str("];\n");
    std::fs::write(out_dir.join("function_metadata.rs"), output)?;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);
    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);
    let entries = load_function_metadata(&manifest_dir.join("data/functions.yaml"))?;
    build_function_metadata(&entries, &out_dir)?;
    Ok(())
}
