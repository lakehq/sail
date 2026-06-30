use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FunctionMetadataEntry {
    name: String,
    #[serde(default)]
    scope: Option<String>,
    #[serde(default)]
    group: Option<String>,
    #[serde(default)]
    source: Option<String>,
    #[serde(default)]
    usage: Option<String>,
    #[serde(default)]
    arguments: Option<String>,
    #[serde(default)]
    examples: Option<String>,
    #[serde(default)]
    note: Option<String>,
    #[serde(default)]
    since: Option<String>,
    #[serde(default)]
    deprecated: Option<String>,
    #[serde(default)]
    class_name: Option<String>,
    #[serde(default)]
    signatures: Vec<String>,
}

impl FunctionMetadataEntry {
    fn validate(&self) -> Result<(), Box<dyn std::error::Error>> {
        if self.name.is_empty() {
            return Err("function metadata entry has an empty name".into());
        }
        if self.name != self.name.to_ascii_lowercase() {
            return Err(format!("function metadata name must be lowercase: {}", self.name).into());
        }
        let Some(scope) = self.scope.as_deref() else {
            return Err(format!("function metadata is missing scope: {}", self.name).into());
        };
        if !matches!(
            scope,
            "sql_builtin" | "spark_internal" | "pyspark_only" | "sail_extension"
        ) {
            return Err(format!(
                "function metadata has an unknown scope for {}: {}",
                self.name, scope
            )
            .into());
        }
        if self.group.as_deref().is_some_and(str::is_empty) {
            return Err(format!("function metadata has an empty group: {}", self.name).into());
        }
        if self.source.as_deref().is_some_and(str::is_empty) {
            return Err(format!("function metadata has an empty source: {}", self.name).into());
        }
        if self.usage.as_deref().is_some_and(str::is_empty) {
            return Err(format!("function metadata has empty usage: {}", self.name).into());
        }
        if self.arguments.as_deref().is_some_and(str::is_empty) {
            return Err(format!("function metadata has empty arguments: {}", self.name).into());
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
        if self.deprecated.as_deref().is_some_and(str::is_empty) {
            return Err(format!("function metadata has empty deprecated: {}", self.name).into());
        }
        if self.signatures.iter().any(|signature| signature.is_empty()) {
            return Err(format!("function metadata has an empty signature: {}", self.name).into());
        }
        if self.usage.as_deref().is_some_and(|usage| !usage.is_empty())
            && self.signatures.is_empty()
        {
            return Err(format!(
                "function metadata has usage but no signature: {}",
                self.name
            )
            .into());
        }
        if scope == "sql_builtin" {
            if self.group.is_none() {
                return Err(
                    format!("built-in function metadata is missing group: {}", self.name).into(),
                );
            }
            if self.source.is_none() {
                return Err(format!(
                    "built-in function metadata is missing source: {}",
                    self.name
                )
                .into());
            }
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

fn load_function_metadata_file(
    path: &Path,
) -> Result<Vec<FunctionMetadataEntry>, Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed={}", path.display());
    let content = std::fs::read_to_string(path)?;
    let entries: Vec<FunctionMetadataEntry> = serde_yaml::from_str(&content)?;

    let mut previous = None;
    for entry in &entries {
        entry.validate()?;
        if previous.is_some_and(|previous| previous > entry.name.as_str()) {
            return Err(format!(
                "function metadata entries must be sorted by name: {}",
                path.display()
            )
            .into());
        }
        previous = Some(entry.name.as_str());
    }
    Ok(entries)
}

fn collect_function_metadata_files(
    path: &Path,
    output: &mut Vec<PathBuf>,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed={}", path.display());
    let mut entries = std::fs::read_dir(path)?.collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(|entry| entry.path());
    for entry in entries {
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            collect_function_metadata_files(&path, output)?;
        } else if file_type.is_file()
            && path
                .extension()
                .and_then(|extension| extension.to_str())
                .is_some_and(|extension| matches!(extension, "yaml" | "yml"))
        {
            output.push(path);
        }
    }
    Ok(())
}

fn load_function_metadata(
    path: &Path,
) -> Result<Vec<FunctionMetadataEntry>, Box<dyn std::error::Error>> {
    let mut paths = Vec::new();
    collect_function_metadata_files(path, &mut paths)?;
    if paths.is_empty() {
        return Err(format!("no function metadata files found in {}", path.display()).into());
    }

    let mut entries = Vec::new();
    let mut seen = HashMap::new();
    for path in paths {
        for entry in load_function_metadata_file(&path)? {
            if let Some(previous_path) = seen.insert(entry.name.clone(), path.display().to_string())
            {
                return Err(format!(
                    "duplicate function metadata entry: {} in {} and {}",
                    entry.name,
                    previous_path,
                    path.display()
                )
                .into());
            }
            entries.push(entry);
        }
    }
    entries.sort_by(|left, right| left.name.cmp(&right.name));
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
        let is_public = entry.scope.as_deref() == Some("sql_builtin");
        let signatures = entry
            .signatures
            .iter()
            .map(|signature| rust_string(signature))
            .collect::<Vec<_>>()
            .join(", ");
        let usage = rust_option_string(entry.usage.as_deref());
        let arguments = rust_option_string(entry.arguments.as_deref());
        let examples = rust_option_string(entry.examples.as_deref());
        let note = rust_option_string(entry.note.as_deref());
        let since = rust_option_string(entry.since.as_deref());
        let deprecated = rust_option_string(entry.deprecated.as_deref());
        let class_name = rust_string(entry.class_name.as_deref().unwrap_or(""));
        output.push_str(&format!(
            "    BuiltInFunctionMetadata {{ name: {name}, signatures: &[{signatures}], usage: {usage}, arguments: {arguments}, examples: {examples}, note: {note}, since: {since}, deprecated: {deprecated}, class_name: {class_name}, is_public: {is_public} }},\n"
        ));
    }
    output.push_str("];\n");
    std::fs::write(out_dir.join("function_metadata.rs"), output)?;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);
    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);
    let entries = load_function_metadata(&manifest_dir.join("data/functions"))?;
    build_function_metadata(&entries, &out_dir)?;
    Ok(())
}
