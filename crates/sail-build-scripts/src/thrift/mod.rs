//! Client-only Thrift generation using Apache's in-memory protocol API.
//!
//! The parser accepts a subset of Thrift IDL. Only selected methods and their
//! transitive type dependencies are emitted; unsupported syntax or reachable types
//! fail generation. Network I/O lives in `sail-thrift`.

mod codegen;
mod parser;

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use crate::error::{BuildError, BuildResult};

fn invalid(message: impl Into<String>) -> BuildError {
    BuildError::InvalidInput(message.into())
}

#[derive(Clone, Debug)]
enum Type {
    Named(String),
    List(Box<Type>),
    Set(Box<Type>),
    Map(Box<Type>, Box<Type>),
}

impl Type {
    fn references(&self, names: &mut BTreeSet<String>) {
        match self {
            Self::Named(n) if !is_primitive(n) => {
                names.insert(n.clone());
            }
            Self::List(t) | Self::Set(t) => t.references(names),
            Self::Map(k, v) => {
                k.references(names);
                v.references(names);
            }
            _ => {}
        }
    }
}

fn is_primitive(n: &str) -> bool {
    matches!(
        n,
        "void" | "bool" | "byte" | "i8" | "i16" | "i32" | "i64" | "double" | "string" | "binary"
    )
}

#[derive(Clone, Debug)]
enum Literal {
    String(String),
    Bool(bool),
    Integer(i64),
    Double(f64),
    Reference(Vec<String>),
}

#[derive(Clone, Debug)]
struct Field {
    id: i16,
    name: String,
    ty: Type,
    required: bool,
    default: Option<Literal>,
}

#[derive(Debug)]
enum Definition {
    Struct(Vec<Field>),
    Exception(Vec<Field>),
    Enum(Vec<(String, i32)>),
    Unsupported,
}

#[derive(Debug)]
struct Method {
    name: String,
    result: Type,
    args: Vec<Field>,
    exceptions: Vec<Field>,
}

struct Schema {
    definitions: BTreeMap<String, Definition>,
    service: String,
    methods: BTreeMap<String, Method>,
}

fn generate(source: &str, selected: &[&str]) -> BuildResult<String> {
    let schema = parser::parse(source)?;
    let selected = selected.iter().copied().collect::<BTreeSet<_>>();
    if selected.is_empty() {
        return Err(invalid("no Thrift client methods selected"));
    }

    let mut needed = BTreeSet::new();
    for name in &selected {
        let method = schema
            .methods
            .get(*name)
            .ok_or_else(|| invalid(format!("unknown Thrift method {name}")))?;
        method.result.references(&mut needed);
        for f in method.args.iter().chain(&method.exceptions) {
            f.ty.references(&mut needed);
        }
    }

    let mut pending = needed.iter().cloned().collect::<Vec<_>>();
    while let Some(name) = pending.pop() {
        let def = schema
            .definitions
            .get(&name)
            .ok_or_else(|| invalid(format!("unknown Thrift type {name}")))?;
        match def {
            Definition::Struct(fields) | Definition::Exception(fields) => {
                let mut refs = BTreeSet::new();
                for f in fields {
                    f.ty.references(&mut refs);
                }

                for name in refs {
                    if needed.insert(name.clone()) {
                        pending.push(name);
                    }
                }
            }
            Definition::Enum(_) => {}
            _ => return Err(invalid(format!("unsupported reachable Thrift type {name}"))),
        }
    }

    let tokens = codegen::generate(&schema, &needed, &selected)?;
    // Parse and format once in process, without rustfmt subprocesses or per-type files.
    Ok(prettyplease::unparse(&syn::parse2(tokens)?))
}

/// Generate selected client methods and reachable types at the given output path.
///
/// The IDL must contain exactly one service. Its name determines the generated
/// client type; method names are the original IDL names. The consuming crate needs
/// dependencies on `thrift` and `sail-thrift`.
pub fn generate_client(
    idl: impl AsRef<Path>,
    output_path: impl AsRef<Path>,
    methods: &[&str],
) -> BuildResult<()> {
    let idl = idl.as_ref();
    println!("cargo:rerun-if-changed={}", idl.display());
    let output = generate(&std::fs::read_to_string(idl)?, methods)?;
    let target = output_path.as_ref();
    if std::fs::read_to_string(target).ok().as_ref() != Some(&output) {
        std::fs::write(target, output)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::generate;

    #[test]
    fn generates_only_reachable_types_and_preserves_wire_ids()
    -> Result<(), Box<dyn std::error::Error>> {
        let output = generate(
            "struct Table { 1: string tableName, 2: optional bool temporary=false } exception MetaException { 1: string message } struct Unused { 1: string ignored } service Catalog { void drop_table(1: Table table) throws(3: MetaException o3) void unused() }",
            &["drop_table"],
        )?;
        assert!(output.contains("pub struct Table"));
        assert!(output.contains("pub struct CatalogClient"));
        assert!(output.contains("table_name: Option<String>"));
        assert!(output.contains("Some(false)"));
        assert!(output.contains("O3(MetaException)"));
        assert!(output.contains("(Some(3), TType::Struct)"));
        assert!(!output.contains("Unused"));
        assert!(!output.contains("fn unused"));
        assert!(!output.contains("Server"));
        assert!(generate("service Catalog { void ok() }", &["missing"]).is_err());
        Ok(())
    }
}
