//! Source-level coverage check for `RemoteExecutionCodec`.
//!
//! When a new `ExecutionPlan` is added to the workspace, three things have to be
//! kept in sync or distributed execution silently breaks at runtime:
//!   1. an encode branch in `codec.rs::try_encode`
//!   2. a decode arm in `codec.rs::try_decode`
//!   3. a `oneof NodeKind` variant in `physical.proto`
//!
//! This test scans the workspace and asserts those three sets line up. It will
//! not catch wrong-field bugs — only "you forgot the wiring entirely". Pair it
//! with cluster-mode integration tests for correctness.

use std::collections::{BTreeSet, HashSet};
use std::path::{Path, PathBuf};

use regex::Regex;
use walkdir::WalkDir;

// Sail Execs that legitimately do not get a top-level encode branch. Add to
// this list with a comment explaining why.
const ENCODE_ALLOWLIST: &[&str] = &[
    // Driver-side wrappers, never shipped to a worker.
    "ShuffleReadExec",
    "ShuffleWriteExec",
    // Tracing wrapper that wraps another Exec; the inner Exec is what gets encoded.
    "TracingExec",
    // Planner-internal node rewritten to DataFusion's RepartitionExec /
    // CoalescePartitionsExec by RewriteExplicitRepartition before encoding.
    "ExplicitRepartitionExec",
];

// `NodeKind` variants that have an encode branch but no decode arm (or vice
// versa). Empty for new code; entries here document preexisting gaps so the
// test can fail on *new* regressions without forcing us to fix the legacy
// asymmetry in the same change. Add an issue link or short note for each.
const NODE_KIND_ASYMMETRY_ALLOWLIST: &[&str] = &[
    // Encoded by `try_encode` (line ~1266) but never decoded. DataFusion's
    // physical planner can emit `PartialSortExec` as an optimization, so any
    // plan containing one will fail on the worker. Predates this test;
    // tracked separately.
    "PartialSort",
    // Decoded but never encoded. `ValuesExec` is a DataFusion built-in that
    // currently isn't reached by the encode path. Predates this test.
    "Values",
];

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

fn read(path: &Path) -> String {
    std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

// All top-level `impl ExecutionPlan for X` declarations across the workspace.
// "Top-level" = the `impl` keyword starts at column 0; this excludes test
// fixtures defined inside `mod tests { ... }` blocks.
fn declared_execs() -> BTreeSet<String> {
    let impl_re = Regex::new(
        r"(?m)^impl(?:\s*<[^>]+>)?\s+(?:datafusion::physical_plan::)?ExecutionPlan\s+for\s+(\w+)",
    )
    .unwrap();

    let mut out = BTreeSet::new();
    for entry in WalkDir::new(workspace_root().join("crates")) {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("rs") {
            continue;
        }
        // Skip codec.rs and this test itself.
        if path.ends_with("codec.rs") || path.ends_with("codec_coverage.rs") {
            continue;
        }
        let src = read(path);
        for cap in impl_re.captures_iter(&src) {
            out.insert(cap[1].to_string());
        }
    }
    out
}

fn codec_src() -> String {
    read(&workspace_root().join("crates/sail-execution/src/codec.rs"))
}

// Extract the body of the named method by finding `    fn <name>(` (4-space
// indent for trait-method declarations inside `impl PhysicalExtensionCodec`)
// and returning everything up to the next sibling `    fn ` declaration.
// Scoping to plan-level method bodies avoids false positives from the
// UDF/UDAF/Expr methods further down in the same trait impl.
fn method_body<'a>(src: &'a str, name: &str) -> &'a str {
    let needle = format!("    fn {name}(");
    let start = src
        .find(&needle)
        .unwrap_or_else(|| panic!("could not find `{needle}` in codec.rs"));
    let after = &src[start + needle.len()..];
    let end = after.find("\n    fn ").unwrap_or(after.len());
    &after[..end]
}

// Top-level Exec types referenced in `try_encode` via `node.as_any()
// .downcast_ref::<X>()`. The `node.as_any()` qualifier filters out inner
// downcasts (e.g. `source.as_any().downcast_ref::<FileScanConfig>()` inside
// the `DataSourceExec` branch), which are not themselves encoded execs.
fn encoded_outer_execs(src: &str) -> BTreeSet<String> {
    let body = method_body(src, "try_encode");
    // `\s*` matches newlines too — handle calls split across multiple lines.
    let re = Regex::new(
        r"node\s*\.\s*as_any\s*\(\s*\)\s*\.\s*downcast_ref::<(\w+)(?:<[^>]+>)?>",
    )
    .unwrap();
    re.captures_iter(body).map(|c| c[1].to_string()).collect()
}

// `NodeKind::X` variants matched in the body of the named method.
fn node_kinds_in(src: &str, method: &str) -> BTreeSet<String> {
    let body = method_body(src, method);
    let re = Regex::new(r"NodeKind::(\w+)\s*\(").unwrap();
    re.captures_iter(body).map(|c| c[1].to_string()).collect()
}

fn proto_src() -> String {
    read(&workspace_root().join("crates/sail-execution/proto/sail/plan/physical.proto"))
}

// Every `XxxExecNode` message referenced as a field in the proto (i.e. each
// entry in `oneof NodeKind`).
fn proto_messages(src: &str) -> BTreeSet<String> {
    let re = Regex::new(r"(\w+ExecNode)\s+\w+\s*=\s*\d+").unwrap();
    re.captures_iter(src).map(|c| c[1].to_string()).collect()
}

#[test]
fn every_sail_exec_has_codec_encode() {
    let allowlist: HashSet<&str> = ENCODE_ALLOWLIST.iter().copied().collect();
    let declared = declared_execs();
    let encoded = encoded_outer_execs(&codec_src());

    let missing: Vec<&String> = declared
        .iter()
        .filter(|e| !encoded.contains(*e) && !allowlist.contains(e.as_str()))
        .collect();

    assert!(
        missing.is_empty(),
        "These ExecutionPlan impls have no encode branch in codec.rs:\n  {}\n\n\
         Fix by adding `else if let Some(x) = node.as_any().downcast_ref::<X>() {{ ... }}` \
         in `try_encode`, or — if the Exec is intentionally driver-only — add it to \
         ENCODE_ALLOWLIST in this test with a comment explaining why.",
        missing
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>()
            .join("\n  ")
    );
}

#[test]
fn encode_and_decode_node_kinds_match() {
    let src = codec_src();
    let allowlist: HashSet<&str> = NODE_KIND_ASYMMETRY_ALLOWLIST.iter().copied().collect();

    let encoded = node_kinds_in(&src, "try_encode");
    let decoded = node_kinds_in(&src, "try_decode");

    let only_encoded: Vec<&String> = encoded
        .difference(&decoded)
        .filter(|v| !allowlist.contains(v.as_str()))
        .collect();
    let only_decoded: Vec<&String> = decoded
        .difference(&encoded)
        .filter(|v| !allowlist.contains(v.as_str()))
        .collect();

    assert!(
        only_encoded.is_empty() && only_decoded.is_empty(),
        "encode and decode are out of sync in codec.rs:\n\
         encoded but never decoded: {only_encoded:?}\n\
         decoded but never encoded: {only_decoded:?}\n\n\
         Every node type must have BOTH a `NodeKind::X(...)` constructor in `try_encode` \
         and a `NodeKind::X(...)` arm in `try_decode`. If a gap is intentional, add the \
         variant to NODE_KIND_ASYMMETRY_ALLOWLIST with an explanation."
    );
}

#[test]
fn every_node_kind_has_proto_message() {
    let src = codec_src();
    let proto = proto_messages(&proto_src());

    let used: BTreeSet<String> = node_kinds_in(&src, "try_encode")
        .union(&node_kinds_in(&src, "try_decode"))
        .cloned()
        .collect();

    let missing: Vec<String> = used
        .iter()
        .filter_map(|variant| {
            let message = format!("{variant}ExecNode");
            if proto.contains(&message) { None } else { Some(message) }
        })
        .collect();

    assert!(
        missing.is_empty(),
        "These proto messages are referenced via NodeKind in codec.rs but missing from \
         crates/sail-execution/proto/sail/plan/physical.proto:\n  {}\n\n\
         Add a corresponding `oneof NodeKind` variant and message definition.",
        missing.join("\n  ")
    );
}
