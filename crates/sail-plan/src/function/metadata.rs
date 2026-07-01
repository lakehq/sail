use std::collections::HashMap;

use lazy_static::lazy_static;
use sail_common_datafusion::catalog::FunctionStatus;

#[derive(Debug, Clone, Copy)]
pub(crate) struct BuiltInFunctionMetadata {
    pub name: &'static str,
    pub signatures: &'static [&'static str],
    pub usage: Option<&'static str>,
    pub arguments: Option<&'static str>,
    pub examples: Option<&'static str>,
    pub note: Option<&'static str>,
    pub since: Option<&'static str>,
    pub deprecated: Option<&'static str>,
    pub class_name: &'static str,
    pub is_public: bool,
}

include!(concat!(env!("OUT_DIR"), "/function_metadata.rs"));

lazy_static! {
    static ref BUILT_IN_FUNCTION_METADATA_BY_NAME: HashMap<&'static str, BuiltInFunctionMetadata> =
        HashMap::from_iter(
            BUILT_IN_FUNCTION_METADATA
                .iter()
                .map(|metadata| (metadata.name, *metadata))
        );
}

fn built_in_function_status(metadata: &BuiltInFunctionMetadata) -> FunctionStatus {
    FunctionStatus {
        name: metadata.name.to_string(),
        catalog: None,
        namespace: None,
        signatures: metadata
            .signatures
            .iter()
            .map(|signature| signature.to_string())
            .collect(),
        usage: metadata.usage.map(|usage| usage.to_string()),
        arguments: metadata.arguments.map(|arguments| arguments.to_string()),
        examples: metadata.examples.map(|examples| examples.to_string()),
        note: metadata.note.map(|note| note.to_string()),
        since: metadata.since.map(|since| since.to_string()),
        deprecated: metadata.deprecated.map(|deprecated| deprecated.to_string()),
        class_name: metadata.class_name.to_string(),
        is_temporary: true,
    }
}

pub(crate) fn built_in_public_function_status(name: &str) -> Option<FunctionStatus> {
    let metadata = BUILT_IN_FUNCTION_METADATA_BY_NAME.get(name)?;
    metadata
        .is_public
        .then(|| built_in_function_status(metadata))
}
