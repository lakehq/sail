use std::collections::HashMap;

use lazy_static::lazy_static;
use sail_common_datafusion::catalog::FunctionStatus;

#[derive(Debug, Clone, Copy)]
pub(crate) struct BuiltInFunctionMetadata {
    pub name: &'static str,
    pub signatures: &'static [&'static str],
    pub description: Option<&'static str>,
    pub class_name: &'static str,
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

pub(crate) fn built_in_function_status(name: &str) -> FunctionStatus {
    let Some(metadata) = BUILT_IN_FUNCTION_METADATA_BY_NAME.get(name) else {
        return FunctionStatus::built_in(name.to_string());
    };
    FunctionStatus {
        name: name.to_string(),
        catalog: None,
        namespace: None,
        signatures: metadata
            .signatures
            .iter()
            .map(|signature| signature.to_string())
            .collect(),
        description: metadata
            .description
            .map(|description| description.to_string()),
        class_name: metadata.class_name.to_string(),
        is_temporary: false,
    }
}

#[cfg(test)]
pub(crate) fn built_in_function_metadata_names() -> impl Iterator<Item = &'static str> {
    BUILT_IN_FUNCTION_METADATA
        .iter()
        .map(|metadata| metadata.name)
}
