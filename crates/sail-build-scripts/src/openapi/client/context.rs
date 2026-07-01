use std::collections::{BTreeMap, BTreeSet};

use super::config::OpenApiConfig;
use super::operation::{operation_entries, ClientOperation};
use crate::error::BuildError;
use crate::openapi::types::{OpenApi, Parameter, PathItem, RefOr, Response, Schema};

pub(super) struct Context<'a> {
    pub(super) openapi: &'a OpenApi,
    pub(super) excluded_schemas: BTreeSet<&'static str>,
    pub(super) excluded_operations: BTreeSet<&'static str>,
    pub(super) serde_types: BTreeMap<&'static str, &'static str>,
}

impl<'a> Context<'a> {
    pub(super) fn new(openapi: &'a OpenApi, config: &'a OpenApiConfig) -> Self {
        Self {
            openapi,
            excluded_schemas: config.excluded_schemas.iter().copied().collect(),
            excluded_operations: config.excluded_operations.iter().copied().collect(),
            serde_types: config.serde_types.iter().copied().collect(),
        }
    }

    pub(super) fn operations(&self) -> Result<Vec<ClientOperation<'a>>, BuildError> {
        let mut output = Vec::new();
        for (path, item) in &self.openapi.paths {
            let item = self.resolve_path_item(item)?;
            for (method, operation) in operation_entries(item) {
                let operation_id = operation.operation_id.as_deref().ok_or_else(|| {
                    BuildError::InvalidInput(format!(
                        "operation {} {path} is missing operationId",
                        method.name()
                    ))
                })?;
                if self.excluded_operations.contains(operation_id) {
                    continue;
                }
                output.push(ClientOperation {
                    method,
                    path,
                    operation_id,
                    path_item: item,
                    operation,
                });
            }
        }
        Ok(output)
    }

    fn resolve_path_item(&self, item: &'a RefOr<PathItem>) -> Result<&'a PathItem, BuildError> {
        match item {
            RefOr::Value(value) => Ok(value),
            RefOr::Ref(reference) => Err(BuildError::InvalidInput(format!(
                "path item references are not supported: {}",
                reference.reference
            ))),
        }
    }

    pub(super) fn resolve_parameter(
        &self,
        parameter: &'a RefOr<Parameter>,
    ) -> Result<&'a Parameter, BuildError> {
        match parameter {
            RefOr::Value(value) => Ok(value),
            RefOr::Ref(reference) => {
                let name = component_name(&reference.reference, "parameters")?;
                self.openapi
                    .components
                    .parameters
                    .get(name)
                    .ok_or_else(|| {
                        BuildError::InvalidInput(format!("unknown parameter reference: {name}"))
                    })
                    .and_then(|parameter| self.resolve_parameter(parameter))
            }
        }
    }

    pub(super) fn resolve_response(
        &self,
        response: &'a RefOr<Response>,
    ) -> Result<&'a Response, BuildError> {
        match response {
            RefOr::Value(value) => Ok(value),
            RefOr::Ref(reference) => {
                let name = component_name(&reference.reference, "responses")?;
                self.openapi
                    .components
                    .responses
                    .get(name)
                    .ok_or_else(|| {
                        BuildError::InvalidInput(format!("unknown response reference: {name}"))
                    })
                    .and_then(|response| self.resolve_response(response))
            }
        }
    }

    pub(super) fn resolve_schema_ref(
        &self,
        reference: &'a str,
    ) -> Result<(&'a str, &'a Schema), BuildError> {
        let name = component_name(reference, "schemas")?;
        let schema =
            self.openapi.components.schemas.get(name).ok_or_else(|| {
                BuildError::InvalidInput(format!("unknown schema reference: {name}"))
            })?;
        Ok((name, schema))
    }
}

fn component_name<'a>(reference: &'a str, component: &str) -> Result<&'a str, BuildError> {
    let prefix = format!("#/components/{component}/");
    reference
        .strip_prefix(&prefix)
        .ok_or_else(|| BuildError::InvalidInput(format!("unsupported reference: {reference}")))
}
