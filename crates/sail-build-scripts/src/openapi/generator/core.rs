use std::path::Path;

use proc_macro2::TokenStream;
use quote::quote;

use super::config::OpenApiConfig;
use crate::error::{BuildError, BuildResult};
use crate::openapi::generator::operation::OperationDefinition;
use crate::openapi::generator::schema::SchemaDefinition;
use crate::openapi::spec::{
    MaybeRef, OpenApi, Parameter, PathItem, RequestBody, Response, Schema, SchemaReference,
    load_spec,
};

pub fn generate_openapi_client(
    spec_path: impl AsRef<Path>,
    output_path: impl AsRef<Path>,
    config: OpenApiConfig,
) -> BuildResult<()> {
    let spec_path = spec_path.as_ref();
    println!("cargo:rerun-if-changed={}", spec_path.display());
    let openapi = load_spec(spec_path)?;
    let tokens = OpenApiGenerator::new(&openapi, config).generate_client()?;
    std::fs::write(output_path, prettyplease::unparse(&syn::parse2(tokens)?))?;
    Ok(())
}

/// OpenAPI code generator.
///
/// The generator supports a deliberately small OpenAPI subset to cover services
/// used in the codebase.
pub(super) struct OpenApiGenerator<'a> {
    pub(super) openapi: &'a OpenApi,
    pub(super) config: OpenApiConfig,
}

impl<'a> OpenApiGenerator<'a> {
    pub(super) fn new(openapi: &'a OpenApi, config: OpenApiConfig) -> Self {
        Self { openapi, config }
    }

    pub(super) fn generate_client(&self) -> BuildResult<TokenStream> {
        let operations = self.operation_definitions()?;
        let methods = operations
            .iter()
            .map(OperationDefinition::method_tokens)
            .collect::<Result<Vec<_>, _>>()?;
        let errors = operations
            .iter()
            .map(OperationDefinition::error_enum_tokens)
            .collect::<Result<Vec<_>, _>>()?;
        let schemas = self
            .schema_definitions()?
            .iter()
            .map(SchemaDefinition::tokens)
            .collect::<BuildResult<Vec<_>>>()?;

        Ok(quote! {
            #[derive(Debug)]
            pub struct ApiClient {
                pub base: String,
                pub client: reqwest::Client,
                pub headers: reqwest::header::HeaderMap,
            }

            #[derive(Debug)]
            pub struct Response<T> {
                pub inner: T,
                pub status: reqwest::StatusCode,
                pub headers: reqwest::header::HeaderMap,
            }

            #[derive(Debug, thiserror::Error)]
            pub enum ApiError<E> {
                #[error("request error: {0}")]
                Request(#[from] reqwest::Error),
                #[error("unknown error: {0}")]
                Unknown(String),
                #[error("response error: status code {}", .0.status)]
                Response(Response<E>),
            }

            fn eager_percent_encode(value: impl AsRef<str>) -> String {
                percent_encoding::utf8_percent_encode(
                    value.as_ref(),
                    percent_encoding::NON_ALPHANUMERIC,
                )
                .to_string()
            }

            impl ApiClient {
                pub fn new(
                    base: impl Into<String>,
                    client: reqwest::Client,
                    headers: reqwest::header::HeaderMap,
                ) -> Self {
                    Self {
                        base: base.into(),
                        client,
                        headers,
                    }
                }

                #(#methods)*
            }

            #(#errors)*
            #(#schemas)*
        })
    }

    pub(super) fn resolve_path_item(
        &self,
        item: &'a MaybeRef<PathItem>,
    ) -> BuildResult<&'a PathItem> {
        match item {
            MaybeRef::Value(value) => Ok(value),
            MaybeRef::Ref(reference) => Err(BuildError::InvalidInput(format!(
                "path item references are not supported: {}",
                reference.reference
            ))),
        }
    }

    pub(super) fn resolve_parameter(
        &self,
        parameter: &'a MaybeRef<Parameter>,
    ) -> BuildResult<&'a Parameter> {
        match parameter {
            MaybeRef::Value(value) => Ok(value),
            MaybeRef::Ref(reference) => {
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
        response: &'a MaybeRef<Response>,
    ) -> BuildResult<&'a Response> {
        match response {
            MaybeRef::Value(value) => Ok(value),
            MaybeRef::Ref(reference) => {
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

    pub(super) fn resolve_request_body(
        &self,
        body: &'a MaybeRef<RequestBody>,
    ) -> BuildResult<&'a RequestBody> {
        match body {
            MaybeRef::Value(value) => Ok(value),
            MaybeRef::Ref(reference) => Err(BuildError::InvalidInput(format!(
                "request body references are not supported: {}",
                reference.reference
            ))),
        }
    }

    pub(super) fn resolve_schema(
        &self,
        schema: &'a MaybeRef<Schema, SchemaReference>,
    ) -> BuildResult<&'a Schema> {
        match schema {
            MaybeRef::Value(value) => Ok(value),
            MaybeRef::Ref(reference) => {
                let (_, schema) = self.resolve_schema_reference(&reference.reference)?;
                Ok(schema)
            }
        }
    }

    pub(super) fn resolve_schema_reference(
        &self,
        reference: &'a str,
    ) -> BuildResult<(&'a str, &'a Schema)> {
        let name = component_name(reference, "schemas")?;
        let schema =
            self.openapi.components.schemas.get(name).ok_or_else(|| {
                BuildError::InvalidInput(format!("unknown schema reference: {name}"))
            })?;
        match schema {
            MaybeRef::Value(value) => Ok((name, value)),
            MaybeRef::Ref(reference) => self.resolve_schema_reference(&reference.reference),
        }
    }
}

fn component_name<'a>(reference: &'a str, component: &str) -> BuildResult<&'a str> {
    let prefix = format!("#/components/{component}/");
    reference
        .strip_prefix(&prefix)
        .ok_or_else(|| BuildError::InvalidInput(format!("unsupported reference: {reference}")))
}
