use proc_macro2::{Literal, TokenStream};
use quote::quote;

use super::core::OpenApiGenerator;
use super::schema::has_schema_type;
use crate::error::{BuildError, BuildResult};
use crate::openapi::spec::{
    MaybeRef, MediaType, ObjectMap, Operation, ParameterLocation, PathItem, RequestBody, Response,
    Schema, SchemaReference, SchemaType,
};
use crate::openapi::utils::docs::doc_attrs;
use crate::openapi::utils::http::{HttpMethod, HttpStatus};
use crate::openapi::utils::name::{RustName, type_name, value_name};
use crate::openapi::utils::types::{RustType, TypePosition};

impl OpenApiGenerator<'_> {
    pub(super) fn operation_definitions(&self) -> BuildResult<Vec<OperationDefinition>> {
        let mut output = Vec::new();
        for (path, item) in &self.openapi.paths {
            let item = self.resolve_path_item(item)?;
            for (method, operation) in Self::collect_operations(item) {
                let operation_id = operation.operation_id.as_deref().ok_or_else(|| {
                    BuildError::InvalidInput(format!(
                        "operation {} {path} is missing operationId",
                        method.name()
                    ))
                })?;
                if self.config.excluded_operations.contains(operation_id) {
                    continue;
                }
                output.push(self.operation_definition(
                    method,
                    path,
                    operation_id,
                    item,
                    operation,
                )?);
            }
        }
        Ok(output)
    }

    fn operation_definition(
        &self,
        method: HttpMethod,
        path: &str,
        operation_id: &str,
        path_item: &PathItem,
        operation: &Operation,
    ) -> BuildResult<OperationDefinition> {
        let parameters = self.collect_parameters(operation_id, path_item, operation)?;
        let body = operation
            .request_body
            .as_ref()
            .map(|body| self.request_body_type(operation_id, body))
            .transpose()?;
        Ok(OperationDefinition {
            method,
            path: path.to_owned(),
            operation_id: operation_id.to_owned(),
            summary: operation.summary.clone(),
            description: operation.description.clone(),
            success_response: self.success_response(operation_id, operation)?,
            error_responses: self.error_responses(operation_id, operation)?,
            parameters,
            body,
        })
    }

    fn collect_operations(path_item: &PathItem) -> Vec<(HttpMethod, &Operation)> {
        [
            (HttpMethod::Get, path_item.get.as_ref()),
            (HttpMethod::Put, path_item.put.as_ref()),
            (HttpMethod::Post, path_item.post.as_ref()),
            (HttpMethod::Delete, path_item.delete.as_ref()),
            (HttpMethod::Options, path_item.options.as_ref()),
            (HttpMethod::Head, path_item.head.as_ref()),
            (HttpMethod::Patch, path_item.patch.as_ref()),
            (HttpMethod::Trace, path_item.trace.as_ref()),
        ]
        .into_iter()
        .filter_map(|(method, operation)| operation.map(|operation| (method, operation)))
        .collect()
    }

    fn collect_parameters(
        &self,
        operation_id: &str,
        path_item: &PathItem,
        operation: &Operation,
    ) -> BuildResult<Vec<OperationParameter>> {
        let mut output = Vec::new();
        for parameter in path_item
            .parameters
            .iter()
            .chain(operation.parameters.iter())
        {
            let parameter = self.resolve_parameter(parameter)?;
            let location = parameter.location.clone();
            let schema = parameter.schema.as_ref().ok_or_else(|| {
                BuildError::InvalidInput(format!(
                    "parameter {} in operation {} is missing a schema",
                    parameter.name, operation_id
                ))
            })?;
            let required = if matches!(location, ParameterLocation::Path) {
                // The OpenAPI standard requires path parameters to have `required: true`.
                // The Iceberg REST catalog specification intentionally marks the `{prefix}` path
                // parameter as `required: false`, so honor an explicit `false` here.
                parameter.required.unwrap_or(true)
            } else {
                parameter.required == Some(true)
            };
            let schema_type = self.parameter_type(schema)?;
            let is_array = schema_type.is_vec();
            let rust_type = if required {
                schema_type.clone()
            } else {
                RustType::Option(Box::new(schema_type))
            };
            let kind = match location {
                ParameterLocation::Path => ParameterKind::Path { required },
                ParameterLocation::Query => ParameterKind::Query {
                    required,
                    array: is_array,
                },
                ParameterLocation::Header => ParameterKind::Header {
                    required,
                    array: is_array,
                },
                ParameterLocation::Cookie => {
                    return Err(BuildError::InvalidInput(format!(
                        "operation {} contains unsupported {:?} parameter {}",
                        operation_id, parameter.location, parameter.name
                    )));
                }
            };
            let identifier = if matches!(kind, ParameterKind::Header { .. }) {
                parameter
                    .name
                    .strip_prefix("X-")
                    .or_else(|| parameter.name.strip_prefix("x-"))
                    .unwrap_or(&parameter.name)
            } else {
                &parameter.name
            };
            output.push(OperationParameter {
                name: parameter.name.clone(),
                identifier: value_name(identifier),
                rust_type,
                kind,
            });
        }
        Ok(output)
    }

    fn request_body_type(
        &self,
        operation_id: &str,
        body: &MaybeRef<RequestBody>,
    ) -> BuildResult<OperationBody> {
        let body = self.resolve_request_body(body)?;
        let schema = json_content_schema(&body.content)?.ok_or_else(|| {
            BuildError::InvalidInput(format!(
                "operation {operation_id} has request body without JSON schema"
            ))
        })?;
        Ok(OperationBody {
            rust_type: self.schema_type(schema, TypePosition::Normal)?,
        })
    }

    fn success_response(
        &self,
        operation_id: &str,
        operation: &Operation,
    ) -> BuildResult<OperationResponse> {
        let mut response = None;
        for (status, item) in &operation.responses {
            let status = HttpStatus::parse(status)?;
            if matches!(status, HttpStatus::Success(_)) {
                if response.is_some() {
                    return Err(BuildError::InvalidInput(format!(
                        "operation {operation_id} defines multiple success responses"
                    )));
                }
                let item = self.resolve_response(item)?;
                response = Some(OperationResponse {
                    status,
                    rust_type: self.response_type(item)?,
                });
            }
        }
        response.ok_or_else(|| {
            BuildError::InvalidInput(format!(
                "operation {operation_id} does not define a success response"
            ))
        })
    }

    fn error_responses(
        &self,
        _operation_id: &str,
        operation: &Operation,
    ) -> BuildResult<Vec<OperationResponse>> {
        let mut output = Vec::new();
        for (status, response) in &operation.responses {
            let status = HttpStatus::parse(status)?;
            if matches!(status, HttpStatus::Success(_)) {
                continue;
            }
            let response = self.resolve_response(response)?;
            output.push(OperationResponse {
                status,
                rust_type: self.response_type(response)?,
            });
        }
        Ok(output)
    }

    fn response_type(&self, response: &Response) -> BuildResult<RustType> {
        json_content_schema(&response.content)?
            .map(|schema| self.schema_type(schema, TypePosition::Normal))
            .transpose()
            .map(|value| value.unwrap_or(RustType::Unit))
    }

    fn parameter_type(&self, schema: &MaybeRef<Schema, SchemaReference>) -> BuildResult<RustType> {
        let schema = self.resolve_schema(schema)?;

        if has_schema_type(schema, SchemaType::Array) {
            let items = schema.items.as_deref().ok_or_else(|| {
                BuildError::InvalidInput("array parameter schema is missing items".to_owned())
            })?;
            let item = self.parameter_type(items)?;
            Ok(RustType::Vec(Box::new(item)))
        } else if has_schema_type(schema, SchemaType::Boolean) {
            Ok(RustType::Bool)
        } else if has_schema_type(schema, SchemaType::Integer) {
            Ok(match schema.format.as_deref() {
                Some("int64") => RustType::I64,
                _ => RustType::I32,
            })
        } else if has_schema_type(schema, SchemaType::Number) {
            Ok(RustType::F64)
        } else if has_schema_type(schema, SchemaType::String) {
            Ok(RustType::String)
        } else {
            Err(BuildError::InvalidInput(
                "path, query, and header parameters must be primitive JSON values".to_owned(),
            ))
        }
    }
}

pub(super) struct OperationDefinition {
    method: HttpMethod,
    path: String,
    operation_id: String,
    summary: Option<String>,
    description: Option<String>,
    parameters: Vec<OperationParameter>,
    body: Option<OperationBody>,
    success_response: OperationResponse,
    error_responses: Vec<OperationResponse>,
}

impl OperationDefinition {
    pub(super) fn method_tokens(&self) -> BuildResult<TokenStream> {
        let method_name = value_name(&self.operation_id);
        let error_type = operation_error_type_name(&self.operation_id);
        let path = generate_path_expression(&self.path, &self.parameters)?;
        let method = RustName::new(self.method.name());
        let docs = doc_attrs(self.summary.as_deref(), self.description.as_deref());

        let arguments = self.parameters.iter().map(|parameter| {
            let ident = &parameter.identifier;
            let rust_type = &parameter.rust_type;
            quote! { #ident: #rust_type }
        });
        let body_argument = self.body.as_ref().map(|body| {
            let rust_type = &body.rust_type;
            quote! { body: #rust_type }
        });
        let query = generate_query(&self.parameters);
        let headers = generate_headers(&self.parameters);
        let has_query = !query.is_empty();
        let has_headers = !headers.is_empty();
        let request = if has_query || has_headers || self.body.is_some() {
            quote! {
                let mut request = self
                    .client
                    .request(reqwest::Method::#method, url)
                    .headers(self.headers.clone());
            }
        } else {
            quote! {
                let request = self
                    .client
                    .request(reqwest::Method::#method, url)
                    .headers(self.headers.clone());
            }
        };
        let query_request = has_query.then(|| {
            quote! {
                if !query.is_empty() {
                    request = request.query(&query);
                }
            }
        });
        let body_request = self.body.is_some().then(|| {
            quote! {
                request = request.json(&body);
            }
        });
        let success_type = &self.success_response.rust_type;
        let success_status = match self.success_response.status {
            HttpStatus::Success(status) => status,
            HttpStatus::ExactError(status) => {
                return Err(BuildError::InvalidInput(format!(
                    "success response must use a success HTTP status, got {status}"
                )));
            }
            HttpStatus::ClientError | HttpStatus::ServerError => {
                return Err(BuildError::InvalidInput(
                    "success response must use an exact HTTP status, got range".to_owned(),
                ));
            }
        };
        let success_body = generate_response_body_expression(&self.success_response.rust_type);
        let error_body = if self.error_responses.is_empty() {
            quote! {
                response.error_for_status_ref()?;
                Err(ApiError::Unknown(format!("unexpected non-error HTTP status: {status}")))
            }
        } else {
            let arms = self
                .error_responses
                .iter()
                .map(|response| generate_error_match_arm(&error_type, response))
                .collect::<BuildResult<Vec<_>>>()?;
            quote! {
                let inner = match status.as_u16() {
                    #(#arms)*
                    _ => {
                        response.error_for_status_ref()?;
                        return Err(ApiError::Unknown(format!(
                            "unexpected non-error HTTP status: {status}"
                        )));
                    }
                };
                Err(ApiError::Response(Response {
                    inner,
                    status,
                    headers,
                }))
            }
        };

        Ok(quote! {
            #(#docs)*
            pub async fn #method_name(
                &self,
                #(#arguments,)*
                #body_argument
            ) -> Result<Response<#success_type>, ApiError<#error_type>> {
                let path = #path;
                let url = format!("{}{}", self.base.trim_end_matches('/'), path);
                #(#query)*
                #request
                #(#headers)*
                #query_request
                #body_request
                let response = request.send().await?;
                let status = response.status();
                let headers = response.headers().clone();
                if status.as_u16() == #success_status {
                    let inner = #success_body;
                    return Ok(Response {
                        inner,
                        status,
                        headers,
                    });
                }
                #error_body
            }
        })
    }

    pub(super) fn error_enum_tokens(&self) -> BuildResult<TokenStream> {
        let name = operation_error_type_name(&self.operation_id);
        let variants = self
            .error_responses
            .iter()
            .map(|response| {
                let variant = type_name(response.status.variant()?);
                if response.rust_type.is_unit() {
                    Ok(quote! { #variant, })
                } else {
                    let rust_type = &response.rust_type;
                    Ok(quote! { #variant(#rust_type), })
                }
            })
            .collect::<BuildResult<Vec<_>>>()?;
        Ok(quote! {
            #[derive(Debug)]
            pub enum #name {
                #(#variants)*
            }
        })
    }
}

fn is_json_media_type(media_type: &str) -> bool {
    media_type == "application/json" || media_type.ends_with("+json")
}

fn json_content_schema(
    content: &ObjectMap<MediaType>,
) -> BuildResult<Option<&MaybeRef<Schema, SchemaReference>>> {
    if content.is_empty() {
        return Ok(None);
    }
    let mut schema = None;
    for (media_type, value) in content {
        if !is_json_media_type(media_type) {
            return Err(BuildError::InvalidInput(format!(
                "unsupported media type: {media_type}"
            )));
        }
        schema = value.schema.as_ref();
    }
    Ok(schema)
}

fn generate_path_expression(
    path: &str,
    parameters: &[OperationParameter],
) -> BuildResult<TokenStream> {
    let mut expression = String::new();
    let mut rest = path;
    let mut arguments = Vec::new();
    while let Some(start) = rest.find('{') {
        let (before, after_start) = rest.split_at(start);
        expression.push_str(before);
        let Some(end) = after_start.find('}') else {
            return Err(BuildError::InvalidInput(format!(
                "path contains unclosed parameter: {path}"
            )));
        };
        let name = &after_start[1..end];
        let parameter = parameters
            .iter()
            .find(|parameter| {
                matches!(parameter.kind, ParameterKind::Path { .. }) && parameter.name == name
            })
            .ok_or_else(|| {
                BuildError::InvalidInput(format!(
                    "path parameter {name} in {path} has no parameter definition"
                ))
            })?;
        let ident = &parameter.identifier;
        let required = parameter.kind.required();
        let optional_segment =
            !required && expression.ends_with('/') && after_start[end + 1..].starts_with('/');
        if optional_segment {
            expression.pop();
        }
        expression.push_str("{}");
        let argument = if !required {
            let format = if optional_segment { "/{}" } else { "{}" };
            let value = if parameter.rust_type.is_string() {
                quote! { value.as_str() }
            } else {
                quote! { value.to_string() }
            };
            quote! {
                #ident
                    .as_ref()
                    .map(|value| format!(#format, eager_percent_encode(#value)))
                    .unwrap_or_default()
            }
        } else {
            let value = if parameter.rust_type.is_string() {
                quote! { #ident.as_str() }
            } else {
                quote! { #ident.to_string() }
            };
            quote! { eager_percent_encode(#value) }
        };
        arguments.push(argument);
        rest = &after_start[end + 1..];
    }
    expression.push_str(rest);
    if arguments.is_empty() {
        Ok(quote! { #expression })
    } else {
        Ok(quote! { format!(#expression, #(#arguments),*) })
    }
}

fn generate_query(parameters: &[OperationParameter]) -> Vec<TokenStream> {
    let mut output = Vec::new();
    if parameters
        .iter()
        .any(|parameter| matches!(parameter.kind, ParameterKind::Query { .. }))
    {
        output.push(quote! { let mut query = Vec::<(String, String)>::new(); });
    }
    for parameter in parameters {
        let ParameterKind::Query { required, array } = parameter.kind else {
            continue;
        };
        let name = &parameter.name;
        let identifier = &parameter.identifier;
        output.push(match (required, array) {
            (true, false) => {
                quote! { query.push((#name.to_owned(), #identifier.to_string())); }
            }
            (false, false) => {
                quote! {
                    if let Some(value) = #identifier.as_ref() {
                        query.push((#name.to_owned(), value.to_string()));
                    }
                }
            }
            (true, true) => {
                quote! {
                    for value in &#identifier {
                        query.push((#name.to_owned(), value.to_string()));
                    }
                }
            }
            (false, true) => {
                quote! {
                    if let Some(values) = #identifier.as_ref() {
                        for value in values {
                            query.push((#name.to_owned(), value.to_string()));
                        }
                    }
                }
            }
        });
    }
    output
}

fn generate_headers(parameters: &[OperationParameter]) -> Vec<TokenStream> {
    let mut output = Vec::new();
    for parameter in parameters {
        let ParameterKind::Header { required, array } = parameter.kind else {
            continue;
        };
        let name = &parameter.name;
        let identifier = &parameter.identifier;
        output.push(match (required, array) {
            (true, false) => {
                quote! { request = request.header(#name, #identifier.to_string()); }
            }
            (false, false) => {
                quote! {
                    if let Some(value) = #identifier.as_ref() {
                        request = request.header(#name, value.to_string());
                    }
                }
            }
            (true, true) => {
                quote! {
                    let value = #identifier
                        .iter()
                        .map(std::string::ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(",");
                    request = request.header(#name, value);
                }
            }
            (false, true) => {
                quote! {
                    if let Some(values) = #identifier.as_ref() {
                        let value = values
                            .iter()
                            .map(std::string::ToString::to_string)
                            .collect::<Vec<_>>()
                            .join(",");
                        request = request.header(#name, value);
                    }
                }
            }
        });
    }
    output
}

fn generate_response_body_expression(rust_type: &RustType) -> TokenStream {
    if rust_type.is_unit() {
        quote! { () }
    } else {
        quote! { response.json::<#rust_type>().await? }
    }
}

fn generate_error_match_arm(
    error_type: &RustName,
    response: &OperationResponse,
) -> BuildResult<TokenStream> {
    let variant = type_name(response.status.variant()?);
    let value = if response.rust_type.is_unit() {
        quote! { #error_type::#variant }
    } else {
        let body = generate_response_body_expression(&response.rust_type);
        quote! { #error_type::#variant(#body) }
    };
    if let Some((start, end)) = response.status.range() {
        let start = Literal::u16_unsuffixed(start);
        let end = Literal::u16_unsuffixed(end);
        Ok(quote! { #start..=#end => #value, })
    } else if let HttpStatus::ExactError(status) = response.status {
        let status = Literal::u16_unsuffixed(status);
        Ok(quote! { #status => #value, })
    } else {
        Err(BuildError::InvalidInput(format!(
            "error response must use an exact HTTP status or range, got {:?}",
            response.status
        )))
    }
}

fn operation_error_type_name(operation_id: &str) -> RustName {
    RustName::new(format!("{}Error", type_name(operation_id)))
}

#[derive(Clone)]
struct OperationParameter {
    name: String,
    identifier: RustName,
    rust_type: RustType,
    kind: ParameterKind,
}

#[derive(Clone, Copy)]
enum ParameterKind {
    Path { required: bool },
    Query { required: bool, array: bool },
    Header { required: bool, array: bool },
}

impl ParameterKind {
    fn required(&self) -> bool {
        match self {
            Self::Path { required }
            | Self::Query { required, .. }
            | Self::Header { required, .. } => *required,
        }
    }
}

struct OperationBody {
    rust_type: RustType,
}

struct OperationResponse {
    status: HttpStatus,
    rust_type: RustType,
}
