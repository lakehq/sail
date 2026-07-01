use proc_macro2::{Ident, Literal, TokenStream};
use quote::{format_ident, quote};

use crate::error::BuildError;
use crate::openapi::types::{
    MediaType, ObjectMap, Operation, ParameterLocation, PathItem, RefOr, RequestBody, Response,
    Schema, SchemaType,
};

use super::context::Context;
use super::core::{
    to_snake_case, type_ident, type_name_text, value_ident, RustType, TypePosition, JSON_MEDIA_TYPE,
};
use super::schema::{has_schema_type, integer_type, schema_type};

pub(super) struct ClientOperation<'a> {
    pub(super) method: HttpMethod,
    pub(super) path: &'a str,
    pub(super) operation_id: &'a str,
    pub(super) path_item: &'a PathItem,
    pub(super) operation: &'a Operation,
}

#[derive(Clone, Copy)]
pub(super) enum HttpMethod {
    Get,
    Put,
    Post,
    Delete,
    Options,
    Head,
    Patch,
    Trace,
}

impl HttpMethod {
    pub(super) fn name(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Put => "PUT",
            Self::Post => "POST",
            Self::Delete => "DELETE",
            Self::Options => "OPTIONS",
            Self::Head => "HEAD",
            Self::Patch => "PATCH",
            Self::Trace => "TRACE",
        }
    }

    fn method_ident(self) -> Ident {
        format_ident!("{}", self.name())
    }
}

pub(super) fn operation_entries(path_item: &PathItem) -> Vec<(HttpMethod, &Operation)> {
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

pub(super) fn generate_method(
    context: &Context<'_>,
    operation: &ClientOperation<'_>,
) -> Result<TokenStream, BuildError> {
    let method_name = value_ident(&to_snake_case(operation.operation_id));
    let error_type = operation_error_type(operation.operation_id);
    let parameters = collect_parameters(context, operation)?;
    let body = operation
        .operation
        .request_body
        .as_ref()
        .map(|body| request_body_type(context, operation.operation_id, body))
        .transpose()?;
    let success = success_response(context, operation.operation_id, operation.operation)?;
    let errors = error_responses(context, operation.operation_id, operation.operation)?;
    let path = generate_path_expression(operation.path, &parameters)?;
    let method = operation.method.method_ident();

    let arguments = parameters.iter().map(|parameter| {
        let ident = &parameter.identifier;
        let rust_type = &parameter.rust_type.tokens;
        quote! { #ident: #rust_type }
    });
    let body_argument = body.as_ref().map(|body| {
        let rust_type = &body.rust_type.tokens;
        quote! { body: #rust_type }
    });
    let query = generate_query(&parameters);
    let headers = generate_headers(&parameters);
    let has_query = !query.is_empty();
    let has_headers = !headers.is_empty();
    let request = if has_query || has_headers || body.is_some() {
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
    let body_request = body.is_some().then(|| {
        quote! {
            request = request.json(&body);
        }
    });
    let success_type = &success.rust_type.tokens;
    let success_status = status_code_constant(success.status)?;
    let success_body = response_body_expression(&success.rust_type);
    let docs = doc_attrs(
        operation.operation.summary.as_deref(),
        operation.operation.description.as_deref(),
    );
    let error_body = if errors.is_empty() {
        quote! {
            response.error_for_status_ref().map_err(ApiError::Request)?;
            unreachable!("unexpected non-error HTTP status: {status}")
        }
    } else {
        let arms = errors
            .iter()
            .map(|response| generate_error_match_arm(&error_type, response))
            .collect::<Vec<_>>();
        quote! {
            let inner = match status.as_u16() {
                #(#arms)*
                _ => {
                    response.error_for_status_ref().map_err(ApiError::Request)?;
                    unreachable!("unexpected non-error HTTP status: {status}")
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
            let response = request.send().await.map_err(ApiError::Request)?;
            let status = response.status();
            let headers = response.headers().clone();
            if status == reqwest::StatusCode::#success_status {
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

pub(super) fn generate_error_enum(
    context: &Context<'_>,
    operation: &ClientOperation<'_>,
) -> Result<TokenStream, BuildError> {
    let name = operation_error_type(operation.operation_id);
    let variants = error_responses(context, operation.operation_id, operation.operation)?
        .into_iter()
        .map(|response| {
            let variant = response.variant;
            if response.rust_type.is_unit {
                quote! { #variant, }
            } else {
                let rust_type = response.rust_type.tokens;
                quote! { #variant(#rust_type), }
            }
        })
        .collect::<Vec<_>>();
    Ok(quote! {
        #[derive(Debug)]
        pub enum #name {
            #(#variants)*
        }
    })
}

fn collect_parameters(
    context: &Context<'_>,
    operation: &ClientOperation<'_>,
) -> Result<Vec<ClientParameter>, BuildError> {
    let mut output = Vec::new();
    for parameter in operation
        .path_item
        .parameters
        .iter()
        .chain(operation.operation.parameters.iter())
    {
        let parameter = context.resolve_parameter(parameter)?;
        let location = parameter.location.clone();
        if matches!(location, ParameterLocation::Cookie) {
            return Err(BuildError::InvalidInput(format!(
                "operation {} contains unsupported {:?} parameter {}",
                operation.operation_id, parameter.location, parameter.name
            )));
        }
        let schema = parameter.schema.as_ref().ok_or_else(|| {
            BuildError::InvalidInput(format!(
                "parameter {} in operation {} is missing a schema",
                parameter.name, operation.operation_id
            ))
        })?;
        let required = if matches!(location, ParameterLocation::Path) {
            // The OpenAPI standard requires path parameters to have `required: true`.
            // The Iceberg REST catalog spec intentionally marks the `{prefix}` path
            // parameter as `required: false`, so honor an explicit `false` here.
            parameter.required.unwrap_or(true)
        } else {
            parameter.required == Some(true)
        };
        let schema_type = parameter_schema_type(context, schema)?;
        let rust_type = if required {
            schema_type.rust_type.clone()
        } else {
            schema_type.rust_type.option()
        };
        let query_kind = match (location.clone(), required, schema_type.is_array) {
            (ParameterLocation::Query, true, false) => QueryKind::RequiredScalar,
            (ParameterLocation::Query, false, false) => QueryKind::OptionalScalar,
            (ParameterLocation::Query, true, true) => QueryKind::RequiredArray,
            (ParameterLocation::Query, false, true) => QueryKind::OptionalArray,
            _ => QueryKind::NotQuery,
        };
        let header_kind = match (location.clone(), required, schema_type.is_array) {
            (ParameterLocation::Header, true, false) => HeaderKind::RequiredScalar,
            (ParameterLocation::Header, false, false) => HeaderKind::OptionalScalar,
            (ParameterLocation::Header, true, true) => HeaderKind::RequiredArray,
            (ParameterLocation::Header, false, true) => HeaderKind::OptionalArray,
            _ => HeaderKind::NotHeader,
        };
        let identifier_name = if location == ParameterLocation::Header {
            parameter
                .name
                .strip_prefix("X-")
                .or_else(|| parameter.name.strip_prefix("x-"))
                .unwrap_or(&parameter.name)
        } else {
            &parameter.name
        };
        output.push(ClientParameter {
            name: parameter.name.clone(),
            identifier: value_ident(&to_snake_case(identifier_name)),
            rust_type,
            location,
            required,
            is_string: schema_type.is_string,
            query_kind,
            header_kind,
        });
    }
    Ok(output)
}

fn request_body_type(
    context: &Context<'_>,
    operation_id: &str,
    body: &RefOr<RequestBody>,
) -> Result<BodyType, BuildError> {
    let body = match body {
        RefOr::Value(value) => value,
        RefOr::Ref(reference) => {
            return Err(BuildError::InvalidInput(format!(
                "operation {operation_id} has unsupported request body reference {}",
                reference.reference
            )));
        }
    };
    let schema = json_content_schema(&body.content)?.ok_or_else(|| {
        BuildError::InvalidInput(format!(
            "operation {operation_id} has request body without JSON schema"
        ))
    })?;
    Ok(BodyType {
        rust_type: schema_type(context, schema, TypePosition::Normal)?,
    })
}

fn success_response(
    context: &Context<'_>,
    operation_id: &str,
    operation: &Operation,
) -> Result<ClientResponse, BuildError> {
    let mut responses = Vec::new();
    for (status, response) in &operation.responses {
        if let Some(status) = exact_status(status)? {
            if (200..300).contains(&status) {
                let response = context.resolve_response(response)?;
                responses.push(ClientResponse {
                    status,
                    is_range: false,
                    variant: format_ident!("Success"),
                    rust_type: response_type(context, response)?,
                });
            }
        }
    }
    if responses.is_empty() {
        return Err(BuildError::InvalidInput(format!(
            "operation {operation_id} does not define a success response"
        )));
    }
    if responses.len() > 1 {
        return Err(BuildError::InvalidInput(format!(
            "operation {operation_id} defines multiple success responses"
        )));
    }
    Ok(responses.remove(0))
}

fn error_responses(
    context: &Context<'_>,
    operation_id: &str,
    operation: &Operation,
) -> Result<Vec<ClientResponse>, BuildError> {
    let mut output = Vec::new();
    for (status, response) in &operation.responses {
        if let Some(code) = exact_status(status)? {
            if (200..300).contains(&code) {
                continue;
            }
            let response = context.resolve_response(response)?;
            output.push(ClientResponse {
                status: code,
                is_range: false,
                variant: status_variant(status)?,
                rust_type: response_type(context, response)?,
            });
        } else if is_status_range(status) {
            let response = context.resolve_response(response)?;
            output.push(ClientResponse {
                status: status_range_start(status)?,
                is_range: true,
                variant: status_variant(status)?,
                rust_type: response_type(context, response)?,
            });
        } else {
            return Err(BuildError::InvalidInput(format!(
                "operation {operation_id} has unsupported response status {status}"
            )));
        }
    }
    Ok(output)
}

fn response_type(context: &Context<'_>, response: &Response) -> Result<RustType, BuildError> {
    json_content_schema(&response.content)?
        .map(|schema| schema_type(context, schema, TypePosition::Normal))
        .transpose()
        .map(|value| value.unwrap_or_else(RustType::unit))
}

fn json_content_schema(content: &ObjectMap<MediaType>) -> Result<Option<&Schema>, BuildError> {
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

fn is_json_media_type(media_type: &str) -> bool {
    media_type == JSON_MEDIA_TYPE || media_type.ends_with("+json")
}

fn parameter_schema_type(
    context: &Context<'_>,
    schema: &Schema,
) -> Result<ParameterSchemaType, BuildError> {
    if let Some(reference) = &schema.reference {
        let (_, schema) = context.resolve_schema_ref(reference)?;
        return parameter_schema_type(context, schema);
    }
    if has_schema_type(schema, SchemaType::Array) {
        let items = schema.items.as_deref().ok_or_else(|| {
            BuildError::InvalidInput("array parameter schema is missing items".to_owned())
        })?;
        let item = parameter_schema_type(context, items)?;
        let item_type = item.rust_type.tokens;
        return Ok(ParameterSchemaType {
            rust_type: RustType::new(quote! { Vec<#item_type> }),
            is_array: true,
            is_string: false,
        });
    }
    let (rust_type, is_string) = if has_schema_type(schema, SchemaType::Boolean) {
        (RustType::new(quote! { bool }), false)
    } else if has_schema_type(schema, SchemaType::Integer) {
        let ident = integer_type(schema);
        (RustType::new(quote! { #ident }), false)
    } else if has_schema_type(schema, SchemaType::Number) {
        (RustType::new(quote! { f64 }), false)
    } else if has_schema_type(schema, SchemaType::String) {
        (RustType::new(quote! { String }), true)
    } else {
        return Err(BuildError::InvalidInput(
            "path, query, and header parameters must be primitive JSON values".to_owned(),
        ));
    };
    Ok(ParameterSchemaType {
        rust_type,
        is_array: false,
        is_string,
    })
}

fn generate_path_expression(
    path: &str,
    parameters: &[ClientParameter],
) -> Result<TokenStream, BuildError> {
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
                parameter.location == ParameterLocation::Path && parameter.name == name
            })
            .ok_or_else(|| {
                BuildError::InvalidInput(format!(
                    "path parameter {name} in {path} has no parameter definition"
                ))
            })?;
        let ident = &parameter.identifier;
        let optional_segment = !parameter.required
            && expression.ends_with('/')
            && after_start[end + 1..].starts_with('/');
        if optional_segment {
            expression.pop();
        }
        expression.push_str("{}");
        if !parameter.required && parameter.is_string {
            let prefix = if optional_segment { "/" } else { "" };
            arguments.push(quote! {
                #ident
                    .as_ref()
                    .map(|value| {
                        format!(
                            "{}{}",
                            #prefix,
                            percent_encoding::utf8_percent_encode(
                                value.as_str(),
                                percent_encoding::NON_ALPHANUMERIC,
                            )
                        )
                    })
                    .unwrap_or_default()
            });
        } else if !parameter.required {
            let prefix = if optional_segment { "/" } else { "" };
            arguments.push(quote! {
                #ident
                    .as_ref()
                    .map(|value| {
                        format!(
                            "{}{}",
                            #prefix,
                            percent_encoding::utf8_percent_encode(
                                value.to_string().as_str(),
                                percent_encoding::NON_ALPHANUMERIC,
                            )
                        )
                    })
                    .unwrap_or_default()
            });
        } else if parameter.is_string {
            arguments.push(quote! {
                percent_encoding::utf8_percent_encode(
                    #ident.as_str(),
                    percent_encoding::NON_ALPHANUMERIC,
                ).to_string()
            });
        } else {
            arguments.push(quote! {
                percent_encoding::utf8_percent_encode(
                    #ident.to_string().as_str(),
                    percent_encoding::NON_ALPHANUMERIC,
                ).to_string()
            });
        }
        rest = &after_start[end + 1..];
    }
    expression.push_str(rest);
    if arguments.is_empty() {
        Ok(quote! { #expression })
    } else {
        Ok(quote! { format!(#expression, #(#arguments),*) })
    }
}

fn generate_query(parameters: &[ClientParameter]) -> Vec<TokenStream> {
    let mut output = Vec::new();
    if parameters
        .iter()
        .any(|parameter| parameter.location == ParameterLocation::Query)
    {
        output.push(quote! { let mut query = Vec::<(String, String)>::new(); });
    }
    for parameter in parameters {
        if parameter.location != ParameterLocation::Query {
            continue;
        }
        let name = &parameter.name;
        let identifier = &parameter.identifier;
        output.push(match &parameter.query_kind {
            QueryKind::RequiredScalar => {
                quote! { query.push((#name.to_owned(), #identifier.to_string())); }
            }
            QueryKind::OptionalScalar => {
                quote! {
                    if let Some(value) = #identifier.as_ref() {
                        query.push((#name.to_owned(), value.to_string()));
                    }
                }
            }
            QueryKind::RequiredArray => {
                quote! {
                    for value in &#identifier {
                        query.push((#name.to_owned(), value.to_string()));
                    }
                }
            }
            QueryKind::OptionalArray => {
                quote! {
                    if let Some(values) = #identifier.as_ref() {
                        for value in values {
                            query.push((#name.to_owned(), value.to_string()));
                        }
                    }
                }
            }
            QueryKind::NotQuery => TokenStream::new(),
        });
    }
    output
}

fn generate_headers(parameters: &[ClientParameter]) -> Vec<TokenStream> {
    let mut output = Vec::new();
    for parameter in parameters {
        if parameter.location != ParameterLocation::Header {
            continue;
        }
        let name = &parameter.name;
        let identifier = &parameter.identifier;
        output.push(match &parameter.header_kind {
            HeaderKind::RequiredScalar => {
                quote! { request = request.header(#name, #identifier.to_string()); }
            }
            HeaderKind::OptionalScalar => {
                quote! {
                    if let Some(value) = #identifier.as_ref() {
                        request = request.header(#name, value.to_string());
                    }
                }
            }
            HeaderKind::RequiredArray => {
                quote! {
                    let value = #identifier
                        .iter()
                        .map(std::string::ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(",");
                    request = request.header(#name, value);
                }
            }
            HeaderKind::OptionalArray => {
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
            HeaderKind::NotHeader => TokenStream::new(),
        });
    }
    output
}

fn response_body_expression(rust_type: &RustType) -> TokenStream {
    let tokens = &rust_type.tokens;
    if rust_type.is_unit {
        quote! { () }
    } else {
        quote! { response.json::<#tokens>().await.map_err(ApiError::Request)? }
    }
}

fn generate_error_match_arm(error_type: &Ident, response: &ClientResponse) -> TokenStream {
    let variant = &response.variant;
    let value = if response.rust_type.is_unit {
        quote! { #error_type::#variant }
    } else {
        let body = response_body_expression(&response.rust_type);
        quote! { #error_type::#variant(#body) }
    };
    if response.is_range {
        let start = Literal::u16_unsuffixed(response.status);
        let end = Literal::u16_unsuffixed(response.status + 99);
        quote! { #start..=#end => #value, }
    } else {
        let status = Literal::u16_unsuffixed(response.status);
        quote! { #status => #value, }
    }
}

fn doc_attrs(summary: Option<&str>, description: Option<&str>) -> Vec<TokenStream> {
    let mut lines = Vec::new();
    if let Some(summary) = summary {
        push_doc_lines(summary, &mut lines);
    }
    if summary.is_some() && description.is_some() {
        lines.push(String::new());
    }
    if let Some(description) = description {
        push_doc_lines(description, &mut lines);
    }
    lines
        .into_iter()
        .map(|line| format!(" {line}"))
        .map(|line| quote! { #[doc = #line] })
        .collect()
}

fn push_doc_lines(value: &str, lines: &mut Vec<String>) {
    lines.extend(value.replace('\r', "").lines().map(str::to_owned));
}

fn operation_error_type(operation_id: &str) -> Ident {
    type_ident(&format!("{}Error", type_name_text(operation_id)))
}

fn exact_status(status: &str) -> Result<Option<u16>, BuildError> {
    if is_status_range(status) {
        Ok(None)
    } else {
        status
            .parse::<u16>()
            .map(Some)
            .map_err(|_| BuildError::InvalidInput(format!("unsupported response status: {status}")))
    }
}

fn is_status_range(status: &str) -> bool {
    status.len() == 3 && status.ends_with("XX")
}

fn status_range_start(status: &str) -> Result<u16, BuildError> {
    let prefix = status.strip_suffix("XX").ok_or_else(|| {
        BuildError::InvalidInput(format!("unsupported response status range: {status}"))
    })?;
    prefix.parse::<u16>().map(|value| value * 100).map_err(|_| {
        BuildError::InvalidInput(format!("unsupported response status range: {status}"))
    })
}

fn status_variant(status: &str) -> Result<Ident, BuildError> {
    if status == "4XX" {
        return Ok(format_ident!("ClientError"));
    }
    if status == "5XX" {
        return Ok(format_ident!("ServerError"));
    }
    let status = exact_status(status)?.ok_or_else(|| {
        BuildError::InvalidInput(format!(
            "unsupported response status range for variant: {status}"
        ))
    })?;
    let phrase = match status {
        300 => "MultipleChoices",
        301 => "MovedPermanently",
        302 => "Found",
        303 => "SeeOther",
        304 => "NotModified",
        307 => "TemporaryRedirect",
        308 => "PermanentRedirect",
        400 => "BadRequest",
        401 => "Unauthorized",
        402 => "PaymentRequired",
        403 => "Forbidden",
        404 => "NotFound",
        405 => "MethodNotAllowed",
        406 => "NotAcceptable",
        407 => "ProxyAuthenticationRequired",
        408 => "RequestTimeout",
        409 => "Conflict",
        410 => "Gone",
        411 => "LengthRequired",
        412 => "PreconditionFailed",
        413 => "PayloadTooLarge",
        414 => "UriTooLong",
        415 => "UnsupportedMediaType",
        416 => "RangeNotSatisfiable",
        417 => "ExpectationFailed",
        418 => "ImATeapot",
        419 => "AuthenticationTimeout",
        422 => "UnprocessableEntity",
        423 => "Locked",
        424 => "FailedDependency",
        425 => "TooEarly",
        426 => "UpgradeRequired",
        428 => "PreconditionRequired",
        429 => "TooManyRequests",
        431 => "RequestHeaderFieldsTooLarge",
        451 => "UnavailableForLegalReasons",
        500 => "InternalServerError",
        501 => "NotImplemented",
        502 => "BadGateway",
        503 => "ServiceUnavailable",
        504 => "GatewayTimeout",
        505 => "HttpVersionNotSupported",
        506 => "VariantAlsoNegotiates",
        507 => "InsufficientStorage",
        508 => "LoopDetected",
        510 => "NotExtended",
        511 => "NetworkAuthenticationRequired",
        _ => {
            return Err(BuildError::InvalidInput(format!(
                "unsupported response status: {status}"
            )))
        }
    };
    Ok(type_ident(phrase))
}

fn status_code_constant(status: u16) -> Result<Ident, BuildError> {
    let name = match status {
        200 => "OK",
        201 => "CREATED",
        202 => "ACCEPTED",
        203 => "NON_AUTHORITATIVE_INFORMATION",
        204 => "NO_CONTENT",
        205 => "RESET_CONTENT",
        206 => "PARTIAL_CONTENT",
        _ => {
            return Err(BuildError::InvalidInput(format!(
                "unsupported success response status: {status}"
            )));
        }
    };
    Ok(format_ident!("{name}"))
}

#[derive(Clone)]
struct ClientParameter {
    name: String,
    identifier: Ident,
    rust_type: RustType,
    location: ParameterLocation,
    required: bool,
    is_string: bool,
    query_kind: QueryKind,
    header_kind: HeaderKind,
}

#[derive(Clone)]
enum QueryKind {
    RequiredScalar,
    OptionalScalar,
    RequiredArray,
    OptionalArray,
    NotQuery,
}

#[derive(Clone)]
enum HeaderKind {
    RequiredScalar,
    OptionalScalar,
    RequiredArray,
    OptionalArray,
    NotHeader,
}

struct BodyType {
    rust_type: RustType,
}

struct ClientResponse {
    status: u16,
    is_range: bool,
    variant: Ident,
    rust_type: RustType,
}

struct ParameterSchemaType {
    rust_type: RustType,
    is_array: bool,
    is_string: bool,
}
