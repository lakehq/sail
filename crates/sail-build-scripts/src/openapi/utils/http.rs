use proc_macro2::Ident;
use quote::format_ident;

use crate::error::{BuildError, BuildResult};
use crate::openapi::spec::{Operation, PathItem};
use crate::openapi::utils::name::type_ident;

#[derive(Clone, Copy)]
pub enum HttpMethod {
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
    pub fn name(self) -> &'static str {
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

    pub fn ident(self) -> Ident {
        format_ident!("{}", self.name())
    }
}

pub fn operation_entries(path_item: &PathItem) -> Vec<(HttpMethod, &Operation)> {
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

pub fn is_json_media_type(media_type: &str) -> bool {
    media_type == "application/json" || media_type.ends_with("+json")
}

#[derive(Clone, Copy)]
pub enum HttpStatus {
    Success(u16),
    ExactError(u16),
    ClientError,
    ServerError,
}

impl HttpStatus {
    pub fn parse(status: &str) -> BuildResult<Self> {
        if status == "4XX" {
            return Ok(Self::ClientError);
        }
        if status == "5XX" {
            return Ok(Self::ServerError);
        }
        if let Ok(status) = status.parse::<u16>() {
            if (200..300).contains(&status) {
                return Ok(Self::Success(status));
            }
            if (300..600).contains(&status) {
                return Ok(Self::ExactError(status));
            }
        }
        Err(BuildError::InvalidInput(format!(
            "unsupported response status: {status}"
        )))
    }

    pub fn range(self) -> Option<(u16, u16)> {
        match self {
            Self::ClientError => Some((400, 499)),
            Self::ServerError => Some((500, 599)),
            Self::Success(_) | Self::ExactError(_) => None,
        }
    }

    pub fn variant(self) -> BuildResult<Ident> {
        let phrase = match self {
            Self::Success(_) => return Ok(format_ident!("Success")),
            Self::ClientError => return Ok(format_ident!("ClientError")),
            Self::ServerError => return Ok(format_ident!("ServerError")),
            Self::ExactError(status) => match status {
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
                    )));
                }
            },
        };
        Ok(type_ident(phrase))
    }
}
