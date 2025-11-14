// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{error, fmt};

#[derive(Debug, Clone)]
pub struct ResponseContent<T> {
    pub status: reqwest::StatusCode,
    pub content: String,
    pub entity: Option<T>,
}

#[derive(Debug)]
pub enum Error<T> {
    Reqwest(reqwest::Error),
    Serde(serde_json::Error),
    Io(std::io::Error),
    ResponseError(ResponseContent<T>),
}

impl<T> fmt::Display for Error<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (module, e) = match self {
            Error::Reqwest(e) => ("reqwest", e.to_string()),
            Error::Serde(e) => ("serde", e.to_string()),
            Error::Io(e) => ("IO", e.to_string()),
            Error::ResponseError(e) => ("response", format!("status code {}", e.status)),
        };
        write!(f, "error in {}: {}", module, e)
    }
}

impl<T: fmt::Debug> error::Error for Error<T> {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        Some(match self {
            Error::Reqwest(e) => e,
            Error::Serde(e) => e,
            Error::Io(e) => e,
            Error::ResponseError(_) => return None,
        })
    }
}

impl<T> From<reqwest::Error> for Error<T> {
    fn from(e: reqwest::Error) -> Self {
        Error::Reqwest(e)
    }
}

impl<T> From<serde_json::Error> for Error<T> {
    fn from(e: serde_json::Error) -> Self {
        Error::Serde(e)
    }
}

impl<T> From<std::io::Error> for Error<T> {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

pub fn urlencode<T: AsRef<str>>(s: T) -> String {
    ::url::form_urlencoded::byte_serialize(s.as_ref().as_bytes()).collect()
}

pub fn parse_deep_object(prefix: &str, value: &serde_json::Value) -> Vec<(String, String)> {
    if let serde_json::Value::Object(object) = value {
        let mut params = vec![];

        for (key, value) in object {
            match value {
                serde_json::Value::Object(_) => params.append(&mut parse_deep_object(
                    &format!("{}[{}]", prefix, key),
                    value,
                )),
                serde_json::Value::Array(array) => {
                    for (i, value) in array.iter().enumerate() {
                        params.append(&mut parse_deep_object(
                            &format!("{}[{}][{}]", prefix, key, i),
                            value,
                        ));
                    }
                }
                serde_json::Value::String(s) => {
                    params.push((format!("{}[{}]", prefix, key), s.clone()))
                }
                _ => params.push((format!("{}[{}]", prefix, key), value.to_string())),
            }
        }

        return params;
    }

    unimplemented!("Only objects are supported with style=deepObject")
}

/// Internal use only
/// A content type supported by this client.
#[allow(dead_code)]
enum ContentType {
    Json,
    Text,
    Unsupported(String),
}

impl From<&str> for ContentType {
    fn from(content_type: &str) -> Self {
        if content_type.starts_with("application") && content_type.contains("json") {
            return Self::Json;
        } else if content_type.starts_with("text/plain") {
            return Self::Text;
        } else {
            return Self::Unsupported(content_type.to_string());
        }
    }
}

pub mod catalog_api_api;
pub mod configuration_api_api;
pub mod o_auth2_api_api;

pub mod configuration;

use std::sync::Arc;

pub trait Api {
    fn catalog_api_api(&self) -> &dyn catalog_api_api::CatalogApiApi;
    fn configuration_api_api(&self) -> &dyn configuration_api_api::ConfigurationApiApi;
    fn o_auth2_api_api(&self) -> &dyn o_auth2_api_api::OAuth2ApiApi;
}

pub struct ApiClient {
    catalog_api_api: Box<dyn catalog_api_api::CatalogApiApi>,
    configuration_api_api: Box<dyn configuration_api_api::ConfigurationApiApi>,
    o_auth2_api_api: Box<dyn o_auth2_api_api::OAuth2ApiApi>,
}

impl ApiClient {
    pub fn new(configuration: Arc<configuration::Configuration>) -> Self {
        Self {
            catalog_api_api: Box::new(catalog_api_api::CatalogApiApiClient::new(
                configuration.clone(),
            )),
            configuration_api_api: Box::new(configuration_api_api::ConfigurationApiApiClient::new(
                configuration.clone(),
            )),
            o_auth2_api_api: Box::new(o_auth2_api_api::OAuth2ApiApiClient::new(
                configuration.clone(),
            )),
        }
    }
}

impl Api for ApiClient {
    fn catalog_api_api(&self) -> &dyn catalog_api_api::CatalogApiApi {
        self.catalog_api_api.as_ref()
    }
    fn configuration_api_api(&self) -> &dyn configuration_api_api::ConfigurationApiApi {
        self.configuration_api_api.as_ref()
    }
    fn o_auth2_api_api(&self) -> &dyn o_auth2_api_api::OAuth2ApiApi {
        self.o_auth2_api_api.as_ref()
    }
}
