// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// [CREDIT]: https://github.com/apache/iceberg-rust/blob/b8467510f8da71b8f180affdef7b1a2d3dd4cfa9/crates/catalog/rest/src/auth/sigv4.rs

//! AWS SigV4 request signing for the REST catalog.
//!
//! Ported from apache/iceberg-rust `crates/catalog/rest/src/auth/sigv4.rs`
//! (PR #3082), adapted to sign `reqwest::Request` directly.

use std::time::SystemTime;

use aws_credential_types::Credentials;
use reqwest::Request;
use sha2::{Digest, Sha256};

#[derive(Debug, thiserror::Error)]
pub enum SigV4Error {
    #[error("invalid x-amz-content-sha256 value")]
    InvalidContentHash(#[source] reqwest::header::InvalidHeaderValue),
    #[error("failed to build SigV4 params")]
    InvalidSigningParams(#[source] aws_sigv4::sign::v4::signing_params::BuildError),
    #[error("request is not signable")]
    UnsignableRequest(#[source] aws_sigv4::http_request::SigningError),
    #[error("SigV4 signing failed")]
    SigningFailed(#[source] aws_sigv4::http_request::SigningError),
    #[error("cannot sign a streaming request body")]
    StreamingBody,
    #[error("cannot sign non-UTF-8 header value for `{0}`")]
    NonUtf8Header(String, #[source] std::str::Utf8Error),
    #[error("cannot strip userinfo from the request URL")]
    StripUserinfo,
    #[error("invalid signed header name")]
    InvalidSignedHeaderName(#[source] reqwest::header::InvalidHeaderName),
    #[error("invalid signed header value")]
    InvalidSignedHeaderValue(#[source] reqwest::header::InvalidHeaderValue),
}

type Result<T> = std::result::Result<T, SigV4Error>;

/// How the payload hash is encoded in the `x-amz-content-sha256` header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PayloadHashMode {
    /// Iceberg Java's style: a base64 header when there is a body, hex when
    /// there is none, and hex in the canonical request. The base64 comes from
    /// how Java configures the AWS SDK, not from SigV4, so a verifier that
    /// trusts the header instead of hashing the body rejects it.
    IcebergRest,
    /// Standard SigV4: hex everywhere.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "kept from the upstream port; only the tests sign in this mode"
        )
    )]
    StandardAws,
}

fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn base64_encode(bytes: &[u8]) -> String {
    base64::engine::Engine::encode(&base64::engine::general_purpose::STANDARD, bytes)
}

/// The content header and canonical payload hash, computed from one digest.
fn payload_hashes(body: Option<&[u8]>, mode: PayloadHashMode) -> (String, String) {
    let digest = Sha256::digest(body.unwrap_or_default());
    let hex = encode_hex(&digest);
    let header = match mode {
        PayloadHashMode::IcebergRest if body.is_some() => base64_encode(&digest),
        _ => hex.clone(),
    };
    (header, hex)
}

/// Signs REST catalog requests the way Iceberg Java's `RESTSigV4AuthSession`
/// does. Carries no credentials, so one signer serves every session.
///
/// Built with [`SigV4Signer::builder`].
#[derive(Clone, Debug)]
pub struct SigV4Signer {
    /// The signing region, e.g. `us-east-1`.
    region: String,
    /// The signing name, e.g. `execute-api`.
    service: String,
    /// How the payload hash is encoded.
    mode: PayloadHashMode,
}

impl SigV4Signer {
    pub fn new(
        region: impl Into<String>,
        service: impl Into<String>,
        mode: PayloadHashMode,
    ) -> Self {
        SigV4Signer {
            region: region.into(),
            service: service.into(),
            mode,
        }
    }

    /// Signs `request` in place. An existing `Authorization` moves to
    /// `Original-Authorization`, and a caller's `x-amz-date`,
    /// `x-amz-content-sha256` or `x-amz-security-token` that the signer
    /// overwrites moves to `Original-<name>` (Java keeps a caller's content
    /// hash when there is a body). Userinfo leaves the URL, and a `+` in the
    /// query becomes `%20`, so write a literal plus as `%2B`.
    ///
    /// Uses `credentials` as given and never refreshes them: resolve temporary
    /// ones from their provider before each call.
    ///
    /// On error, including for a streaming body or a non-UTF-8 header, the
    /// request is left unchanged.
    ///
    /// Send the result through a client that does not follow redirects: a
    /// redirect replays the signature, and across hosts reqwest drops
    /// `Authorization` but keeps `Original-Authorization`.
    ///
    /// `aws_sigv4` traces requests without redacting `Original-Authorization`,
    /// but `CompositeLogger` drops those records.
    pub fn sign(&self, request: &mut Request, credentials: &Credentials) -> Result<()> {
        self.sign_at(request, credentials, SystemTime::now())
    }

    fn sign_at(
        &self,
        request: &mut Request,
        credentials: &Credentials,
        now: SystemTime,
    ) -> Result<()> {
        let headers = request.headers().clone();
        let url = request.url().clone();
        let signed = self.sign_in_place(request, credentials, now);
        if signed.is_err() {
            *request.headers_mut() = headers;
            *request.url_mut() = url;
        }
        signed
    }

    fn sign_in_place(
        &self,
        request: &mut reqwest::Request,
        credentials: &Credentials,
        now: SystemTime,
    ) -> Result<()> {
        use aws_sigv4::http_request::{SignableBody, SignableRequest, sign};
        use aws_sigv4::sign::v4;

        // Before the content hash is displaced, which takes it out of signing.
        signable_headers(request).try_for_each(|header| header.map(|_| ()))?;
        let (content_header, payload_hash) = payload_hashes(signable_body(request)?, self.mode);

        convert_headers(request);

        // Relocated after signing, so the `Original-` copy is not signed.
        let displaced_content_hash: Vec<_> = request
            .headers()
            .get_all(CONTENT_SHA256)
            .iter()
            .filter(|v| v.as_bytes() != content_header.as_bytes())
            .cloned()
            .collect();
        let content_value = content_header
            .parse()
            .map_err(SigV4Error::InvalidContentHash)?;
        request.headers_mut().insert(CONTENT_SHA256, content_value);

        rewrite_url_for_signing(request)?;

        let identity = credentials.clone().into();
        let params = v4::SigningParams::builder()
            .identity(&identity)
            .region(&self.region)
            .name(&self.service)
            .time(now)
            .settings(signing_settings())
            .build()
            .map_err(SigV4Error::InvalidSigningParams)?
            .into();

        let headers = signable_headers(request).collect::<Result<Vec<_>>>()?;
        let signable = SignableRequest::new(
            request.method().as_str(),
            request.url().as_str(),
            headers.into_iter(),
            SignableBody::Precomputed(payload_hash),
        )
        .map_err(SigV4Error::UnsignableRequest)?;

        let signed = sign(signable, &params);
        let (instructions, _signature) = signed.map_err(SigV4Error::SigningFailed)?.into_parts();

        update_request_headers(request, instructions, displaced_content_hash)
    }
}

/// The body to sign; as in Java, an absent body and an empty one differ.
fn signable_body(request: &reqwest::Request) -> Result<Option<&[u8]>> {
    match request.body().map(|body| body.as_bytes()) {
        None => Ok(None),
        Some(Some(bytes)) => Ok(Some(bytes)),
        Some(None) => Err(SigV4Error::StreamingBody),
    }
}

/// The headers to sign. A non-UTF-8 one is an error: skipping it would send
/// it unsigned.
fn signable_headers(request: &reqwest::Request) -> impl Iterator<Item = Result<(&str, &str)>> {
    request.headers().iter().map(|(n, v)| {
        let v = std::str::from_utf8(v.as_bytes())
            .map_err(|e| SigV4Error::NonUtf8Header(n.as_str().into(), e))?;
        Ok((n.as_str(), v))
    })
}

/// Drops userinfo, which the wire `Host` never carries, and rewrites `+` in the
/// query as `%20`: a space to AWS and Java either way, but unambiguous to any
/// verifier.
fn rewrite_url_for_signing(request: &mut reqwest::Request) -> Result<()> {
    if !request.url().username().is_empty() || request.url().password().is_some() {
        let url = request.url_mut();
        url.set_username("")
            .and_then(|()| url.set_password(None))
            .map_err(|()| SigV4Error::StripUserinfo)?;
    }
    if let Some(query) = request.url().query().filter(|q| q.contains('+')) {
        let unambiguous = query.replace('+', "%20");
        request.url_mut().set_query(Some(&unambiguous));
    }
    Ok(())
}

/// `Aws4Signer`'s settings: normalized double-encoded path, and Java's ignore
/// list.
fn signing_settings() -> aws_sigv4::http_request::SigningSettings {
    use aws_sigv4::http_request::{
        PayloadChecksumKind, PercentEncodingMode, SigningSettings, UriPathNormalizationMode,
    };

    let mut settings = SigningSettings::default();
    settings.percent_encoding_mode = PercentEncodingMode::Double;
    settings.uri_path_normalization_mode = UriPathNormalizationMode::Enabled;
    // We set and relocate the content hash ourselves, outside the instructions.
    settings.payload_checksum_kind = PayloadChecksumKind::NoHeader;
    let mut excluded = settings.excluded_headers.take().unwrap_or_default();
    excluded.extend([
        // Java's list, spelled out rather than left to the crate's defaults.
        "connection".into(),
        "expect".into(),
        "transfer-encoding".into(),
        "user-agent".into(),
        "x-amzn-trace-id".into(),
        // Not Java's, but proxies append to it too.
        "x-forwarded-for".into(),
        // Relocation appends to these after signing.
        "original-x-amz-date".into(),
        "original-x-amz-content-sha256".into(),
        "original-x-amz-security-token".into(),
    ]);
    settings.excluded_headers = Some(excluded);
    settings
}

/// Java's `convertHeaders`: moves `Authorization` aside before signing, so the
/// moved copy is signed.
fn convert_headers(request: &mut reqwest::Request) {
    let displaced: Vec<_> = request
        .headers()
        .get_all(reqwest::header::AUTHORIZATION)
        .iter()
        .cloned()
        .collect();
    if displaced.is_empty() {
        return;
    }
    request.headers_mut().remove(reqwest::header::AUTHORIZATION);
    for mut value in displaced {
        value.set_sensitive(true);
        request.headers_mut().append(RELOCATED_AUTHORIZATION, value);
    }
}

/// Java's `updateRequestHeaders`: installs the signed headers, moving
/// conflicting caller values aside.
fn update_request_headers(
    request: &mut reqwest::Request,
    instructions: aws_sigv4::http_request::SigningInstructions,
    displaced_content_hash: Vec<reqwest::header::HeaderValue>,
) -> Result<()> {
    let (signed_headers, _params) = instructions.into_parts();
    let h = request.headers_mut();
    for mut value in displaced_content_hash {
        // The original may carry a credential.
        value.set_sensitive(true);
        h.append(RELOCATED_CONTENT_SHA256, value);
    }
    for header in signed_headers {
        let name: reqwest::header::HeaderName = header
            .name()
            .parse()
            .map_err(SigV4Error::InvalidSignedHeaderName)?;
        if let Some(relocated) = relocated_name(name.as_str()) {
            relocate_conflicting(h, name.as_str(), header.value(), relocated);
        }
        let mut value: reqwest::header::HeaderValue = header
            .value()
            .parse()
            .map_err(SigV4Error::InvalidSignedHeaderValue)?;
        if name == reqwest::header::AUTHORIZATION || name == SECURITY_TOKEN {
            value.set_sensitive(true);
        }
        h.insert(name, value);
    }
    Ok(())
}

const CONTENT_SHA256: reqwest::header::HeaderName =
    reqwest::header::HeaderName::from_static("x-amz-content-sha256");
const AMZ_DATE: reqwest::header::HeaderName =
    reqwest::header::HeaderName::from_static("x-amz-date");
const SECURITY_TOKEN: reqwest::header::HeaderName =
    reqwest::header::HeaderName::from_static("x-amz-security-token");

const RELOCATED_AUTHORIZATION: reqwest::header::HeaderName =
    reqwest::header::HeaderName::from_static("original-authorization");
const RELOCATED_AMZ_DATE: reqwest::header::HeaderName =
    reqwest::header::HeaderName::from_static("original-x-amz-date");
const RELOCATED_CONTENT_SHA256: reqwest::header::HeaderName =
    reqwest::header::HeaderName::from_static("original-x-amz-content-sha256");
const RELOCATED_SECURITY_TOKEN: reqwest::header::HeaderName =
    reqwest::header::HeaderName::from_static("original-x-amz-security-token");

/// The `Original-<name>` counterpart of a header the signer generates.
fn relocated_name(name: &str) -> Option<reqwest::header::HeaderName> {
    match name {
        n if n == AMZ_DATE => Some(RELOCATED_AMZ_DATE),
        n if n == SECURITY_TOKEN => Some(RELOCATED_SECURITY_TOKEN),
        _ => None,
    }
}

/// Moves `name`'s values that differ from `signed` to `relocated`.
fn relocate_conflicting(
    headers: &mut reqwest::header::HeaderMap,
    name: &str,
    signed: &str,
    relocated: reqwest::header::HeaderName,
) {
    let conflicting: Vec<_> = headers
        .get_all(name)
        .iter()
        .filter(|value| value.as_bytes() != signed.as_bytes())
        .cloned()
        .collect();
    for mut value in conflicting {
        // It may carry a credential, e.g. a session token.
        value.set_sensitive(true);
        headers.append(relocated.clone(), value);
    }
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used)]
    use std::time::SystemTime;

    use chrono::{TimeZone, Utc};

    use super::*;

    const EMPTY_HEX: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    #[test]
    fn signing_rewrites_an_ambiguous_plus_out_of_the_query() {
        // reqwest writes a space as `+`; signing makes it `%20`.
        let mut request = reqwest::Client::new()
            .get("https://rest.example.com/v1/namespaces")
            .query(&[("parent", "my ns")])
            .build()
            .unwrap();
        assert!(request.url().query().unwrap().contains("my+ns"));

        let signer = test_signer(PayloadHashMode::StandardAws);
        let now = test_time();
        signer
            .sign_at(&mut request, &test_credentials(), now)
            .unwrap();

        let query = request.url().query().unwrap();
        assert!(!query.contains('+'), "{query}");
        assert!(query.contains("my%20ns"), "{query}");
        assert_signature_is(
            &request,
            "b7bb5a323a1ce0ace18454171084deef2dac44c933c3949771fc70179d3cce2b",
        );
    }

    #[test]
    fn payload_hashes_iceberg_mode() {
        let v = payload_hashes(Some(b"hello"), PayloadHashMode::IcebergRest).0;
        assert_eq!(v, "LPJNul+wow4m6DsqxbninhsWHlwfp0JecwQzYpOLmCQ=");
        let e = payload_hashes(None, PayloadHashMode::IcebergRest).0;
        assert_eq!(e, EMPTY_HEX);
    }

    /// As in Java, an empty body is hashed, unlike an absent one.
    #[test]
    fn signing_separates_an_empty_body_from_an_absent_one() {
        let signer = test_signer(PayloadHashMode::IcebergRest);
        let now = test_time();
        let hash_of = |builder: reqwest::RequestBuilder| {
            let mut req = builder.build().unwrap();
            signer.sign_at(&mut req, &test_credentials(), now).unwrap();
            req.headers()
                .get("x-amz-content-sha256")
                .unwrap()
                .to_str()
                .unwrap()
                .to_string()
        };

        let client = reqwest::Client::new();
        let url = "https://rest.example.com/v1/namespaces";
        assert_eq!(
            hash_of(client.post(url).body("")),
            "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU="
        );
        assert_eq!(hash_of(client.post(url)), EMPTY_HEX);
    }

    #[test]
    fn payload_hashes_standard_mode() {
        let v = payload_hashes(Some(b"hello"), PayloadHashMode::StandardAws).0;
        assert_eq!(
            v,
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }

    /// Pins a signature this crate produced; `signatures_match_iceberg_java`
    /// checks against Java.
    fn assert_signature_is(req: &reqwest::Request, expected: &str) {
        let auth = req
            .headers()
            .get("authorization")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(auth.ends_with(&format!("Signature={expected}")), "{auth}");
    }

    fn test_signer(mode: PayloadHashMode) -> SigV4Signer {
        SigV4Signer::new("us-east-1", "execute-api", mode)
    }

    fn test_credentials() -> Credentials {
        Credentials::new("ak", "sk", None::<String>, None, "test")
    }

    fn example_credentials(session_token: Option<&str>) -> Credentials {
        Credentials::new(
            "AKIDEXAMPLE",
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            session_token.map(str::to_string),
            None,
            "test",
        )
    }

    fn test_time() -> SystemTime {
        Utc.with_ymd_and_hms(2015, 8, 30, 12, 36, 0).unwrap().into()
    }

    fn signed_headers(req: &reqwest::Request) -> Vec<String> {
        let auth = req
            .headers()
            .get("authorization")
            .unwrap()
            .to_str()
            .unwrap();
        let signed = auth.split("SignedHeaders=").nth(1).unwrap();
        let signed = signed.split(',').next().unwrap();
        signed.split(';').map(str::to_string).collect()
    }

    /// Every header value as `(name, value)`, sorted.
    fn header_list(req: &reqwest::Request) -> Vec<(String, String)> {
        let mut headers: Vec<_> = req
            .headers()
            .iter()
            .map(|(n, v)| (n.to_string(), v.to_str().unwrap().to_string()))
            .collect();
        headers.sort();
        headers
    }

    fn sorted_pairs(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        let mut pairs: Vec<_> = pairs
            .iter()
            .map(|(n, v)| (n.to_string(), v.to_string()))
            .collect();
        pairs.sort();
        pairs
    }

    #[test]
    fn a_non_utf8_header_is_rejected_without_changing_the_request() {
        let signer = test_signer(PayloadHashMode::IcebergRest);
        for name in ["x-amz-meta-tenant", "authorization", "x-amz-content-sha256"] {
            let mut req = reqwest::Request::new(
                reqwest::Method::GET,
                "https://user:pw@rest.example.com/v1/config?warehouse=my+catalog"
                    .parse()
                    .unwrap(),
            );
            req.headers_mut()
                .insert("authorization", "Bearer delegate-token".parse().unwrap());
            req.headers_mut()
                .insert("x-amz-content-sha256", "caller-hash".parse().unwrap());
            req.headers_mut().insert(
                name,
                reqwest::header::HeaderValue::from_bytes(b"acme\xfa").unwrap(),
            );
            let headers = req.headers().clone();
            let url = req.url().clone();

            let err = signer
                .sign_at(&mut req, &test_credentials(), test_time())
                .unwrap_err();
            assert!(
                matches!(&err, SigV4Error::NonUtf8Header(h, _) if h == name),
                "{name}: {err}"
            );
            assert_eq!(req.headers(), &headers, "{name}");
            assert_eq!(req.url(), &url, "{name}");
        }
    }

    #[test]
    fn a_non_ascii_utf8_header_is_signed() {
        let signer = test_signer(PayloadHashMode::StandardAws);
        let mut req = reqwest::Request::new(
            reqwest::Method::GET,
            "https://rest.example.com/v1/config".parse().unwrap(),
        );
        req.headers_mut().insert(
            "x-tenant",
            reqwest::header::HeaderValue::from_bytes("Zürich".as_bytes()).unwrap(),
        );

        signer
            .sign_at(&mut req, &test_credentials(), test_time())
            .unwrap();

        assert_eq!(
            signed_headers(&req),
            ["host", "x-amz-content-sha256", "x-amz-date", "x-tenant"]
        );
    }

    #[test]
    fn a_signing_failure_after_relocation_restores_the_request() {
        let signer = test_signer(PayloadHashMode::IcebergRest);
        // Longer than `http::Uri` accepts, so signing fails after the headers
        // and query were rewritten.
        let url = format!(
            "https://rest.example.com/v1/config?warehouse=my+catalog&pad={}",
            "a".repeat(70_000)
        );
        let mut req = reqwest::Client::new()
            .get(url)
            .header("authorization", "Bearer delegate-token")
            .header("x-amz-content-sha256", "caller-hash")
            .build()
            .unwrap();
        let headers = req.headers().clone();
        let url = req.url().clone();

        let err = signer
            .sign_at(&mut req, &test_credentials(), test_time())
            .unwrap_err();

        assert!(matches!(&err, SigV4Error::UnsignableRequest(_)), "{err}");
        assert_eq!(req.headers(), &headers);
        assert_eq!(req.url(), &url);
    }

    /// Relocation appends to a caller's `Original-x-amz-*` after signing, so it
    /// must stay unsigned.
    #[test]
    fn a_caller_supplied_relocation_header_is_not_signed() {
        let signer = test_signer(PayloadHashMode::StandardAws);
        let mut req = reqwest::Client::new()
            .get("https://rest.example.com/v1/config")
            .header("x-amz-content-sha256", "caller-hash")
            .header("original-x-amz-content-sha256", "previous")
            .build()
            .unwrap();

        signer
            .sign_at(&mut req, &test_credentials(), test_time())
            .unwrap();

        assert_eq!(
            signed_headers(&req),
            ["host", "x-amz-content-sha256", "x-amz-date"]
        );
        let relocated: Vec<_> = req
            .headers()
            .get_all("original-x-amz-content-sha256")
            .iter()
            .map(|v| v.to_str().unwrap())
            .collect();
        assert_eq!(relocated, ["previous", "caller-hash"]);
    }

    #[test]
    fn userinfo_is_stripped_before_signing() {
        // A hand-built request can carry userinfo; the wire `Host` never does.
        let signer = test_signer(PayloadHashMode::StandardAws);
        let mut req = reqwest::Request::new(
            reqwest::Method::GET,
            "https://user:pw@rest.example.com/v1/config"
                .parse()
                .unwrap(),
        );
        assert_eq!(req.url().username(), "user");
        let now = test_time();

        signer.sign_at(&mut req, &test_credentials(), now).unwrap();

        assert_eq!(req.url().username(), "");
        assert_eq!(req.url().password(), None);
        let mut plain = reqwest::Request::new(
            reqwest::Method::GET,
            "https://rest.example.com/v1/config".parse().unwrap(),
        );
        signer
            .sign_at(&mut plain, &test_credentials(), now)
            .unwrap();
        assert_eq!(
            req.headers().get("authorization"),
            plain.headers().get("authorization"),
        );
    }

    #[test]
    fn a_doubled_slash_in_the_path_is_normalized() {
        // A trailing slash on the catalog URI gives `//v1/...`, which
        // `Aws4Signer` collapses.
        let signer = test_signer(PayloadHashMode::StandardAws);
        let mut req = reqwest::Request::new(
            reqwest::Method::GET,
            "https://rest.example.com//v1//config".parse().unwrap(),
        );
        let now = test_time();

        signer.sign_at(&mut req, &test_credentials(), now).unwrap();

        let mut plain = reqwest::Request::new(
            reqwest::Method::GET,
            "https://rest.example.com/v1/config".parse().unwrap(),
        );
        signer
            .sign_at(&mut plain, &test_credentials(), now)
            .unwrap();
        assert_eq!(
            req.headers().get("authorization"),
            plain.headers().get("authorization"),
        );
    }

    #[test]
    fn caller_headers_the_signer_overwrites_are_relocated() {
        // As in Java, conflicting caller values move to `Original-<name>`.
        let creds = Credentials::new(
            "ak".to_string(),
            "sk".to_string(),
            Some("signer-token".to_string()),
            None,
            "test",
        );
        let signer = test_signer(PayloadHashMode::StandardAws);
        let mut req = reqwest::Client::new()
            .get("https://rest.example.com/v1/config")
            .header("authorization", "Bearer caller-token")
            .header("x-amz-date", "19700101T000000Z")
            .header("x-amz-security-token", "caller-session")
            .header("x-amz-content-sha256", "caller-hash")
            .build()
            .unwrap();

        signer.sign_at(&mut req, &creds, test_time()).unwrap();

        assert_eq!(
            header_list(&req),
            sorted_pairs(&[
                (
                    "authorization",
                    "AWS4-HMAC-SHA256 Credential=ak/20150830/us-east-1/execute-api/aws4_request, \
                     SignedHeaders=host;original-authorization;x-amz-content-sha256;x-amz-date;\
                     x-amz-security-token, \
                     Signature=87560cb735277b284a547d63f57cfe3a721f9bce0b08a9402a58d754d2ed707c"
                ),
                ("original-authorization", "Bearer caller-token"),
                ("original-x-amz-content-sha256", "caller-hash"),
                ("original-x-amz-date", "19700101T000000Z"),
                ("original-x-amz-security-token", "caller-session"),
                ("x-amz-content-sha256", EMPTY_HEX),
                ("x-amz-date", "20150830T123600Z"),
                ("x-amz-security-token", "signer-token"),
            ])
        );
        // Relocated originals may be credentials themselves.
        for relocated in [RELOCATED_SECURITY_TOKEN, RELOCATED_CONTENT_SHA256] {
            assert!(req.headers().get(relocated).unwrap().is_sensitive());
        }
    }

    #[test]
    fn an_existing_authorization_is_never_signed() {
        // The signer replaces `authorization`, and a proxy may rewrite
        // `user-agent`, so neither is signed.
        let signer = test_signer(PayloadHashMode::StandardAws);
        let mut req = reqwest::Client::new()
            .get("https://rest.example.com/v1/config")
            .header("authorization", "Bearer caller-token")
            .header("user-agent", "example/1.0")
            .build()
            .unwrap();

        signer
            .sign_at(&mut req, &test_credentials(), test_time())
            .unwrap();

        assert_eq!(
            signed_headers(&req),
            [
                "host",
                "original-authorization",
                "x-amz-content-sha256",
                "x-amz-date"
            ]
        );
    }

    /// As in Java, every `Authorization` value is relocated, and stays redacted.
    #[test]
    fn every_repeated_authorization_is_relocated_and_kept_sensitive() {
        let signer = test_signer(PayloadHashMode::StandardAws);
        let mut req = reqwest::Client::new()
            .get("https://rest.example.com/v1/config")
            .header("authorization", "Bearer first")
            .header("authorization", "Bearer second")
            .build()
            .unwrap();

        signer
            .sign_at(&mut req, &test_credentials(), test_time())
            .unwrap();

        let relocated: Vec<_> = req
            .headers()
            .get_all(RELOCATED_AUTHORIZATION)
            .iter()
            .collect();
        assert_eq!(relocated.len(), 2, "{relocated:?}");
        assert_eq!(relocated[0], "Bearer first");
        assert_eq!(relocated[1], "Bearer second");
        assert!(relocated.iter().all(|v| v.is_sensitive()), "{relocated:?}");
    }

    #[test]
    fn hop_by_hop_headers_are_not_signed() {
        // A proxy may drop or rewrite these, so they are not signed.
        let signer = test_signer(PayloadHashMode::StandardAws);
        let mut req = reqwest::Client::new()
            .get("https://rest.example.com/v1/config")
            .header("expect", "100-continue")
            .header("connection", "keep-alive")
            .header("x-forwarded-for", "203.0.113.7")
            .header("x-tenant", "acme")
            .build()
            .unwrap();
        let now = test_time();

        signer.sign_at(&mut req, &test_credentials(), now).unwrap();

        assert_eq!(
            signed_headers(&req),
            ["host", "x-amz-content-sha256", "x-amz-date", "x-tenant"]
        );
        assert_signature_is(
            &req,
            "f938221412ed6b55cf3db380ce6ded476419ad3d7db4c76d932031c98465ce79",
        );
    }

    #[test]
    fn signed_credentials_are_marked_sensitive() {
        // Both carry a credential, so `Debug` must not print them.
        let creds = Credentials::new(
            "ak".to_string(),
            "sk".to_string(),
            Some("session-token".to_string()),
            None,
            "test",
        );
        let signer = test_signer(PayloadHashMode::StandardAws);
        let mut req = reqwest::Client::new()
            .get("https://rest.example.com/v1/config")
            .build()
            .unwrap();

        signer.sign_at(&mut req, &creds, test_time()).unwrap();

        assert!(req.headers().get("authorization").unwrap().is_sensitive());
        assert!(
            req.headers()
                .get("x-amz-security-token")
                .unwrap()
                .is_sensitive()
        );
        let debug = format!("{:?}", req.headers());
        assert!(!debug.contains("session-token"), "{debug}");
    }

    #[test]
    fn signs_with_a_non_default_service_and_session_token() {
        // As a non-AWS catalog might vend: its own signing name, and STS
        // credentials.
        let creds = Credentials::new(
            "STS.EXAMPLEACCESSKEYID",
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            Some("example-session-token".to_string()),
            None,
            "test",
        );
        let signer = SigV4Signer::new("us-east-1", "custom-service", PayloadHashMode::IcebergRest);
        let mut req = reqwest::Client::new()
            .get("https://catalog.example.com/v1/config?warehouse=my-catalog")
            .build()
            .unwrap();
        let now = Utc.with_ymd_and_hms(2026, 8, 26, 12, 0, 0).unwrap();

        signer.sign_at(&mut req, &creds, now.into()).unwrap();

        assert_signature_is(
            &req,
            "6b7065e5f44da4f5c3654126b8d5fe29599905afb6b98f4de65b6ec6e1be783f",
        );
        let auth = req
            .headers()
            .get("authorization")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(
            auth.contains("/us-east-1/custom-service/aws4_request"),
            "{auth}"
        );
        assert_eq!(
            req.headers().get("x-amz-security-token").unwrap(),
            "example-session-token"
        );
    }

    #[test]
    fn signs_request_iceberg_mode() {
        let creds = example_credentials(Some("SESSIONTOKEN"));
        let signer = SigV4Signer::new("us-east-1", "glue", PayloadHashMode::IcebergRest);
        let client = reqwest::Client::new();
        let mut req = client
            .post("https://rest.example.com/v1/namespaces")
            .body("{}")
            .build()
            .unwrap();

        signer.sign_at(&mut req, &creds, test_time()).unwrap();

        assert_eq!(
            header_list(&req),
            sorted_pairs(&[
                (
                    "authorization",
                    "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/glue/aws4_request, \
                     SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-security-token, \
                     Signature=effad6acde583dd14ba7aff52b2b83776a54421c010fe82067f166819057cb32"
                ),
                (
                    "x-amz-content-sha256",
                    "RBNvo1WzZ4oRRq0W9+hknpT7T8If536DEMBg9hyq/4o="
                ),
                ("x-amz-date", "20150830T123600Z"),
                ("x-amz-security-token", "SESSIONTOKEN"),
            ])
        );
    }

    /// An empty body hashes to the hex constant, and caller headers are signed
    /// (Java's `authenticateWithoutBody`).
    #[test]
    fn signs_empty_body_and_all_headers() {
        let creds = example_credentials(None);
        let signer = SigV4Signer::new("us-east-1", "glue", PayloadHashMode::IcebergRest);
        let client = reqwest::Client::new();
        let mut req = client
            .get("https://rest.example.com/v1/config")
            .header("content-type", "application/json")
            .header("content-encoding", "gzip")
            .build()
            .unwrap();

        signer.sign_at(&mut req, &creds, test_time()).unwrap();

        assert_eq!(
            header_list(&req),
            sorted_pairs(&[
                (
                    "authorization",
                    "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/glue/aws4_request, \
                     SignedHeaders=content-encoding;content-type;host;x-amz-content-sha256;x-amz-date, \
                     Signature=eaef7eb88d9cd810031684748671d8a3c9394ea5168622a212ed041b670b9777"
                ),
                ("content-encoding", "gzip"),
                ("content-type", "application/json"),
                ("x-amz-content-sha256", EMPTY_HEX),
                ("x-amz-date", "20150830T123600Z"),
            ])
        );
    }

    #[test]
    fn iceberg_mode_signs_the_hex_payload_hash_not_the_base64_header() {
        // Needs a body: without one the header is the hex constant too.
        let creds = example_credentials(None);
        let signer = test_signer(PayloadHashMode::IcebergRest);
        let body = br#"{"namespace":["ns"]}"#;
        let mut req = reqwest::Client::new()
            .post("https://rest.example.com/v1/namespaces")
            .body(body.to_vec())
            .build()
            .unwrap();
        let now = test_time();

        signer.sign_at(&mut req, &creds, now).unwrap();

        assert_eq!(
            req.headers().get("x-amz-content-sha256").unwrap(),
            payload_hashes(Some(body), PayloadHashMode::IcebergRest)
                .0
                .as_str()
        );
        assert_ne!(
            req.headers().get("x-amz-content-sha256").unwrap(),
            encode_hex(&Sha256::digest(body)).as_str()
        );
        assert_signature_is(
            &req,
            "c68682c26cab6a781256f83b0076f50014f4922c3907f4ff09c204a74d61fc1d",
        );
    }

    /// What Iceberg Java's `RESTSigV4AuthSession` (iceberg-aws 1.10.1) sent for
    /// these requests, given these credentials as `rest.*` properties.
    #[test]
    fn signatures_match_iceberg_java() {
        let creds = example_credentials(Some("example-session-token"));
        let signer = test_signer(PayloadHashMode::IcebergRest);
        let now = Utc.with_ymd_and_hms(2026, 10, 2, 11, 40, 30).unwrap();
        let client = reqwest::Client::new();

        // A body, so the header carries base64, and a token to relocate.
        let mut post = client
            .post("https://rest.example.com/v1/namespaces")
            .header("content-type", "application/json")
            .header("authorization", "Bearer delegate-token")
            .body(r#"{"namespace":["a","b"],"properties":{}}"#)
            .build()
            .unwrap();
        signer.sign_at(&mut post, &creds, now.into()).unwrap();
        assert_eq!(
            post.headers().get("authorization").unwrap(),
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20261002/us-east-1/execute-api/aws4_request, \
             SignedHeaders=content-type;host;original-authorization;\
             x-amz-content-sha256;x-amz-date;x-amz-security-token, \
             Signature=c5d6bfcecd19c5421e8696c67465f6909b5e09398608d8e79a9eb391e04de556"
        );
        assert_eq!(
            post.headers().get("x-amz-content-sha256").unwrap(),
            "n1CB8Yl4L77sBwHTc5qTpUEhlsHHOXFzg0dV0fbOwXs="
        );

        // No body, a multi-level namespace and an encoded query.
        let mut get = client
            .get("https://rest.example.com/v1/namespaces/a%1Fb/tables/x,y?pageToken=a%20b%2Fc")
            .build()
            .unwrap();
        signer.sign_at(&mut get, &creds, now.into()).unwrap();
        assert_eq!(
            get.headers().get("authorization").unwrap(),
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20261002/us-east-1/execute-api/aws4_request, \
             SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-security-token, \
             Signature=5f6832ec82fc821b08cd3eac3a86ffc3e0c333571fd0a5796e43f2534f23329f"
        );
    }

    /// The signed `host` keeps a non-default port, as on the wire.
    #[test]
    fn signs_host_with_non_default_port() {
        let creds = example_credentials(None);
        let signer = SigV4Signer::new("us-east-1", "glue", PayloadHashMode::IcebergRest);
        let client = reqwest::Client::new();
        let mut req = client
            .get("https://rest.example.com:8181/v1/config")
            .build()
            .unwrap();
        let now = test_time();

        signer.sign_at(&mut req, &creds, now).unwrap();
        assert_signature_is(
            &req,
            "f7801a4ecac5fe6dcc4ee385223428ec4833f21d0c2fc10d2fb00694b2c0def7",
        );
    }

    /// Like `Aws4Signer`, the canonical path is encoded again: `,` becomes `%2C`
    /// and `%2C` becomes `%252C`.
    #[test]
    fn canonical_uri_is_aws_double_encoded() {
        let creds = example_credentials(None);
        let signer = SigV4Signer::new("us-east-1", "glue", PayloadHashMode::IcebergRest);
        let client = reqwest::Client::new();
        let mut req = client
            .get("https://rest.example.com/v1/namespaces/a%2Cb/tables/x,y")
            .build()
            .unwrap();
        let now = test_time();

        signer.sign_at(&mut req, &creds, now).unwrap();
        assert_signature_is(
            &req,
            "4d966b6fc2dfb62be5a603e4e07e5dc85b2af1c7e6a181c5646bfbf38ddfa543",
        );
    }

    /// The AWS SigV4 test suite's `post-x-www-form-urlencoded`, which signs a
    /// hex `x-amz-content-sha256` like this mode.
    #[test]
    fn standard_mode_matches_the_aws_test_suite() {
        let creds = example_credentials(None);
        let signer = SigV4Signer::new("us-east-1", "service", PayloadHashMode::StandardAws);
        let mut req = reqwest::Client::new()
            .post("https://example.amazonaws.com/")
            .header("content-type", "application/x-www-form-urlencoded")
            .header("content-length", "13")
            .body("Param1=value1")
            .build()
            .unwrap();

        signer.sign_at(&mut req, &creds, test_time()).unwrap();

        assert_eq!(
            req.headers().get("x-amz-content-sha256").unwrap(),
            "9095672bbd1f56dfc5b65f3e153adc8731a4a654192329106275f4c7b24d0b6e"
        );
        assert_eq!(
            req.headers().get("authorization").unwrap(),
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, \
             SignedHeaders=content-length;content-type;host;x-amz-content-sha256;x-amz-date, \
             Signature=d3875051da38690788ef43de4db0d8f280229d82040bfac253562e56c3f20e0b"
        );
    }

    /// The one test on the live clock: everything else pins the time.
    #[test]
    fn sign_stamps_the_current_time() {
        use chrono::SubsecRound;

        let signer = test_signer(PayloadHashMode::IcebergRest);
        let mut req = reqwest::Client::new()
            .get("https://rest.example.com/v1/config")
            .build()
            .unwrap();
        let before = Utc::now();
        signer.sign(&mut req, &test_credentials()).unwrap();
        let after = Utc::now();

        let date = req.headers().get("x-amz-date").unwrap().to_str().unwrap();
        let stamped = chrono::NaiveDateTime::parse_from_str(date, "%Y%m%dT%H%M%SZ")
            .unwrap()
            .and_utc();
        assert_eq!(date.len(), "YYYYMMDDTHHMMSSZ".len(), "{date}");
        // The header drops sub-seconds, so compare against a truncated start.
        assert!(
            before.trunc_subsecs(0) <= stamped && stamped <= after,
            "{date}"
        );
    }
}
