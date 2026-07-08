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

use sail_build_scripts::openapi::generator::{OpenApiConfig, generate_openapi_client};

fn build_unity_catalog() -> Result<(), Box<dyn std::error::Error>> {
    let src = "spec/unity-catalog-all.yaml";
    println!("cargo:rerun-if-changed={src}");

    let file = std::fs::File::open(src)?;
    let spec = serde_yaml::from_reader(file)?;

    let mut settings = progenitor::GenerationSettings::new();
    settings.with_interface(progenitor::InterfaceStyle::Builder);
    let mut generator = progenitor::Generator::new(&settings);
    let tokens = generator.generate_tokens(&spec)?;
    let ast = syn::parse2(tokens)?;
    let content = prettyplease::unparse(&ast);

    let mut out_file = std::path::Path::new(&std::env::var("OUT_DIR")?).to_path_buf();
    out_file.push("unity_catalog.rs");

    let content = content
        .replace(
            "pub fn new(baseurl: &str) -> Self {",
            "pub fn new(baseurl: &str) -> Result<Self, reqwest::Error> {",
        )
        .replace(
            "Self::new_with_client(baseurl, client.build().unwrap())",
            "Ok(Self::new_with_client(baseurl, client.build()?))",
        )
        .replace(
            "pub(crate) client: reqwest::Client,\n}",
            "pub(crate) client: reqwest::Client,\n    pub(crate) request_headers: reqwest::header::HeaderMap,\n}",
        )
        .replace(
            "Self {\n            baseurl: baseurl.to_string(),\n            client,\n        }\n    }\n}",
            "Self::new_with_client_and_headers(baseurl, client, reqwest::header::HeaderMap::new())\n    }\n\n    /// Construct a new client with an existing `reqwest::Client` and per-request headers.\n    pub fn new_with_client_and_headers(\n        baseurl: &str,\n        client: reqwest::Client,\n        request_headers: reqwest::header::HeaderMap,\n    ) -> Self {\n        Self {\n            baseurl: baseurl.to_string(),\n            client,\n            request_headers,\n        }\n    }\n}",
        )
        .replace(
            "impl ClientHooks<()> for &Client {}\n",
            "impl ClientHooks<()> for &Client {}\nimpl ClientHooks<()> for Client {\n    async fn pre<E>(\n        &self,\n        request: &mut reqwest::Request,\n        _info: &OperationInfo,\n    ) -> ::std::result::Result<(), Error<E>> {\n        request.headers_mut().extend(self.request_headers.clone());\n        Ok(())\n    }\n}\n",
        )
        // We need to prevent the code examples from being treated as ignored tests.
        // These code snippets do not compile when running `cargo test -- --ignored`.
        .replace("```ignore", "```notrust");
    let content = add_column_type_name_aliases(content);
    let content = add_nullable_array_deserializer(content);
    let content = add_table_info_columns_deserializer(content);

    std::fs::write(out_file, content)?;

    Ok(())
}

fn add_column_type_name_aliases(content: String) -> String {
    // OneLake returns data types in uppercase.
    [
        ("BOOLEAN", "boolean", "Boolean"),
        ("BYTE", "byte", "Byte"),
        ("SHORT", "short", "Short"),
        ("INT", "int", "Int"),
        ("LONG", "long", "Long"),
        ("FLOAT", "float", "Float"),
        ("DOUBLE", "double", "Double"),
        ("DATE", "date", "Date"),
        ("TIMESTAMP", "timestamp", "Timestamp"),
        ("TIMESTAMP_NTZ", "timestamp_ntz", "TimestampNtz"),
        ("STRING", "string", "String"),
        ("BINARY", "binary", "Binary"),
        ("DECIMAL", "decimal", "Decimal"),
        ("INTERVAL", "interval", "Interval"),
        ("ARRAY", "array", "Array"),
        ("STRUCT", "struct", "Struct"),
        ("MAP", "map", "Map"),
        ("CHAR", "char", "Char"),
        ("NULL", "null", "Null"),
        ("USER_DEFINED_TYPE", "user_defined_type", "UserDefinedType"),
        ("TABLE_TYPE", "table_type", "TableType"),
    ]
    .into_iter()
    .fold(content, |content, (name, alias, variant)| {
        content.replace(
            &format!("#[serde(rename = \"{name}\")]\n        {variant},"),
            &format!("#[serde(rename = \"{name}\", alias = \"{alias}\")]\n        {variant},"),
        )
    })
}

fn add_nullable_array_deserializer(content: String) -> String {
    // OneLake may return `null` for columns when listing tables.
    content.replace(
        "pub mod types {\n",
        r#"pub mod types {
    fn deserialize_null_as_default<'de, D, T>(
        deserializer: D,
    ) -> ::std::result::Result<T, D::Error>
    where
        D: ::serde::Deserializer<'de>,
        T: ::std::default::Default + ::serde::Deserialize<'de>,
    {
        Ok(<::std::option::Option<T> as ::serde::Deserialize>::deserialize(deserializer)?
            .unwrap_or_default())
    }
"#,
    )
}

fn add_table_info_columns_deserializer(content: String) -> String {
    let marker = "    pub struct TableInfo {\n        ///Name of parent catalog.";
    let old = r#"        #[serde(default, skip_serializing_if = "::std::vec::Vec::is_empty")]
        pub columns: ::std::vec::Vec<ColumnInfo>,"#;
    let new = r#"        #[serde(
            default,
            deserialize_with = "deserialize_null_as_default",
            skip_serializing_if = "::std::vec::Vec::is_empty"
        )]
        pub columns: ::std::vec::Vec<ColumnInfo>,"#;
    if let Some((before, after)) = content.split_once(marker) {
        format!("{before}{marker}{}", after.replacen(old, new, 1))
    } else {
        content
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=build.rs");
    build_unity_catalog()?;

    generate_openapi_client(
        "spec/unity-catalog-all.yaml",
        std::path::Path::new(&std::env::var("OUT_DIR")?).join("unity_catalog_gen.rs"),
        OpenApiConfig::new(),
    )?;

    Ok(())
}
