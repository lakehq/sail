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
    Ok(())
}
