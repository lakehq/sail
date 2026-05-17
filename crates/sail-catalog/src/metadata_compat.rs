use datafusion_common::Result;
use lazy_static::lazy_static;
use regex::Regex;
use sail_common_datafusion::catalog::TableStatus;
use sail_common_datafusion::session::plan::PlanFormatter;

const REDACTION_REPLACEMENT_TEXT: &str = "*********(redacted)";

fn create_regex(regex: std::result::Result<Regex, regex::Error>) -> Regex {
    #[expect(clippy::expect_used)]
    regex.expect("valid regex")
}

lazy_static! {
    static ref SQL_OPTIONS_REDACTION_PATTERN: Regex = create_regex(Regex::new("(?i)url"));
    static ref SECRET_REDACTION_PATTERN: Regex =
        create_regex(Regex::new("(?i)secret|password|token|access[.]?key"));
}

const SPARK_RESERVED_TABLE_PROPERTIES: &[&str] = &[
    "comment",
    "collation",
    "location",
    "provider",
    "owner",
    "external",
    "is_managed_location",
    "table_type",
];

pub fn show_tblproperties_rows(
    table_name: &str,
    properties: &[(String, String)],
    property_key: Option<&str>,
) -> Vec<(String, String)> {
    let mut visible_properties: Vec<(String, String)> = properties
        .iter()
        .filter(|(k, _)| !SPARK_RESERVED_TABLE_PROPERTIES.contains(&k.as_str()))
        .map(|(k, v)| (k.clone(), redact_property_value(k, v)))
        .collect();

    match property_key {
        Some(key) => {
            let value = visible_properties
                .iter()
                .find(|(k, _)| k == key)
                .map(|(_, v)| v.clone())
                .unwrap_or_else(|| format!("Table {table_name} does not have property: {key}"));
            vec![(key.to_string(), value)]
        }
        None => {
            visible_properties.sort_by(|a, b| a.0.cmp(&b.0));
            visible_properties
        }
    }
}

pub fn describe_extended_metadata_rows(table: &TableStatus) -> Vec<(String, String)> {
    detailed_table_rows(table, true)
}

pub fn show_table_extended_information(
    table: &TableStatus,
    formatter: &dyn PlanFormatter,
) -> Result<String> {
    let mut output = String::new();

    for (key, value) in detailed_table_rows(table, false) {
        output.push_str(&format!("{key}: {value}\n"));
    }

    output.push_str("Schema: root\n");
    for column in table.kind.columns() {
        let data_type = formatter
            .data_type_to_simple_string(&column.data_type)
            .unwrap_or_else(|_| "invalid".to_string());
        let nullable = if column.nullable { "true" } else { "false" };
        output.push_str(&format!(
            " |-- {}: {} (nullable = {})\n",
            column.name, data_type, nullable
        ));
    }

    Ok(output)
}

fn detailed_table_rows(
    table: &TableStatus,
    always_include_properties: bool,
) -> Vec<(String, String)> {
    let mut rows = Vec::new();

    rows.push(("Database".to_string(), table.database.join(".")));
    rows.push(("Table".to_string(), table.name.clone()));
    rows.push(("Type".to_string(), table.kind.type_name().to_string()));

    if let Some(format) = table.kind.format() {
        rows.push(("Provider".to_string(), format.to_string()));
    }

    if let Some(comment) = table.kind.comment() {
        rows.push(("Comment".to_string(), comment));
    }

    if let Some(definition) = table.kind.view_definition() {
        rows.push(("View Text".to_string(), definition.to_string()));
    }

    if let Some(props) = table_properties_cell(table.kind.properties(), always_include_properties) {
        rows.push(("Table Properties".to_string(), props));
    }

    if let Some(loc) = table.kind.location() {
        rows.push(("Location".to_string(), loc.to_string()));
    }

    rows
}

fn table_properties_cell(
    properties: &[(String, String)],
    always_include_properties: bool,
) -> Option<String> {
    if properties.is_empty() && !always_include_properties {
        return None;
    }
    let visible_properties = visible_properties(properties);
    if visible_properties.is_empty() {
        return Some("[]".to_string());
    }
    let props_str = visible_properties
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(", ");
    Some(format!("[{props_str}]"))
}

fn visible_properties(properties: &[(String, String)]) -> Vec<(String, String)> {
    let mut visible_properties: Vec<(String, String)> = properties
        .iter()
        .filter(|(k, _)| !SPARK_RESERVED_TABLE_PROPERTIES.contains(&k.as_str()))
        .map(|(k, v)| (k.clone(), redact_property_value(k, v)))
        .collect();
    visible_properties.sort_by(|a, b| a.0.cmp(&b.0));
    visible_properties
}

pub fn redact_property_value(key: &str, value: &str) -> String {
    if SQL_OPTIONS_REDACTION_PATTERN.is_match(key)
        || SECRET_REDACTION_PATTERN.is_match(key)
        || SECRET_REDACTION_PATTERN.is_match(value)
    {
        REDACTION_REPLACEMENT_TEXT.to_string()
    } else {
        value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::DataType;
    use datafusion_common::ScalarValue;
    use sail_common_datafusion::catalog::{TableColumnStatus, TableKind, TableStatus};
    use sail_common_datafusion::session::plan::PlanFormatter;

    use super::{
        describe_extended_metadata_rows, redact_property_value, show_table_extended_information,
        show_tblproperties_rows, REDACTION_REPLACEMENT_TEXT,
    };

    #[test]
    fn show_tblproperties_filters_reserved_keys_and_sorts() {
        let rows = show_tblproperties_rows(
            "spark_catalog.default.t",
            &[
                ("z".to_string(), "9".to_string()),
                ("provider".to_string(), "delta".to_string()),
                ("a".to_string(), "1".to_string()),
                ("comment".to_string(), "ignored".to_string()),
            ],
            None,
        );
        assert_eq!(
            rows,
            vec![
                ("a".to_string(), "1".to_string()),
                ("z".to_string(), "9".to_string()),
            ]
        );
    }

    #[test]
    fn show_tblproperties_reserved_lookup_behaves_like_missing() {
        let rows = show_tblproperties_rows(
            "spark_catalog.default.t",
            &[("provider".to_string(), "delta".to_string())],
            Some("provider"),
        );
        assert_eq!(
            rows,
            vec![(
                "provider".to_string(),
                "Table spark_catalog.default.t does not have property: provider".to_string()
            )]
        );
    }

    #[test]
    fn show_tblproperties_returns_explicit_value_for_visible_key_lookup() {
        let rows = show_tblproperties_rows(
            "spark_catalog.default.t",
            &[("custom.key".to_string(), "value".to_string())],
            Some("custom.key"),
        );
        assert_eq!(rows, vec![("custom.key".to_string(), "value".to_string())]);
    }

    #[test]
    fn show_tblproperties_redacts_sensitive_property_values() {
        let rows = show_tblproperties_rows(
            "spark_catalog.default.t",
            &[
                ("password".to_string(), "hunter2".to_string()),
                ("url".to_string(), "https://example.test".to_string()),
                ("user".to_string(), "andrew".to_string()),
                (
                    "endpoint".to_string(),
                    "https://example.test/storage".to_string(),
                ),
            ],
            None,
        );
        assert_eq!(
            rows,
            vec![
                (
                    "endpoint".to_string(),
                    "https://example.test/storage".to_string()
                ),
                ("password".to_string(), "*********(redacted)".to_string()),
                ("url".to_string(), "*********(redacted)".to_string()),
                ("user".to_string(), "andrew".to_string()),
            ]
        );
    }

    #[test]
    fn describe_extended_redacts_sensitive_table_properties() {
        let table = test_table(vec![
            ("password".to_string(), "hunter2".to_string()),
            ("user".to_string(), "andrew".to_string()),
        ]);
        let rows = describe_extended_metadata_rows(&table);
        let table_properties = rows
            .iter()
            .find(|(k, _)| k == "Table Properties")
            .map(|(_, v)| v.as_str());
        assert_eq!(
            table_properties,
            Some("[password=*********(redacted), user=andrew]")
        );
    }

    #[test]
    fn describe_extended_always_includes_table_properties_row() {
        let table = test_table(vec![]);
        let rows = describe_extended_metadata_rows(&table);
        assert!(rows
            .iter()
            .any(|(k, v)| k == "Table Properties" && v == "[]"));
    }

    #[test]
    fn describe_extended_filters_reserved_properties_and_sorts_visible_properties() {
        let table = test_table(vec![
            ("z".to_string(), "9".to_string()),
            ("provider".to_string(), "delta".to_string()),
            ("a".to_string(), "1".to_string()),
        ]);
        let rows = describe_extended_metadata_rows(&table);
        let table_properties = rows
            .iter()
            .find(|(k, _)| k == "Table Properties")
            .map(|(_, v)| v.as_str());
        assert_eq!(table_properties, Some("[a=1, z=9]"));
    }

    #[test]
    fn show_table_extended_omits_table_properties_when_no_underlying_properties(
    ) -> datafusion_common::Result<()> {
        let table = test_table(vec![]);
        let formatter = TestPlanFormatter;
        let information = show_table_extended_information(&table, &formatter)?;
        assert!(!information.contains("Table Properties:"));
        Ok(())
    }

    #[test]
    fn show_table_extended_keeps_table_properties_when_only_reserved_properties_exist(
    ) -> datafusion_common::Result<()> {
        let table = test_table(vec![("provider".to_string(), "delta".to_string())]);
        let formatter = TestPlanFormatter;
        let information = show_table_extended_information(&table, &formatter)?;
        assert!(information.contains("Table Properties: []"));
        Ok(())
    }

    #[test]
    fn redact_property_value_redacts_when_value_contains_secret_pattern() {
        assert_eq!(
            redact_property_value("endpoint", "s3://access-key:secret@bucket/path"),
            REDACTION_REPLACEMENT_TEXT
        );
        assert_eq!(
            redact_property_value("endpoint", "https://example.test/storage"),
            "https://example.test/storage"
        );
    }

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
    struct TestPlanFormatter;

    impl PlanFormatter for TestPlanFormatter {
        fn data_type_to_simple_string(
            &self,
            data_type: &DataType,
        ) -> datafusion_common::Result<String> {
            Ok(match data_type {
                DataType::Int32 => "int".to_string(),
                _ => "unknown".to_string(),
            })
        }

        fn literal_to_string(
            &self,
            _literal: &ScalarValue,
            _display_timezone: &str,
        ) -> datafusion_common::Result<String> {
            Ok(String::new())
        }

        fn function_to_string(
            &self,
            _name: &str,
            _arguments: Vec<&str>,
            _is_distinct: bool,
        ) -> datafusion_common::Result<String> {
            Ok(String::new())
        }
    }

    fn test_table(properties: Vec<(String, String)>) -> TableStatus {
        TableStatus {
            catalog: Some("spark_catalog".to_string()),
            database: vec!["default".to_string()],
            name: "t".to_string(),
            kind: TableKind::Table {
                columns: vec![TableColumnStatus {
                    name: "id".to_string(),
                    data_type: DataType::Int32,
                    nullable: true,
                    comment: None,
                    default: None,
                    generated_always_as: None,
                    is_partition: false,
                    is_bucket: false,
                    is_cluster: false,
                }],
                comment: Some("c".to_string()),
                constraints: vec![],
                location: Some("file:/tmp/t".to_string()),
                format: "parquet".to_string(),
                partition_by: vec![],
                sort_by: vec![],
                bucket_by: None,
                properties,
                is_external: false,
            },
        }
    }
}
