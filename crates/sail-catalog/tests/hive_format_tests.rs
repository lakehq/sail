#![expect(clippy::unwrap_used)]

use sail_catalog::hive_format::{HiveCatalogFormat, HiveDetectedFormat, HiveStorageFormat};

#[test]
fn test_textfile_storage_format_uses_lazy_simple_serde() {
    let format = HiveStorageFormat::textfile();
    assert_eq!(
        format.input_format,
        "org.apache.hadoop.mapred.TextInputFormat"
    );
    assert_eq!(
        format.output_format,
        "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    );
    assert_eq!(
        format.serde_library,
        "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
    );
}

#[test]
fn test_detect_textfile_from_lazy_simple_serde() {
    let detected = HiveDetectedFormat::detect(
        Some("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"),
        Some("org.apache.hadoop.mapred.TextInputFormat"),
        Some("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"),
    );
    assert_eq!(detected, HiveDetectedFormat::TextFile);
}

#[test]
fn test_detect_csv_from_open_csv_serde() {
    let detected = HiveDetectedFormat::detect(
        Some("org.apache.hadoop.hive.serde2.OpenCSVSerde"),
        Some("org.apache.hadoop.mapred.TextInputFormat"),
        Some("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"),
    );
    assert_eq!(detected, HiveDetectedFormat::Csv);
}

#[test]
fn test_detect_json_before_text_fallback() {
    let detected = HiveDetectedFormat::detect(
        Some("org.openx.data.jsonserde.JsonSerDe"),
        Some("org.apache.hadoop.mapred.TextInputFormat"),
        Some("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"),
    );
    assert_eq!(detected, HiveDetectedFormat::Json);
}

#[test]
fn test_catalog_format_maps_delta_to_parquet_storage() {
    let format = HiveCatalogFormat::from_format("deltalake").unwrap();
    assert_eq!(format.logical_format, "delta");
    assert_eq!(format.storage_format, HiveStorageFormat::parquet());
}

#[test]
fn test_catalog_format_maps_textfile_to_textfile_storage() {
    let format = HiveCatalogFormat::from_format("textfile").unwrap();
    assert_eq!(format.logical_format, "textfile");
    assert_eq!(format.storage_format, HiveStorageFormat::textfile());
}
