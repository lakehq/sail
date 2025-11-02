diesel::table! {
    ducklake_snapshot (snapshot_id) {
        snapshot_id -> BigInt,
        snapshot_time -> Text,
        schema_version -> BigInt,
        next_catalog_id -> BigInt,
        next_file_id -> BigInt,
    }
}

diesel::table! {
    ducklake_schema (schema_id) {
        schema_id -> BigInt,
        schema_uuid -> Text,
        begin_snapshot -> Nullable<BigInt>,
        end_snapshot -> Nullable<BigInt>,
        schema_name -> Text,
        path -> Text,
        path_is_relative -> Bool,
    }
}

diesel::table! {
    ducklake_table (table_id) {
        table_id -> BigInt,
        table_uuid -> Text,
        begin_snapshot -> Nullable<BigInt>,
        end_snapshot -> Nullable<BigInt>,
        schema_id -> BigInt,
        table_name -> Text,
        path -> Text,
        path_is_relative -> Bool,
    }
}

diesel::table! {
    ducklake_column (column_id) {
        column_id -> BigInt,
        begin_snapshot -> Nullable<BigInt>,
        end_snapshot -> Nullable<BigInt>,
        table_id -> BigInt,
        column_order -> BigInt,
        column_name -> Text,
        column_type -> Text,
        initial_default -> Nullable<Text>,
        default_value -> Nullable<Text>,
        nulls_allowed -> Bool,
        parent_column -> Nullable<BigInt>,
    }
}

diesel::table! {
    ducklake_data_file (data_file_id) {
        data_file_id -> BigInt,
        table_id -> BigInt,
        begin_snapshot -> Nullable<BigInt>,
        end_snapshot -> Nullable<BigInt>,
        file_order -> Nullable<BigInt>,
        path -> Text,
        path_is_relative -> Bool,
        file_format -> Text,
        record_count -> BigInt,
        file_size_bytes -> BigInt,
        footer_size -> Nullable<BigInt>,
        row_id_start -> Nullable<BigInt>,
        partition_id -> Nullable<BigInt>,
        encryption_key -> Nullable<Text>,
        partial_file_info -> Nullable<Text>,
        mapping_id -> Nullable<BigInt>,
    }
}

diesel::allow_tables_to_appear_in_same_query!(
    ducklake_snapshot,
    ducklake_schema,
    ducklake_table,
    ducklake_column,
    ducklake_data_file,
);
