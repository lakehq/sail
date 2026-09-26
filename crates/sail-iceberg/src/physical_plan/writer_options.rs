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

use std::collections::HashMap;

use datafusion_common::{DataFusionError, Result};
use parquet::basic::{Compression, GzipLevel, ZstdLevel};
use parquet::file::properties::WriterProperties;
use sail_common_datafusion::catalog::LakehouseExecutionContext;
use sail_common_datafusion::datasource::OptionLayer;
use sail_common_datafusion::variant::DEFAULT_VARIANT_INFERENCE_NODE_BUDGET;
use serde::{Deserialize, Serialize};

use crate::operations::write::config::VariantShreddingConfig;
use crate::options::r#gen::IcebergWriteOptions;

const PARQUET_SHRED_VARIANTS: &str = "write.parquet.shred-variants";
const PARQUET_VARIANT_INFERENCE_BUFFER_SIZE: &str = "write.parquet.variant-inference-buffer-size";
const SHRED_VARIANTS_OPTION_KEYS: &[&str] = &["shred-variants", "shred_variants", "shredVariants"];
const VARIANT_INFERENCE_BUFFER_SIZE_OPTION_KEYS: &[&str] = &[
    "variant-inference-buffer-size",
    "variant_inference_buffer_size",
    "variantInferenceBufferSize",
];

#[derive(Debug, Clone, Copy, Default)]
pub struct VariantShreddingOptionPresence {
    shred_variants: bool,
    variant_inference_buffer_size: bool,
}

/// Options for the Iceberg writer execution plan.
/// This is a subset of `IcebergWriteOptions` containing only the fields used
/// during physical writing. It derives serde for use in the physical plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergWriterExecOptions {
    pub copy_on_write_partitioning: bool,
    pub target_file_size_bytes: Option<u64>,
    pub compression_codec: Option<String>,
    pub compression_level: Option<String>,
    pub merge_schema: bool,
    pub overwrite_schema: bool,
    pub write_data_path: Option<String>,
    pub write_folder_storage_path: Option<String>,
    pub table_properties: Vec<(String, String)>,
    pub lakehouse_table: Option<LakehouseExecutionContext>,
    pub shred_variants: bool,
    pub shred_variants_explicit: bool,
    pub variant_inference_buffer_size: usize,
    pub variant_inference_buffer_size_explicit: bool,
}

impl Default for IcebergWriterExecOptions {
    fn default() -> Self {
        Self {
            copy_on_write_partitioning: true,
            target_file_size_bytes: None,
            compression_codec: None,
            compression_level: None,
            merge_schema: false,
            overwrite_schema: false,
            write_data_path: None,
            write_folder_storage_path: None,
            table_properties: vec![],
            lakehouse_table: None,
            shred_variants: false,
            shred_variants_explicit: false,
            variant_inference_buffer_size: 100,
            variant_inference_buffer_size_explicit: false,
        }
    }
}

impl From<IcebergWriteOptions> for IcebergWriterExecOptions {
    fn from(options: IcebergWriteOptions) -> Self {
        Self {
            copy_on_write_partitioning: true,
            target_file_size_bytes: options.target_file_size_bytes,
            compression_codec: options.compression_codec,
            compression_level: options.compression_level,
            merge_schema: options.merge_schema,
            overwrite_schema: options.overwrite_schema,
            write_data_path: options.write_data_path,
            write_folder_storage_path: options.write_folder_storage_path,
            table_properties: vec![],
            lakehouse_table: None,
            shred_variants: options.shred_variants,
            shred_variants_explicit: false,
            variant_inference_buffer_size: options.variant_inference_buffer_size,
            variant_inference_buffer_size_explicit: false,
        }
    }
}

impl IcebergWriterExecOptions {
    pub fn target_file_size(&self, properties: &HashMap<String, String>) -> Result<u64> {
        let size = self.target_file_size_bytes.map(Ok).unwrap_or_else(|| {
            properties
                .get("write.target-file-size-bytes")
                .map(|value| {
                    value.parse::<u64>().map_err(|_| {
                        DataFusionError::Plan(format!(
                            "Invalid Iceberg write.target-file-size-bytes: {value}"
                        ))
                    })
                })
                .unwrap_or(Ok(512 * 1024 * 1024))
        })?;
        if size == 0 {
            return datafusion_common::plan_err!("Iceberg target file size must be positive");
        }
        Ok(size)
    }

    pub fn parquet_properties(
        &self,
        properties: &HashMap<String, String>,
    ) -> Result<WriterProperties> {
        let codec = self
            .compression_codec
            .as_deref()
            .or_else(|| {
                properties
                    .get("write.parquet.compression-codec")
                    .map(String::as_str)
            })
            .unwrap_or("zstd");
        let level = self.compression_level.as_deref().or_else(|| {
            properties
                .get("write.parquet.compression-level")
                .map(String::as_str)
        });
        let invalid = |error: String| {
            DataFusionError::Plan(format!(
                "Invalid Iceberg Parquet compression {codec}: {error}"
            ))
        };
        let compression = match codec.to_ascii_lowercase().as_str() {
            "uncompressed" | "none" => Compression::UNCOMPRESSED,
            "snappy" => Compression::SNAPPY,
            "lz4" | "lz4_raw" => Compression::LZ4_RAW,
            "zstd" => Compression::ZSTD(match level {
                Some(level) => ZstdLevel::try_new(
                    level
                        .parse()
                        .map_err(|error: std::num::ParseIntError| invalid(error.to_string()))?,
                )
                .map_err(|error| invalid(error.to_string()))?,
                None => ZstdLevel::default(),
            }),
            "gzip" => Compression::GZIP(match level {
                Some(level) => GzipLevel::try_new(
                    level
                        .parse()
                        .map_err(|error: std::num::ParseIntError| invalid(error.to_string()))?,
                )
                .map_err(|error| invalid(error.to_string()))?,
                None => GzipLevel::default(),
            }),
            _ => {
                return datafusion_common::plan_err!(
                    "Unsupported Iceberg Parquet compression codec: {codec}"
                );
            }
        };
        let positive = |key: &str, default: usize| -> Result<usize> {
            let value = properties
                .get(key)
                .map(|value| parse_usize_property(key, value))
                .transpose()?
                .unwrap_or(default);
            if value == 0 {
                return datafusion_common::plan_err!("Iceberg {key} must be positive");
            }
            Ok(value)
        };
        Ok(WriterProperties::builder()
            .set_compression(compression)
            .set_statistics_truncate_length(None)
            .set_max_row_group_bytes(Some(positive(
                "write.parquet.row-group-size-bytes",
                128 * 1024 * 1024,
            )?))
            .set_data_page_size_limit(positive("write.parquet.page-size-bytes", 1024 * 1024)?)
            .set_data_page_row_count_limit(positive("write.parquet.page-row-limit", 20_000)?)
            .set_dictionary_page_size_limit(positive(
                "write.parquet.dict-size-bytes",
                2 * 1024 * 1024,
            )?)
            .build())
    }

    pub fn variant_shredding_option_presence(
        layers: &[OptionLayer],
    ) -> VariantShreddingOptionPresence {
        let mut presence = VariantShreddingOptionPresence::default();
        for layer in layers {
            match layer {
                OptionLayer::OptionList { items } => {
                    for (key, _) in items {
                        if SHRED_VARIANTS_OPTION_KEYS
                            .iter()
                            .any(|candidate| key.eq_ignore_ascii_case(candidate))
                        {
                            presence.shred_variants = true;
                        }
                        if VARIANT_INFERENCE_BUFFER_SIZE_OPTION_KEYS
                            .iter()
                            .any(|candidate| key.eq_ignore_ascii_case(candidate))
                        {
                            presence.variant_inference_buffer_size = true;
                        }
                    }
                }
                OptionLayer::TablePropertyList { items } => {
                    for (key, _) in items {
                        if key == PARQUET_SHRED_VARIANTS {
                            presence.shred_variants = true;
                        }
                        if key == PARQUET_VARIANT_INFERENCE_BUFFER_SIZE {
                            presence.variant_inference_buffer_size = true;
                        }
                    }
                }
                OptionLayer::TableLocation { .. }
                | OptionLayer::AsOfTimestamp { .. }
                | OptionLayer::AsOfIntegerVersion { .. }
                | OptionLayer::AsOfStringVersion { .. } => {}
            }
        }
        presence
    }

    pub fn apply_variant_shredding_option_presence(
        &mut self,
        presence: VariantShreddingOptionPresence,
    ) {
        self.shred_variants_explicit = presence.shred_variants;
        self.variant_inference_buffer_size_explicit = presence.variant_inference_buffer_size;
    }

    pub fn variant_shredding_config(
        &self,
        table_properties: &HashMap<String, String>,
    ) -> Result<VariantShreddingConfig> {
        let enabled = if self.shred_variants_explicit {
            self.shred_variants
        } else {
            table_properties
                .get(PARQUET_SHRED_VARIANTS)
                .map(|value| parse_bool_property(PARQUET_SHRED_VARIANTS, value))
                .transpose()?
                .unwrap_or(self.shred_variants)
        };

        let inference_buffer_size = if self.variant_inference_buffer_size_explicit {
            self.variant_inference_buffer_size
        } else {
            table_properties
                .get(PARQUET_VARIANT_INFERENCE_BUFFER_SIZE)
                .map(|value| parse_usize_property(PARQUET_VARIANT_INFERENCE_BUFFER_SIZE, value))
                .transpose()?
                .unwrap_or(self.variant_inference_buffer_size)
        };

        Ok(VariantShreddingConfig {
            enabled,
            inference_buffer_size,
            inference_node_budget: DEFAULT_VARIANT_INFERENCE_NODE_BUDGET,
        })
    }
}

fn parse_bool_property(key: &str, value: &str) -> Result<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        _ => Err(DataFusionError::Plan(format!(
            "invalid Iceberg table property {key} value: {value}"
        ))),
    }
}

fn parse_usize_property(key: &str, value: &str) -> Result<usize> {
    value.parse::<usize>().map_err(|_| {
        DataFusionError::Plan(format!(
            "invalid Iceberg table property {key} value: {value}"
        ))
    })
}
