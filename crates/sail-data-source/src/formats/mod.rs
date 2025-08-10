// use datafusion_common::Result;
// use std::str::FromStr;

pub mod delta;
pub mod listing;
pub mod text;

// #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
// pub enum CompressionTypeVariant {
//     UNCOMPRESSED,
//     BZIP2,
//     GZIP,
//     ZSTD,
//     SNAPPY,
//     LZ4,
//     XZ,
// }
//
// impl FromStr for CompressionTypeVariant {
//     type Err = datafusion_common::DataFusionError;
//
//     fn from_str(s: &str) -> Result<Self> {
//         let s = s.trim().to_uppercase();
//         match s.as_str() {
//             "" | "UNCOMPRESSED" | "NONE" => Ok(Self::UNCOMPRESSED),
//             "BZIP2" | "BZ2" => Ok(Self::BZIP2),
//             "GZIP" | "GZ" => Ok(Self::GZIP),
//             "ZSTD" | "ZST" => Ok(Self::ZSTD),
//             "SNAPPY" => Ok(Self::SNAPPY),
//             "LZ4" => Ok(Self::LZ4),
//             "XZ" => Ok(Self::XZ),
//             _ => Err(datafusion_common::DataFusionError::Plan(format!(
//                 "Unsupported file compression type: {s}"
//             ))),
//         }
//     }
// }
