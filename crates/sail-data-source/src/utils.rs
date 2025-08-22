use datafusion_common::{plan_err, DataFusionError};

pub fn char_to_u8(c: char, option: &str) -> datafusion_common::Result<u8> {
    if c.is_ascii() {
        Ok(c as u8)
    } else {
        plan_err!("invalid {option} character '{c}': must be an ASCII character")
    }
}

// [CREDIT]: https://github.com/apache/datafusion/blob/92d516cc9b1bb8912f7b9c4f122903491c0c9a85/datafusion/common/src/file_options/parquet_writer.rs#L308-L325
pub fn split_parquet_compression_string(
    str_setting: &str,
) -> datafusion_common::Result<(String, Option<u32>)> {
    let str_setting = str_setting.replace(['\'', '"'], "");
    let split_setting = str_setting.split_once('(');

    match split_setting {
        Some((codec, rh)) => {
            let level = &rh[..rh.len() - 1].parse::<u32>().map_err(|_| {
                DataFusionError::Configuration(format!(
                    "Could not parse compression string. \
                    Got codec: {codec} and unknown level from {str_setting}"
                ))
            })?;
            Ok((codec.to_owned(), Some(*level)))
        }
        None => Ok((str_setting.to_owned(), None)),
    }
}
