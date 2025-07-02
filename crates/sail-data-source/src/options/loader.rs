use std::collections::HashMap;

use sail_common::error::{CommonError, CommonResult};
use serde::de::value::MapDeserializer;

use crate::options::DataSourceOptions;

// TODO: implement a function to load options with only allowed keys
//   This is to avoid deserializing unknown external table options for table read/write.

pub fn load_options<T: DataSourceOptions>(options: HashMap<String, String>) -> CommonResult<T> {
    let options = options.into_iter().map(|(k, v)| (k.to_lowercase(), v));
    T::deserialize(<MapDeserializer<'_, _, serde::de::value::Error>>::new(
        options,
    ))
    .map_err(|e| CommonError::invalid(e.to_string()))
}

pub fn load_default_options<T: DataSourceOptions>() -> CommonResult<T> {
    let options = T::DEFAULT_VALUES
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    load_options(options)
}
