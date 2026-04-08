use crate::error::{DataSourceError, DataSourceResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeltaLogReplayStrategyOption {
    #[default]
    Auto,
    Sort,
    Hash,
}

pub fn parse_delta_log_replay_strategy(
    key: &str,
    value: &str,
) -> DataSourceResult<DeltaLogReplayStrategyOption> {
    match value.to_ascii_lowercase().as_str() {
        "auto" => Ok(DeltaLogReplayStrategyOption::Auto),
        "sort" => Ok(DeltaLogReplayStrategyOption::Sort),
        "hash" => Ok(DeltaLogReplayStrategyOption::Hash),
        _ => Err(DataSourceError::InvalidOption {
            key: key.to_string(),
            value: value.to_string(),
        }),
    }
}
