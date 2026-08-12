use std::sync::Arc;

use sail_common_datafusion::datasource::OptionLayer;

pub mod arrow;
pub mod avro;
pub mod binary;
pub mod console;
pub mod csv;
pub mod json;
pub mod noop;
pub mod parquet;
pub mod python;
pub mod rate;
pub mod socket;
pub mod text;

fn effective_string_option(
    options: &[OptionLayer],
    aliases: &[&str],
    default: &'static str,
) -> Arc<str> {
    let mut output = Arc::<str>::from(default);
    for layer in options {
        let (items, table_properties) = match layer {
            OptionLayer::TablePropertyList { items } => (items, true),
            OptionLayer::OptionList { items } => (items, false),
            _ => continue,
        };
        for (key, value) in items {
            let key = if table_properties {
                let Some(key) = key
                    .get(..7)
                    .filter(|prefix| prefix.eq_ignore_ascii_case("option."))
                    .and_then(|_| key.get(7..))
                else {
                    continue;
                };
                key
            } else {
                key
            };
            if aliases.iter().any(|alias| key.eq_ignore_ascii_case(alias)) {
                output = Arc::from(value.as_str());
            }
        }
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn option_prefix_is_only_special_in_table_properties() {
        let options = vec![
            OptionLayer::TablePropertyList {
                items: vec![("option.timestampFormat".to_string(), "table".to_string())],
            },
            OptionLayer::OptionList {
                items: vec![("option.timestampFormat".to_string(), "ignored".to_string())],
            },
        ];
        assert_eq!(
            effective_string_option(&options, &["timestampFormat"], "default").as_ref(),
            "table"
        );
    }
}
