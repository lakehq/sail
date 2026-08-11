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
        let items = match layer {
            OptionLayer::TablePropertyList { items } | OptionLayer::OptionList { items } => items,
            _ => continue,
        };
        for (key, value) in items {
            let key = key
                .get(..7)
                .filter(|prefix| prefix.eq_ignore_ascii_case("option."))
                .and_then(|_| key.get(7..))
                .unwrap_or(key);
            if aliases.iter().any(|alias| key.eq_ignore_ascii_case(alias)) {
                output = Arc::from(value.as_str());
            }
        }
    }
    output
}
