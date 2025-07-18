use figment::value::{Dict, Map, Tag, Value};
use figment::{Error, Metadata, Profile, Provider};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "lowercase")]
enum ConfigType {
    String,
    Number,
    Boolean,
    Array,
    Map,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfigItem {
    key: String,
    #[expect(unused)]
    r#type: ConfigType,
    /// Every configuration item must have a default value.
    /// The default value is a string that can be parsed into
    /// [`figment::value::Value`].
    ///
    /// Note that configuration of the [`Option`] type can not be parsed
    /// by default, in which case a custom `serde` attribute is needed in the
    /// configuration struct field to handle the representation for [`None`].
    default: String,
    /// The description of the configuration item.
    ///
    /// The text should be written in Markdown format.
    #[expect(unused)]
    description: String,
    #[expect(unused)]
    #[serde(default)]
    experimental: bool,
    #[expect(unused)]
    #[serde(default)]
    hidden: bool,
}

/// Deserialize a unit value and ignore the `serde(deny_unknown_fields)` attribute.
pub fn deserialize_unknown_unit<'de, D>(_: D) -> Result<(), D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(())
}

pub fn deserialize_non_empty_string<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    if value.is_empty() {
        Ok(None)
    } else {
        Ok(Some(value))
    }
}

pub fn deserialize_non_zero<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: num_traits::Zero + serde::Deserialize<'de>,
{
    let value = T::deserialize(deserializer)?;
    if value.is_zero() {
        Ok(None)
    } else {
        Ok(Some(value))
    }
}

pub struct ConfigDefinition<'a> {
    raw: &'a str,
    profile: Profile,
}

impl<'a> ConfigDefinition<'a> {
    pub fn new(raw: &'a str) -> Self {
        Self {
            raw,
            profile: Profile::Default,
        }
    }
}

impl Provider for ConfigDefinition<'_> {
    fn metadata(&self) -> Metadata {
        Metadata::named("definition")
    }

    fn data(&self) -> Result<Map<Profile, Dict>, Error> {
        let items: Vec<ConfigItem> =
            serde_yaml::from_str(self.raw).map_err(|e| Error::from(e.to_string()))?;
        let mut dict = Dict::new();
        for item in items {
            let value: Value = item.default.parse().unwrap();
            let split = item.key.split('.').collect::<Vec<_>>();
            let [prefixes @ .., last] = split.as_slice() else {
                return Err(Error::from(format!("invalid key: {}", item.key)));
            };
            let mut current = &mut dict;
            for prefix in prefixes {
                let Value::Dict(_, v) = current
                    .entry(prefix.to_string())
                    .or_insert_with(|| Value::Dict(Tag::default(), Dict::new()))
                else {
                    return Err(Error::from(format!(
                        "conflicting value type for key: {}",
                        item.key
                    )));
                };
                current = v;
            }
            current.insert(last.to_string(), value);
        }
        let mut map = Map::new();
        map.insert(self.profile.clone(), dict);
        Ok(map)
    }

    fn profile(&self) -> Option<Profile> {
        Some(self.profile.clone())
    }
}
