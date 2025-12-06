use std::fmt;
use std::path::PathBuf;

use quote::{format_ident, quote};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MetricDefinition {
    /// The key for the metric.
    name: String,
    /// The optional unit for the metric.
    /// According to OpenTelemetry semantic conventions, units should follow
    /// the UCUM (The Unified Code for Units of Measure) standard: <https://ucum.org/ucum>.
    ///
    /// See also: <https://opentelemetry.io/docs/specs/semconv/general/metrics/#units>
    #[serde(default)]
    unit: Option<MetricUnit>,
    /// The metric description in Markdown format.
    description: String,
    /// The metric type.
    r#type: MetricType,
    /// The metric value type.
    value_type: MetricValueType,
    /// The allowed attributes associated with the metric.
    attributes: Vec<AttributeName>,
}

impl MetricDefinition {
    fn field_name(&self) -> String {
        self.name.replace('.', "_")
    }
}

/// The units supported in the metric registry.
/// Although OpenTelemetry allows broader set of strings as units,
/// we restrict the units to a small set to avoid typos and inconsistencies
/// in our metric definitions.
#[derive(Deserialize)]
enum MetricUnit {
    #[serde(rename = "byte")]
    Byte,
    #[serde(rename = "s")]
    Second,
}

impl fmt::Display for MetricUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MetricUnit::Byte => write!(f, "byte"),
            MetricUnit::Second => write!(f, "s"),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
enum MetricType {
    Counter,
    UpDownCounter,
    Gauge,
    Histogram,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum MetricValueType {
    U64,
    I64,
    F64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MetricAttribute {
    /// The key for the attribute.
    name: AttributeName,
    /// The attribute description in Markdown format.
    description: String,
}

#[derive(Deserialize)]
struct AttributeName(String);

impl AttributeName {
    fn key(&self) -> String {
        self.0.clone()
    }

    fn constant_name(&self) -> String {
        self.0.replace('.', "_").to_uppercase()
    }
}

fn build_metric_registry() -> Result<(), Box<dyn std::error::Error>> {
    let path = "src/metrics/data/registry.yaml";
    println!("cargo:rerun-if-changed={path}");

    let content = std::fs::read_to_string(path)?;
    let metrics: Vec<MetricDefinition> = serde_yaml::from_str(&content)?;
    let fields = metrics
        .iter()
        .map(|metric| {
            let description = &metric.description;
            let doc = if let Some(unit) = &metric.unit {
                let unit = format!("The unit is `{unit}`.");
                quote! {
                    #[doc = #description]
                    #[doc = ""]
                    #[doc = #unit]
                }
            } else {
                quote! { #[doc = #description] }
            };
            let field_name = format_ident!("{}", metric.field_name());
            let field_type = match (&metric.r#type, &metric.value_type) {
                (MetricType::Counter, MetricValueType::U64) => {
                    quote! { crate::metrics::Counter<u64> }
                }
                (MetricType::Counter, MetricValueType::F64) => {
                    quote! { crate::metrics::Counter<f64> }
                }
                (MetricType::UpDownCounter, MetricValueType::I64) => {
                    quote! { crate::metrics::UpDownCounter<i64> }
                }
                (MetricType::UpDownCounter, MetricValueType::F64) => {
                    quote! { crate::metrics::UpDownCounter<f64> }
                }
                (MetricType::Gauge, MetricValueType::U64) => {
                    quote! { crate::metrics::Gauge<u64> }
                }
                (MetricType::Gauge, MetricValueType::I64) => {
                    quote! { crate::metrics::Gauge<i64> }
                }
                (MetricType::Gauge, MetricValueType::F64) => {
                    quote! { crate::metrics::Gauge<f64> }
                }
                (MetricType::Histogram, MetricValueType::U64) => {
                    quote! { crate::metrics::Histogram<u64> }
                }
                (MetricType::Histogram, MetricValueType::F64) => {
                    quote! { crate::metrics::Histogram<f64> }
                }
                (t, vt) => {
                    return Err(syn::Error::new(
                        proc_macro2::Span::call_site(),
                        format!("invalid metric: {t:?} {vt:?}"),
                    ))
                }
            };
            Ok(quote! {
                #doc
                pub #field_name: #field_type,
            })
        })
        .collect::<Result<Vec<_>, syn::Error>>()?;

    let initializers = metrics
        .iter()
        .map(|metric| {
            let field_name = format_ident!("{}", metric.field_name());
            let metric_name = &metric.name;
            let with_unit = match &metric.unit {
                Some(unit) => {
                    let unit = unit.to_string();
                    quote! { .with_unit(#unit) }
                }
                None => quote! {},
            };
            let description = &metric.description;
            let build = quote! { #with_unit.with_description(#description).build() };
            let attributes = metric
                .attributes
                .iter()
                .map(|attr| {
                    let constant_name = format_ident!("{}", attr.constant_name());
                    quote! { crate::metrics::MetricAttribute::#constant_name }
                })
                .collect::<Vec<_>>();
            let keys = quote! { std::sync::Arc::from([#(#attributes),*]) };
            let initializer = match (&metric.r#type, &metric.value_type) {
                (MetricType::Counter, MetricValueType::U64) => quote! {
                    #field_name: crate::metrics::Counter::new(
                        meter.u64_counter(#metric_name)#build,
                        #keys,
                    ),
                },
                (MetricType::Counter, MetricValueType::F64) => quote! {
                    #field_name: crate::metrics::Counter::new(
                        meter.f64_counter(#metric_name)#build,
                        #keys,
                    ),
                },
                (MetricType::UpDownCounter, MetricValueType::I64) => quote! {
                    #field_name: crate::metrics::UpDownCounter::new(
                        meter.i64_up_down_counter(#metric_name)#build,
                        #keys,
                    ),
                },
                (MetricType::UpDownCounter, MetricValueType::F64) => quote! {
                    #field_name: crate::metrics::UpDownCounter::new(
                        meter.f64_up_down_counter(#metric_name)#build,
                        #keys,
                    ),
                },
                (MetricType::Gauge, MetricValueType::U64) => quote! {
                    #field_name: crate::metrics::Gauge::new(
                        meter.u64_gauge(#metric_name)#build,
                        #keys,
                    ),
                },
                (MetricType::Gauge, MetricValueType::I64) => quote! {
                    #field_name: crate::metrics::Gauge::new(
                        meter.i64_gauge(#metric_name)#build,
                        #keys,
                    ),
                },
                (MetricType::Gauge, MetricValueType::F64) => quote! {
                    #field_name: crate::metrics::Gauge::new(
                        meter.f64_gauge(#metric_name)#build,
                        #keys,
                    ),
                },
                (MetricType::Histogram, MetricValueType::U64) => quote! {
                    #field_name: crate::metrics::Histogram::new(
                        meter.u64_histogram(#metric_name)#build,
                        #keys,
                    ),
                },
                (MetricType::Histogram, MetricValueType::F64) => quote! {
                    #field_name: crate::metrics::Histogram::new(
                        meter.f64_histogram(#metric_name)#build,
                        #keys,
                    ),
                },
                (t, vt) => {
                    return Err(syn::Error::new(
                        proc_macro2::Span::call_site(),
                        format!("invalid metric: {t:?} {vt:?}"),
                    ))
                }
            };
            Ok(initializer)
        })
        .collect::<Result<Vec<_>, syn::Error>>()?;

    let tokens = quote! {
        #[doc = "A registry of all the metrics reported by the application."]
        #[doc = ""]
        #[doc = "We could derive `Clone` for the registry, since all the instruments"]
        #[doc = "use `Arc` for their states internally. But we do not do so since"]
        #[doc = "it is more performant to clone an `Arc` of the registry instead."]
        pub struct MetricRegistry {
            #(#fields)*
        }

        impl MetricRegistry {
            pub fn new(meter: &opentelemetry::metrics::Meter) -> Self {
                Self {
                    #(#initializers)*
                }
            }
        }
    };

    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);
    std::fs::write(
        out_dir.join("metric_registry.rs"),
        prettyplease::unparse(&syn::parse2(tokens)?),
    )?;
    Ok(())
}

fn build_metric_attributes() -> Result<(), Box<dyn std::error::Error>> {
    let path = "src/metrics/data/attributes.yaml";
    println!("cargo:rerun-if-changed={path}");

    let content = std::fs::read_to_string(path)?;
    let attrs: Vec<MetricAttribute> = serde_yaml::from_str(&content)?;

    let constants = attrs
        .iter()
        .map(|attr| {
            let description = &attr.description;
            let key = &attr.name.key();
            let constant_name = format_ident!("{}", &attr.name.constant_name());
            quote! {
                #[doc = #description]
                pub const #constant_name: &str = #key;
            }
        })
        .collect::<Vec<_>>();

    let tokens = quote! {
        #[doc = "Common metric attribute names."]
        pub struct MetricAttribute;

        impl MetricAttribute {
            #(#constants)*
        }
    };

    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);
    std::fs::write(
        out_dir.join("metric_attributes.rs"),
        prettyplease::unparse(&syn::parse2(tokens)?),
    )?;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=build.rs");
    build_metric_registry()?;
    build_metric_attributes()?;
    Ok(())
}
