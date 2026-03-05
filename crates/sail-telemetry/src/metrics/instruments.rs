use std::borrow::Cow;
use std::sync::Arc;

use datafusion::physical_plan::metrics::{Count, Time};
use num_traits::ToPrimitive;
use opentelemetry::metrics::{HistogramBuilder, InstrumentBuilder};

use crate::common::KeyValue;

/// A trait for instruments that can emit measurements with attributes.
pub trait InstrumentEmittable<T> {
    fn emit(&self, value: T, attributes: Cow<'_, [KeyValue]>);
}

/// A builder for emitting measurements with attributes
/// using an instrument that implements [`InstrumentEmittable`].
pub struct InstrumentEmitter<'a, I, T> {
    instrument: &'a I,
    value: T,
    attributes: Cow<'a, [KeyValue]>,
}

impl<'a, I, T> InstrumentEmitter<'a, I, T>
where
    I: InstrumentEmittable<T>,
{
    pub fn with_attributes(mut self, attributes: &'a [KeyValue]) -> Self {
        if self.attributes.is_empty() {
            self.attributes = Cow::Borrowed(attributes);
        } else {
            match self.attributes {
                Cow::Borrowed(existing) => {
                    let mut combined = Vec::with_capacity(existing.len() + attributes.len());
                    combined.extend_from_slice(existing);
                    combined.extend_from_slice(attributes);
                    self.attributes = Cow::Owned(combined);
                }
                Cow::Owned(ref mut existing) => {
                    existing.extend_from_slice(attributes);
                }
            }
        }
        self
    }

    pub fn with_attribute(mut self, attribute: KeyValue) -> Self {
        match self.attributes {
            Cow::Borrowed(existing) => {
                let mut combined = Vec::with_capacity(existing.len() + 1);
                combined.extend_from_slice(existing);
                combined.push(attribute);
                self.attributes = Cow::Owned(combined);
            }
            Cow::Owned(ref mut existing) => {
                existing.push(attribute);
            }
        }
        self
    }

    pub fn with_optional_attribute<V: ToString>(self, key: &'static str, value: Option<V>) -> Self {
        if let Some(value) = value {
            self.with_attribute((key, Cow::Owned(value.to_string())))
        } else {
            self
        }
    }

    pub fn emit(self) {
        self.instrument.emit(self.value, self.attributes);
    }
}

/// A wrapper around [`opentelemetry::metrics::Counter`] that enforces
/// allowed attribute keys.
#[derive(Clone)]
pub struct Counter<T> {
    name: Cow<'static, str>,
    inner: opentelemetry::metrics::Counter<T>,
    keys: Arc<[&'static str]>,
}

impl<T> Counter<T> {
    pub fn name(&self) -> Cow<'static, str> {
        self.name.clone()
    }

    pub fn adder<V>(&self, value: V) -> InstrumentEmitter<'_, Self, V> {
        InstrumentEmitter {
            instrument: self,
            value,
            attributes: Cow::Borrowed(&[]),
        }
    }
}

/// A wrapper around [`opentelemetry::metrics::Counter`] that enforces
/// allowed attribute keys.
#[derive(Clone)]
pub struct UpDownCounter<T> {
    name: Cow<'static, str>,
    inner: opentelemetry::metrics::UpDownCounter<T>,
    keys: Arc<[&'static str]>,
}

impl<T> UpDownCounter<T> {
    pub fn name(&self) -> Cow<'static, str> {
        self.name.clone()
    }

    pub fn adder<V>(&self, value: V) -> InstrumentEmitter<'_, Self, V> {
        InstrumentEmitter {
            instrument: self,
            value,
            attributes: Cow::Borrowed(&[]),
        }
    }
}

/// A wrapper around [`opentelemetry::metrics::Gauge`] that enforces
/// allowed attribute keys.
#[derive(Clone)]
pub struct Gauge<T> {
    name: Cow<'static, str>,
    inner: opentelemetry::metrics::Gauge<T>,
    keys: Arc<[&'static str]>,
}

impl<T> Gauge<T> {
    pub fn name(&self) -> Cow<'static, str> {
        self.name.clone()
    }

    pub fn recorder<V>(&self, value: V) -> InstrumentEmitter<'_, Self, V> {
        InstrumentEmitter {
            instrument: self,
            value,
            attributes: Cow::Borrowed(&[]),
        }
    }
}

/// A wrapper around [`opentelemetry::metrics::Histogram`] that enforces
/// allowed attribute keys.
#[derive(Clone)]
pub struct Histogram<T> {
    name: Cow<'static, str>,
    inner: opentelemetry::metrics::Histogram<T>,
    keys: Arc<[&'static str]>,
}

impl<T> Histogram<T> {
    pub fn name(&self) -> Cow<'static, str> {
        self.name.clone()
    }

    pub fn recorder<V>(&self, value: V) -> InstrumentEmitter<'_, Self, V> {
        InstrumentEmitter {
            instrument: self,
            value,
            attributes: Cow::Borrowed(&[]),
        }
    }
}

macro_rules! impl_new_instrument {
    ($instrument:ty, $builder:ty) => {
        impl $instrument {
            pub fn new(builder: $builder, keys: Arc<[&'static str]>) -> Self {
                Self {
                    name: builder.name.clone(),
                    inner: builder.build(),
                    keys,
                }
            }
        }
    };
}

impl_new_instrument!(
    Counter<u64>,
    InstrumentBuilder<opentelemetry::metrics::Counter<u64>>
);
impl_new_instrument!(
    Counter<f64>,
    InstrumentBuilder<opentelemetry::metrics::Counter<f64>>
);
impl_new_instrument!(
    UpDownCounter<i64>,
    InstrumentBuilder<opentelemetry::metrics::UpDownCounter<i64>>
);
impl_new_instrument!(
    UpDownCounter<f64>,
    InstrumentBuilder<opentelemetry::metrics::UpDownCounter<f64>>
);
impl_new_instrument!(
    Gauge<u64>,
    InstrumentBuilder<opentelemetry::metrics::Gauge<u64>>
);
impl_new_instrument!(
    Gauge<i64>,
    InstrumentBuilder<opentelemetry::metrics::Gauge<i64>>
);
impl_new_instrument!(
    Gauge<f64>,
    InstrumentBuilder<opentelemetry::metrics::Gauge<f64>>
);
impl_new_instrument!(
    Histogram<u64>,
    HistogramBuilder<opentelemetry::metrics::Histogram<u64>>
);
impl_new_instrument!(
    Histogram<f64>,
    HistogramBuilder<opentelemetry::metrics::Histogram<f64>>
);

/// Validates that all attribute keys are in the allowed set of keys
/// for the instrument.
/// This function only performs validation in debug builds
/// for performance reasons.
#[inline]
fn validate_attributes(keys: &[&'static str], attributes: &[KeyValue]) {
    #[cfg(debug_assertions)]
    attributes.iter().for_each(|(k, _)| {
        #[expect(clippy::panic)]
        if !keys.contains(k) {
            panic!("invalid metric attribute key: {k}");
        }
    });
    #[cfg(not(debug_assertions))]
    {
        let _ = keys;
        let _ = attributes;
    }
}

/// Converts a list of attributes into OpenTelemetry key-value pairs.
#[inline]
fn build_attributes(attributes: Cow<'_, [KeyValue]>) -> Vec<opentelemetry::KeyValue> {
    attributes
        .into_owned()
        .into_iter()
        .map(|(k, v)| opentelemetry::KeyValue::new(k, v))
        .collect::<Vec<_>>()
}

macro_rules! impl_primitive_emittable {
    ($instrument: ident, $value_type: ty, $method: ident) => {
        impl InstrumentEmittable<$value_type> for $instrument<$value_type> {
            fn emit(&self, value: $value_type, attributes: Cow<'_, [KeyValue]>) {
                validate_attributes(&self.keys, &attributes);
                self.inner.$method(value, &build_attributes(attributes));
            }
        }
    };
}

impl_primitive_emittable!(Counter, u64, add);
impl_primitive_emittable!(Counter, f64, add);
impl_primitive_emittable!(UpDownCounter, i64, add);
impl_primitive_emittable!(UpDownCounter, f64, add);
impl_primitive_emittable!(Gauge, u64, record);
impl_primitive_emittable!(Gauge, i64, record);
impl_primitive_emittable!(Gauge, f64, record);
impl_primitive_emittable!(Histogram, u64, record);
impl_primitive_emittable!(Histogram, f64, record);

impl<'a> InstrumentEmittable<&'a Time> for Gauge<f64> {
    fn emit(&self, value: &'a Time, attributes: Cow<'_, [KeyValue]>) {
        validate_attributes(&self.keys, &attributes);
        // The DataFusion time metric is measured in nanoseconds while
        // OpenTelemetry semantic conventions say that durations SHOULD be measured in seconds.
        // See also: <https://opentelemetry.io/docs/specs/semconv/general/metrics/#units>
        let nanos = value.value();
        let whole = nanos / 1_000_000_000;
        let fraction = nanos % 1_000_000_000;
        let seconds = match (whole.to_f64(), fraction.to_f64()) {
            (Some(w), Some(f)) => Some(w + f / 1_000_000_000f64),
            _ => None,
        };
        // Ignore the measurement if conversion to seconds failed.
        if let Some(seconds) = seconds {
            self.inner.record(seconds, &build_attributes(attributes));
        }
    }
}

// The DataFusion `Count` measurement is emitted using a gauge
// since we can only access its current value.
impl<'a> InstrumentEmittable<&'a Count> for Gauge<u64> {
    fn emit(&self, value: &'a Count, attributes: Cow<'_, [KeyValue]>) {
        validate_attributes(&self.keys, &attributes);
        // Ignore the measurement if conversion failed.
        if let Ok(count) = u64::try_from(value.value()) {
            self.inner.record(count, &build_attributes(attributes));
        }
    }
}

impl<'a> InstrumentEmittable<&'a datafusion::physical_plan::metrics::Gauge> for Gauge<u64> {
    fn emit(
        &self,
        value: &'a datafusion::physical_plan::metrics::Gauge,
        attributes: Cow<'_, [KeyValue]>,
    ) {
        validate_attributes(&self.keys, &attributes);
        // Ignore the measurement if conversion failed.
        if let Ok(v) = u64::try_from(value.value()) {
            self.inner.record(v, &build_attributes(attributes));
        }
    }
}

impl InstrumentEmittable<usize> for Gauge<u64> {
    fn emit(&self, value: usize, attributes: Cow<'_, [KeyValue]>) {
        validate_attributes(&self.keys, &attributes);
        // Ignore the measurement if conversion failed.
        if let Ok(v) = u64::try_from(value) {
            self.inner.record(v, &build_attributes(attributes));
        }
    }
}
