use std::borrow::Cow;
use std::sync::Arc;

use datafusion::physical_plan::metrics::{Count, Time};
use num_traits::ToPrimitive;
use opentelemetry::KeyValue;

/// A key-value pair for metric attributes.
/// This is a restriction over [`opentelemetry::KeyValue`] to only allow
/// static string keys and clone-on-write string values.
pub type Attribute = (&'static str, Cow<'static, str>);

/// A trait for instruments that can emit measurements with attributes.
pub trait InstrumentEmittable<T> {
    fn emit(&self, value: T, attributes: Cow<'_, [Attribute]>);
}

/// A builder for emitting measurements with attributes
/// using an instrument that implements [`InstrumentEmittable`].
pub struct InstrumentEmitter<'a, I, T> {
    instrument: &'a I,
    value: T,
    attributes: Cow<'a, [Attribute]>,
}

impl<'a, I, T> InstrumentEmitter<'a, I, T>
where
    I: InstrumentEmittable<T>,
{
    pub fn with_attributes(mut self, attributes: &'a [Attribute]) -> Self {
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

    pub fn with_attribute(mut self, attribute: Attribute) -> Self {
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
    inner: opentelemetry::metrics::Counter<T>,
    keys: Arc<[&'static str]>,
}

impl<T> Counter<T> {
    pub fn new(inner: opentelemetry::metrics::Counter<T>, keys: Arc<[&'static str]>) -> Self {
        Counter { inner, keys }
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
    inner: opentelemetry::metrics::UpDownCounter<T>,
    keys: Arc<[&'static str]>,
}

impl<T> UpDownCounter<T> {
    pub fn new(inner: opentelemetry::metrics::UpDownCounter<T>, keys: Arc<[&'static str]>) -> Self {
        UpDownCounter { inner, keys }
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
    inner: opentelemetry::metrics::Gauge<T>,
    keys: Arc<[&'static str]>,
}

impl<T> Gauge<T> {
    pub fn new(inner: opentelemetry::metrics::Gauge<T>, keys: Arc<[&'static str]>) -> Self {
        Gauge { inner, keys }
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
    inner: opentelemetry::metrics::Histogram<T>,
    keys: Arc<[&'static str]>,
}

impl<T> Histogram<T> {
    pub fn new(inner: opentelemetry::metrics::Histogram<T>, keys: Arc<[&'static str]>) -> Self {
        Histogram { inner, keys }
    }

    pub fn recorder<V>(&self, value: V) -> InstrumentEmitter<'_, Self, V> {
        InstrumentEmitter {
            instrument: self,
            value,
            attributes: Cow::Borrowed(&[]),
        }
    }
}

/// Validates that all attribute keys are in the allowed set of keys
/// for the instrument.
/// This function only performs validation in debug builds
/// for performance reasons.
#[inline]
fn validate_attributes(keys: &[&'static str], attributes: &[Attribute]) {
    #[cfg(debug_assertions)]
    attributes.iter().for_each(|(k, _)| {
        #[expect(clippy::panic)]
        if !keys.contains(k) {
            panic!("invalid metric attribute key: {k}");
        }
    });
}

/// Converts a list of attributes into OpenTelemetry key-value pairs.
#[inline]
fn build_attributes(attributes: Cow<'_, [Attribute]>) -> Vec<KeyValue> {
    attributes
        .into_owned()
        .into_iter()
        .map(|(k, v)| KeyValue::new(k, v))
        .collect::<Vec<_>>()
}

macro_rules! impl_primitive_emittable {
    ($instrument: ident, $value_type: ty, $method: ident) => {
        impl InstrumentEmittable<$value_type> for $instrument<$value_type> {
            fn emit(&self, value: $value_type, attributes: Cow<'_, [Attribute]>) {
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
    fn emit(&self, value: &'a Time, attributes: Cow<'_, [Attribute]>) {
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
    fn emit(&self, value: &'a Count, attributes: Cow<'_, [Attribute]>) {
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
        attributes: Cow<'_, [Attribute]>,
    ) {
        validate_attributes(&self.keys, &attributes);
        // Ignore the measurement if conversion failed.
        if let Ok(v) = u64::try_from(value.value()) {
            self.inner.record(v, &build_attributes(attributes));
        }
    }
}

impl InstrumentEmittable<usize> for Gauge<u64> {
    fn emit(&self, value: usize, attributes: Cow<'_, [Attribute]>) {
        validate_attributes(&self.keys, &attributes);
        // Ignore the measurement if conversion failed.
        if let Ok(v) = u64::try_from(value) {
            self.inner.record(v, &build_attributes(attributes));
        }
    }
}
