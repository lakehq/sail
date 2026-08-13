/// Format an `f64` the way `java.lang.Double.toString` does, so Spark-compatible messages use
/// `2.0` rather than `2`, and `-2.0E-6` rather than `-0.000002` (Java switches to scientific
/// notation outside `[1e-3, 1e7)`).
pub fn format_spark_double(value: f64) -> String {
    if value.is_nan() {
        return "NaN".to_string();
    }
    if value.is_infinite() {
        return if value > 0.0 { "Infinity" } else { "-Infinity" }.to_string();
    }
    let magnitude = value.abs();
    if magnitude != 0.0 && !(1e-3..1e7).contains(&magnitude) {
        let formatted = format!("{value:e}");
        let (mantissa, exponent) = match formatted.split_once('e') {
            Some(parts) => parts,
            None => (formatted.as_str(), "0"),
        };
        let mantissa = if mantissa.contains('.') {
            mantissa.to_string()
        } else {
            format!("{mantissa}.0")
        };
        format!("{mantissa}E{exponent}")
    } else {
        let formatted = format!("{value}");
        if formatted.contains('.') {
            formatted
        } else {
            format!("{formatted}.0")
        }
    }
}
