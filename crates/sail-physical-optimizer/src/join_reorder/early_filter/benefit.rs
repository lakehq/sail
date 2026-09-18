//! Conservative work estimates for optional semijoin reductions.

use datafusion::common::{ColumnStatistics, NullEquality, ScalarValue};

use super::*;

// Estimates omit cache effects, skew, and execution scheduling. Require headroom
// rather than accepting a candidate at the estimated break-even point.
const BENEFIT_MARGIN: f64 = 1.5;

pub(super) fn worthwhile(
    target: &Arc<dyn ExecutionPlan>,
    keys: &[usize],
    restriction: &Restriction,
    downstream_work: f64,
    options: &super::super::JoinReorderOptions,
) -> Result<bool> {
    let target_stats = StatisticsContext::new().compute(target.as_ref(), &StatisticsArgs::new())?;
    let Some(&target_rows) = target_stats.num_rows.get_value() else {
        return Ok(false);
    };
    if target_rows <= restriction.rows.saturating_mul(4) {
        return Ok(false);
    }
    let source_stats =
        StatisticsContext::new().compute(restriction.keys.as_ref(), &StatisticsArgs::new())?;
    let retained = keys
        .iter()
        .enumerate()
        .filter_map(|(i, &key)| {
            retained_fraction(
                &source_stats.column_statistics[i],
                restriction.rows,
                &target_stats.column_statistics[key],
                target_rows,
                restriction.null_equality,
            )
        })
        // A composite match must satisfy every component. Do not multiply
        // selectivities: the key columns may be fully correlated.
        .reduce(f64::min);
    let Some(retained) = retained else {
        return Ok(false);
    };
    let rows = target_rows as f64;
    let saved = rows * (1.0 - retained) * downstream_work;
    let added = restriction.scan_rows as f64 * options.probe_side_weight
        + restriction.rows as f64 * options.build_side_weight
        + rows * options.probe_side_weight
        + rows * retained * options.output_weight;
    Ok(saved > BENEFIT_MARGIN * added)
}

fn retained_fraction(
    source: &ColumnStatistics,
    source_rows: usize,
    target: &ColumnStatistics,
    target_rows: usize,
    null_equality: NullEquality,
) -> Option<f64> {
    if target_rows == 0 {
        return None;
    }
    let null_fraction = match null_equality {
        // Ignoring unknown null counts overestimates surviving rows, which is
        // conservative for an ordinary equality reduction.
        NullEquality::NullEqualsNothing => 0.0,
        // Null-safe equality can retain all null rows. Do not assume a nullable
        // target is null-free when its statistics are missing.
        NullEquality::NullEqualsNull => {
            (*target.null_count.get_value()? as f64 / target_rows as f64).min(1.0)
        }
    };
    let range = integer_range(target);
    let source_range = integer_range(source);
    let overlap = range
        .zip(source_range)
        .map(|((lo, hi), (slo, shi))| (hi.min(shi) - lo.max(slo) + 1).max(0) as f64);
    if overlap == Some(0.0) {
        return Some(null_fraction);
    }

    // Prefer target NDV. Parquet often supplies only min/max: for integral keys,
    // use a uniform range estimate only when the target has enough rows to fill
    // that range. In particular, never divide by the dimension's whole domain:
    // facts can occupy just one year of a multi-century date dimension.
    let domain = match target.distinct_count.get_value() {
        Some(&ndv) if ndv > 0 => ndv as f64,
        Some(_) => return Some(null_fraction),
        None => {
            let (lo, hi) = range?;
            let width = (hi - lo + 1) as f64;
            let non_null_rows =
                target_rows.saturating_sub(target.null_count.get_value().copied().unwrap_or(0));
            if width <= 0.0 || width > non_null_rows as f64 {
                return None;
            }
            width
        }
    };
    // Source rows are an upper bound on eligible distinct keys, including
    // duplicate dimension keys. Range intersection can tighten that bound.
    let eligible = overlap.map_or(source_rows as f64, |n| n.min(source_rows as f64));
    Some(null_fraction + (1.0 - null_fraction) * (eligible / domain).min(1.0))
}

fn integer_range(stats: &ColumnStatistics) -> Option<(i128, i128)> {
    fn value(v: &ScalarValue) -> Option<i128> {
        match v {
            ScalarValue::Int8(Some(v)) => Some(i128::from(*v)),
            ScalarValue::Int16(Some(v)) => Some(i128::from(*v)),
            ScalarValue::Int32(Some(v)) | ScalarValue::Date32(Some(v)) => Some(i128::from(*v)),
            ScalarValue::Int64(Some(v)) => Some(i128::from(*v)),
            ScalarValue::UInt8(Some(v)) => Some(i128::from(*v)),
            ScalarValue::UInt16(Some(v)) => Some(i128::from(*v)),
            ScalarValue::UInt32(Some(v)) => Some(i128::from(*v)),
            ScalarValue::UInt64(Some(v)) => Some(i128::from(*v)),
            _ => None,
        }
    }
    let lo = value(stats.min_value.get_value()?)?;
    let hi = value(stats.max_value.get_value()?)?;
    (lo <= hi).then_some((lo, hi))
}
