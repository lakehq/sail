//! Window frame typing depends only on resolved ORDER BY types and constants.

use arrow_schema::{DataType, IntervalUnit};
use datafusion_common::{Result, ScalarValue, plan_err};
use datafusion_expr::{WindowFrame, WindowFrameBound, WindowFrameUnits};

pub fn resolve_frame(mut frame: WindowFrame, order_types: &[DataType]) -> Result<WindowFrame> {
    let finite_offset = |bound: &WindowFrameBound| match bound {
        WindowFrameBound::Preceding(value) | WindowFrameBound::Following(value) => !value.is_null(),
        WindowFrameBound::CurrentRow => false,
    };
    let target = match frame.units {
        WindowFrameUnits::Rows | WindowFrameUnits::Groups => DataType::UInt64,
        WindowFrameUnits::Range => {
            if (finite_offset(&frame.start_bound) || finite_offset(&frame.end_bound))
                && order_types.len() != 1
            {
                return plan_err!("a RANGE offset requires exactly one ORDER BY expression");
            }
            let Some(order_type) = order_types.first() else {
                return plan_err!("a RANGE frame requires ORDER BY");
            };
            range_type(order_type)?
        }
    };
    frame.start_bound = resolve_bound(frame.start_bound, &target)?;
    frame.end_bound = resolve_bound(frame.end_bound, &target)?;
    if matches!(&frame.start_bound, WindowFrameBound::Following(value) if value.is_null())
        || matches!(&frame.end_bound, WindowFrameBound::Preceding(value) if value.is_null())
        || reversed_bounds(&frame.start_bound, &frame.end_bound)?
    {
        return plan_err!("window frame start must not follow its end");
    }
    Ok(frame)
}

fn range_type(data_type: &DataType) -> Result<DataType> {
    match data_type {
        DataType::Dictionary(_, value) => range_type(value),
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _) => {
            Ok(DataType::Interval(IntervalUnit::MonthDayNano))
        }
        value if value.is_numeric() || value.is_string() || value.is_null() => Ok(value.clone()),
        value => plan_err!("unsupported RANGE ordering type: {value}"),
    }
}

fn resolve_bound(bound: WindowFrameBound, target: &DataType) -> Result<WindowFrameBound> {
    let (value, preceding) = match bound {
        WindowFrameBound::CurrentRow => return Ok(WindowFrameBound::CurrentRow),
        WindowFrameBound::Preceding(value) => (value, true),
        WindowFrameBound::Following(value) => (value, false),
    };
    let value = if value.is_null() {
        ScalarValue::try_from(target)?
    } else {
        let value = value.cast_to(target)?;
        if value.is_null() {
            return plan_err!("window frame offset is out of range for {target}");
        }
        value
    };
    Ok(if preceding {
        WindowFrameBound::Preceding(value)
    } else {
        WindowFrameBound::Following(value)
    })
}

fn reversed_bounds(start: &WindowFrameBound, end: &WindowFrameBound) -> Result<bool> {
    use WindowFrameBound::{CurrentRow, Following, Preceding};
    let zero = |value: &ScalarValue| -> Result<bool> {
        Ok(value == &ScalarValue::new_zero(&value.data_type())?)
    };
    Ok(match (start, end) {
        (Preceding(a), Preceding(b)) => !a.is_null() && (b.is_null() || a < b),
        (Following(a), Following(b)) => !b.is_null() && (a.is_null() || a > b),
        (CurrentRow, Preceding(b)) => b.is_null() || !zero(b)?,
        (Following(a), CurrentRow) => a.is_null() || !zero(a)?,
        (Following(a), Preceding(b)) => a.is_null() || b.is_null() || !zero(a)? || !zero(b)?,
        _ => false,
    })
}
