use std::collections::VecDeque;
use std::fmt::Debug;

use datafusion_common::{plan_datafusion_err, plan_err, Result};
use either::Either;
use regex::Regex;

/// A trait for taking items from a container of expected size.
pub(crate) trait ItemTaker {
    type Item;

    fn zero(self) -> Result<()>;
    fn one(self) -> Result<Self::Item>;
    fn two(self) -> Result<(Self::Item, Self::Item)>;
    fn three(self) -> Result<(Self::Item, Self::Item, Self::Item)>;
    #[allow(clippy::type_complexity)]
    fn four(self) -> Result<(Self::Item, Self::Item, Self::Item, Self::Item)>;
    fn at_least_one(self) -> Result<(Self::Item, Vec<Self::Item>)>;
    fn one_or_more(self) -> Result<Either<Self::Item, Vec<Self::Item>>>;
}

impl<T: Debug> ItemTaker for Vec<T> {
    type Item = T;

    fn zero(self) -> Result<()> {
        if !self.is_empty() {
            return plan_err!("zero values expected: {:?}", self);
        }
        Ok(())
    }

    fn one(mut self) -> Result<T> {
        if self.len() != 1 {
            return plan_err!("one value expected: {:?}", self);
        }
        Ok(self.pop().unwrap())
    }

    fn two(mut self) -> Result<(T, T)> {
        if self.len() != 2 {
            return plan_err!("two values expected: {:?}", self);
        }
        let second = self.pop().unwrap();
        let first = self.pop().unwrap();
        Ok((first, second))
    }

    fn three(mut self) -> Result<(T, T, T)> {
        if self.len() != 3 {
            return plan_err!("three values expected: {:?}", self);
        }
        let third = self.pop().unwrap();
        let second = self.pop().unwrap();
        let first = self.pop().unwrap();
        Ok((first, second, third))
    }

    fn four(mut self) -> Result<(T, T, T, T)> {
        if self.len() != 4 {
            return plan_err!("four values expected: {:?}", self);
        }
        let fourth = self.pop().unwrap();
        let third = self.pop().unwrap();
        let second = self.pop().unwrap();
        let first = self.pop().unwrap();
        Ok((first, second, third, fourth))
    }

    fn at_least_one(self) -> Result<(T, Vec<T>)> {
        if self.is_empty() {
            return plan_err!("at least one value expected: {:?}", self);
        }
        let mut deque: VecDeque<T> = VecDeque::from(self);
        let first = deque.pop_front().unwrap();
        let vec: Vec<T> = Vec::from(deque);
        Ok((first, vec))
    }

    fn one_or_more(mut self) -> Result<Either<T, Vec<T>>> {
        if self.is_empty() {
            return plan_err!("one or more values expected: {:?}", self);
        }
        if self.len() == 1 {
            Ok(Either::Left(self.pop().unwrap()))
        } else {
            Ok(Either::Right(self))
        }
    }
}

pub fn spark_datetime_format_to_chrono_strftime(format: &str) -> Result<String> {
    // TODO: This doesn't cover everything.
    //  https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html
    //  https://docs.rs/chrono/latest/chrono/format/strftime/index.html#specifiers

    let patterns = [
        // Fractional seconds patterns (from nanoseconds to deciseconds)
        ("SSSSSSSSS", "%.9f"), // Nanoseconds
        ("SSSSSSSS", "%.8f"),
        ("SSSSSSS", "%.7f"),
        ("SSSSSS", "%.6f"), // Microseconds
        ("SSSSS", "%.5f"),
        ("SSSS", "%.4f"),
        ("SSS", "%.3f"), // Milliseconds
        ("SS", "%.2f"),  // Centiseconds
        ("S", "%.1f"),   // Deciseconds
        // Year patterns
        ("yyyy", "%Y"),
        ("yyy", "%Y"),
        ("yy", "%y"),
        ("y", "%Y"),
        // Day-of-year pattern
        ("D", "%j"),
        // Month patterns
        ("MMMM", "%B"),
        ("MMM", "%b"),
        ("MM", "%m"),
        ("M", "%-m"),
        ("LLLL", "%B"),
        ("LLL", "%b"),
        ("LL", "%m"),
        ("L", "%-m"),
        // Day-of-month patterns
        ("dd", "%d"),
        ("d", "%-d"),
        // Weekday patterns
        ("EEEE", "%A"),
        ("EEE", "%a"),
        ("E", "%a"),
        // Hour patterns
        ("hh", "%I"), // 12-hour clock (01–12)
        ("h", "%-I"), // 12-hour clock (1–12)
        ("HH", "%H"), // 24-hour clock (00–23)
        ("H", "%-H"), // 24-hour clock (0–23)
        ("KK", "%I"), // 12-hour clock (01–12), but Spark's 'K' is 0–11
        ("K", "%l"),  // 12-hour clock (1–12), space-padded
        // Minute patterns
        ("mm", "%M"),
        ("m", "%-M"),
        // Second patterns
        ("ss", "%S"),
        ("s", "%-S"),
        // AM/PM
        ("a", "%p"),
        // Timezone patterns
        ("XXXXX", "%::z"), // ±HH:MM:SS
        ("XXXX", "%z"),    // ±HHMM
        ("XXX", "%:z"),    // ±HH:MM
        ("XX", "%z"),      // ±HHMM
        ("X", "%z"),       // ±HHMM
        ("xxxxx", "%::z"), // ±HH:MM:SS
        ("xxxx", "%z"),    // ±HHMM
        ("xxx", "%:z"),    // ±HH:MM
        ("xx", "%z"),      // ±HHMM
        ("x", "%z"),       // ±HHMM
        ("ZZZZZ", "%::z"), // ±HH:MM:SS
        ("ZZZZ", "%:z"),   // ±HH:MM
        ("ZZZ", "%z"),     // ±HHMM
        ("ZZ", "%z"),      // ±HHMM
        ("Z", "%z"),       // ±HHMM
        ("zzzz", "%Z"),
        ("zzz", "%Z"),
        ("zz", "%Z"),
        ("z", "%Z"),
        ("OOOO", "%Z"),
        ("OO", "%Z"),
        ("VV", "%Z"),
    ];

    // TODO: Create regex using lazy_static
    let mut result = format.to_string();
    for &(pattern, replacement) in &patterns {
        let regex_pattern = format!(r"(?P<pre>^|[^%]){}", regex::escape(pattern));
        let re = Regex::new(&regex_pattern).map_err(|e| {
            plan_datafusion_err!("failed to create regex pattern for '{pattern}': {e}")
        })?;
        let replacement_str = format!("${{pre}}{replacement}");
        result = re
            .replace_all(&result, replacement_str.as_str())
            .to_string()
    }

    Ok(result)
}
