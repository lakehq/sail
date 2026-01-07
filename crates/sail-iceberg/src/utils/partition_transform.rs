use crate::spec::transform::Transform;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionFieldSpec {
    pub source_column: String,
    pub field_name: String,
    pub transform: Transform,
}

pub fn partition_field_name(source_column: &str, transform: Transform) -> String {
    match transform {
        Transform::Identity => source_column.to_string(),
        Transform::Year => format!("{source_column}_year"),
        Transform::Month => format!("{source_column}_month"),
        Transform::Day => format!("{source_column}_day"),
        Transform::Hour => format!("{source_column}_hour"),
        Transform::Bucket(_) => format!("{source_column}_bucket"),
        Transform::Truncate(_) => format!("{source_column}_trunc"),
        Transform::Void => format!("{source_column}_null"),
        Transform::Unknown => format!("{source_column}_unknown"),
    }
}

pub fn format_partition_expr(source_column: &str, transform: Transform) -> String {
    match transform {
        Transform::Identity => source_column.to_string(),
        Transform::Year => format!("years({source_column})"),
        Transform::Month => format!("months({source_column})"),
        Transform::Day => format!("days({source_column})"),
        Transform::Hour => format!("hours({source_column})"),
        Transform::Bucket(n) => format!("bucket({n}, {source_column})"),
        Transform::Truncate(w) => format!("truncate({source_column}, {w})"),
        // keep a reasonable fallback for uncommon transforms
        other => format!("{other}({source_column})"),
    }
}

fn split_args(args: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut buf = String::new();
    let mut depth = 0u32;
    for ch in args.chars() {
        match ch {
            '(' => {
                depth += 1;
                buf.push(ch);
            }
            ')' => {
                depth = depth.saturating_sub(1);
                buf.push(ch);
            }
            ',' if depth == 0 => {
                let s = buf.trim();
                if !s.is_empty() {
                    out.push(s.to_string());
                }
                buf.clear();
            }
            _ => buf.push(ch),
        }
    }
    let s = buf.trim();
    if !s.is_empty() {
        out.push(s.to_string());
    }
    out
}

fn normalize_object_name(raw: &str) -> String {
    fn strip_quotes(s: &str) -> &str {
        let s = s.trim();
        if s.len() >= 2
            && ((s.starts_with('`') && s.ends_with('`'))
                || (s.starts_with('"') && s.ends_with('"')))
        {
            return &s[1..s.len() - 1];
        }
        s
    }
    raw.split('.')
        .map(strip_quotes)
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .collect::<Vec<_>>()
        .join(".")
}

pub fn parse_partition_field_expr(raw: &str) -> Result<PartitionFieldSpec, String> {
    let s = raw.trim();
    if s.is_empty() {
        return Err("empty partition field".to_string());
    }

    let Some(lp) = s.find('(') else {
        // identity partitioning by column
        let col = normalize_object_name(s);
        return Ok(PartitionFieldSpec {
            source_column: col.clone(),
            field_name: col,
            transform: Transform::Identity,
        });
    };
    if !s.ends_with(')') {
        return Err(format!("invalid partition expression (missing ')'): {raw}"));
    }

    let fname = s[..lp].trim().to_ascii_lowercase();
    let inner = &s[lp + 1..s.len() - 1];
    let args = split_args(inner);

    let (transform, source_column) = match fname.as_str() {
        "years" | "year" => {
            if args.len() != 1 {
                return Err(format!(
                    "years() expects 1 argument, got {}: {raw}",
                    args.len()
                ));
            }
            (Transform::Year, normalize_object_name(&args[0]))
        }
        "months" | "month" => {
            if args.len() != 1 {
                return Err(format!(
                    "months() expects 1 argument, got {}: {raw}",
                    args.len()
                ));
            }
            (Transform::Month, normalize_object_name(&args[0]))
        }
        "days" | "day" => {
            if args.len() != 1 {
                return Err(format!(
                    "days() expects 1 argument, got {}: {raw}",
                    args.len()
                ));
            }
            (Transform::Day, normalize_object_name(&args[0]))
        }
        "hours" | "hour" => {
            if args.len() != 1 {
                return Err(format!(
                    "hours() expects 1 argument, got {}: {raw}",
                    args.len()
                ));
            }
            (Transform::Hour, normalize_object_name(&args[0]))
        }
        "bucket" => {
            if args.len() != 2 {
                return Err(format!(
                    "bucket() expects 2 arguments, got {}: {raw}",
                    args.len()
                ));
            }
            let n: u32 = args[0]
                .trim()
                .parse()
                .map_err(|e| format!("bucket() expects integer first arg: {e}"))?;
            (Transform::Bucket(n), normalize_object_name(&args[1]))
        }
        "truncate" => {
            if args.len() != 2 {
                return Err(format!(
                    "truncate() expects 2 arguments, got {}: {raw}",
                    args.len()
                ));
            }
            let w: u32 = args[1]
                .trim()
                .parse()
                .map_err(|e| format!("truncate() expects integer width as second arg: {e}"))?;
            (Transform::Truncate(w), normalize_object_name(&args[0]))
        }
        "identity" => {
            if args.len() != 1 {
                return Err(format!(
                    "identity() expects 1 argument, got {}: {raw}",
                    args.len()
                ));
            }
            (Transform::Identity, normalize_object_name(&args[0]))
        }
        other => {
            return Err(format!(
                "unsupported partition transform '{other}' in: {raw}"
            ));
        }
    };

    if source_column.is_empty() {
        return Err(format!("partition transform missing source column: {raw}"));
    }
    if transform != Transform::Identity && source_column.contains('(') {
        return Err(format!(
            "partition transform source must be a column reference, got: {source_column}"
        ));
    }

    Ok(PartitionFieldSpec {
        field_name: partition_field_name(&source_column, transform),
        source_column,
        transform,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_identity_column() -> Result<(), String> {
        let f = parse_partition_field_expr("event_date")?;
        assert_eq!(f.source_column, "event_date");
        assert_eq!(f.transform, Transform::Identity);
        assert_eq!(f.field_name, "event_date");
        Ok(())
    }

    #[test]
    fn parse_years() -> Result<(), String> {
        let f = parse_partition_field_expr("years(event_date)")?;
        assert_eq!(f.source_column, "event_date");
        assert_eq!(f.transform, Transform::Year);
        assert_eq!(f.field_name, "event_date_year");
        Ok(())
    }

    #[test]
    fn parse_months_with_spaces() -> Result<(), String> {
        let f = parse_partition_field_expr("months( event_ts )")?;
        assert_eq!(f.source_column, "event_ts");
        assert_eq!(f.transform, Transform::Month);
        assert_eq!(f.field_name, "event_ts_month");
        Ok(())
    }

    #[test]
    fn parse_bucket() -> Result<(), String> {
        let f = parse_partition_field_expr("bucket(16, user_id)")?;
        assert_eq!(f.source_column, "user_id");
        assert_eq!(f.transform, Transform::Bucket(16));
        assert_eq!(f.field_name, "user_id_bucket");
        Ok(())
    }

    #[test]
    fn parse_truncate() -> Result<(), String> {
        let f = parse_partition_field_expr("truncate(user_id, 8)")?;
        assert_eq!(f.source_column, "user_id");
        assert_eq!(f.transform, Transform::Truncate(8));
        assert_eq!(f.field_name, "user_id_trunc");
        Ok(())
    }
}
