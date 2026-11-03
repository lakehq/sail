use std::time::Duration;

use datafusion::prelude::SessionConfig;
use pyo3::Python;

use super::*;
use crate::session::SparkSessionOptions;

const TIME_ZONE_KEY: &str = SparkConfigKey::SPARK_SQL_SESSION_TIME_ZONE;

fn pair(key: &str, value: Option<&str>) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: value.map(Into::into),
    }
}

fn context() -> SparkResult<SessionContext> {
    Python::initialize();
    let spark = Arc::new(SparkSession::try_new(
        "timezone-test".into(),
        "".into(),
        SparkSessionOptions {
            execution_heartbeat_interval: Duration::from_secs(1),
        },
    )?);
    spark.set_config(vec![pair(TIME_ZONE_KEY, Some("UTC")).into()])?;
    let mut config = SessionConfig::new().with_extension(spark);
    config.options_mut().execution.time_zone = Some("UTC".parse()?);
    Ok(SessionContext::new_with_config(config))
}

fn assert_timezone(
    ctx: &SessionContext,
    raw: Option<&str>,
    effective: &str,
    arrow: &str,
) -> SparkResult<()> {
    let response = handle_config_get(ctx, vec![TIME_ZONE_KEY.into()])?;
    assert_eq!(response.pairs[0].value.as_deref(), raw);
    assert_eq!(
        ctx.extension::<SparkSession>()?
            .plan_config()?
            .session_timezone
            .as_ref(),
        effective
    );
    assert_eq!(
        ctx.copied_config()
            .options()
            .execution
            .time_zone
            .as_ref()
            .map(|tz| tz.as_str()),
        Some(arrow)
    );
    Ok(())
}

#[tokio::test]
async fn set_timezone_preserves_spark_spelling_and_normalizes_datafusion() -> SparkResult<()> {
    let ctx = context()?;
    // Spark SQLConfSuite keeps the supplied spelling, including GMT+8:00 and short IDs.
    for (raw, arrow) in [
        ("UTC", "UTC"),
        ("+08:00", "+08:00"),
        ("-08", "-08"),
        ("+0530", "+0530"),
        ("Asia/Kolkata", "Asia/Kolkata"),
        ("America/Chicago", "America/Chicago"),
        ("Z", "+00:00"),
        ("UT", "+00:00"),
        ("GMT+8:00", "+08:00"),
        ("UTC+08:00", "+08:00"),
        ("+5:30", "+05:30"),
        ("+05:30:00", "+05:30"),
        ("+053000", "+05:30"),
        ("PST", "America/Los_Angeles"),
        ("CST", "America/Chicago"),
        ("MIT", "Pacific/Apia"),
        // Preserve the existing Arrow-label fallback for offsets with seconds.
        ("+05:30:45", "UTC"),
    ] {
        handle_config_set(&ctx, vec![pair(TIME_ZONE_KEY, Some(raw))])?;
        assert_timezone(&ctx, Some(raw), raw, arrow)?;
    }
    Ok(())
}

#[tokio::test]
async fn invalid_timezone_keeps_both_configurations() -> SparkResult<()> {
    let ctx = context()?;
    handle_config_set(&ctx, vec![pair(TIME_ZONE_KEY, Some("PST"))])?;
    for invalid in ["Nope", "Asia/shanghai", "pst", "", "'Asia/Kolkata'"] {
        let error = handle_config_set(&ctx, vec![pair(TIME_ZONE_KEY, Some(invalid))]).unwrap_err();
        assert!(matches!(error, SparkError::DataFusionError(_)));
        assert_timezone(&ctx, Some("PST"), "PST", "America/Los_Angeles")?;
    }
    // Validate all timezone values before even the first pair is stored.
    assert!(
        handle_config_set(
            &ctx,
            vec![
                pair(TIME_ZONE_KEY, Some("Asia/Tokyo")),
                pair(TIME_ZONE_KEY, Some("Nope")),
            ]
        )
        .is_err()
    );
    assert_timezone(&ctx, Some("PST"), "PST", "America/Los_Angeles")?;
    Ok(())
}

#[tokio::test]
async fn later_config_failure_does_not_desynchronize_timezone() -> SparkResult<()> {
    let ctx = context()?;
    for invalid in [
        pair("missing.value", None),
        pair(SparkConfigKey::SPARK_SQL_SHUFFLE_PARTITIONS, Some("0")),
        pair(TIME_ZONE_KEY, None),
    ] {
        assert!(handle_config_set(&ctx, vec![pair(TIME_ZONE_KEY, Some("PST")), invalid,]).is_err());
        assert_timezone(&ctx, Some("PST"), "PST", "America/Los_Angeles")?;
        handle_config_set(&ctx, vec![pair(TIME_ZONE_KEY, Some("UTC"))])?;
    }
    Ok(())
}

#[tokio::test]
async fn unset_timezone_restores_effective_default() -> SparkResult<()> {
    let ctx = context()?;
    handle_config_set(&ctx, vec![pair(TIME_ZONE_KEY, Some("GMT+8:00"))])?;
    handle_config_set(&ctx, vec![pair("unrelated", Some("value"))])?;
    handle_config_unset(&ctx, vec!["unrelated".into()])?;
    assert_timezone(&ctx, Some("GMT+8:00"), "GMT+8:00", "+08:00")?;
    for _ in 0..2 {
        handle_config_unset(&ctx, vec![TIME_ZONE_KEY.into()])?;
        assert_timezone(&ctx, None, "UTC", "UTC")?;
    }
    Ok(())
}

#[tokio::test]
async fn sql_timezone_rejection_preserves_state_and_reset_restores_system() -> SparkResult<()> {
    let ctx = context()?;
    for key in [TIME_ZONE_KEY, DATAFUSION_TIME_ZONE_KEY] {
        let command = |node| spec::Plan::Command(spec::CommandPlan::new(node));
        let set = |value: &str| {
            command(spec::CommandNode::SetVariable {
                variable: key.into(),
                value: value.into(),
            })
        };
        handle_sql_config(&ctx, &set("GMT+8:00")).await?;
        assert_timezone(&ctx, Some("GMT+8:00"), "GMT+8:00", "+08:00")?;
        for invalid in ["Nope", "Asia/shanghai", "pst", "+18:00:01", " UTC "] {
            // Native SET decodes SQL literals; quote the value to preserve whitespace.
            let value = if key == DATAFUSION_TIME_ZONE_KEY {
                format!("'{invalid}'")
            } else {
                invalid.to_string()
            };
            let error = handle_sql_config(&ctx, &set(&value)).await.unwrap_err();
            assert!(matches!(error, SparkError::InvalidArgument(_)));
            assert_timezone(&ctx, Some("GMT+8:00"), "GMT+8:00", "+08:00")?;
        }
        let reset = command(spec::CommandNode::ResetVariable {
            variable: Some(key.into()),
        });
        let result = handle_sql_config(&ctx, &reset).await?.unwrap();
        assert_eq!(result.num_rows(), 0);
        let system = get_system_timezone()?;
        assert_timezone(&ctx, Some(&system), &system, &system)?;
    }
    Ok(())
}
