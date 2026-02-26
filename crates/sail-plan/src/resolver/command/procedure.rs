use std::sync::Arc;

use datafusion_expr::{Extension, LogicalPlan};
use sail_catalog::manager::CatalogManager;
use sail_common::spec;
use sail_common_datafusion::catalog::TableKind;
use sail_common_datafusion::datasource::{
    ProcedureCapability, ProcedureParameter, TableFormatRegistry,
};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_logical_plan::procedure::{ProcedureNode, ProcedureOptions};

use crate::error::{PlanError, PlanResult};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

const MIN_VACUUM_RETAIN_HOURS: i64 = 168;

#[derive(Debug, Clone)]
struct TargetMetadata {
    format: String,
    path: Option<String>,
}

impl PlanResolver<'_> {
    pub(super) async fn resolve_command_call_procedure(
        &self,
        call: spec::CallProcedure,
        _state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let spec::CallProcedure {
            procedure,
            positional_arguments,
            named_arguments,
            target,
        } = call;
        let named_arguments = named_arguments
            .into_iter()
            .map(|(k, v)| (String::from(k), v))
            .collect::<Vec<_>>();
        let mut target_table = target.map(Vec::<String>::from);
        if target_table.is_none() {
            target_table = parse_table_argument(&named_arguments);
        }

        let target_metadata = if let Some(table) = target_table.as_ref() {
            Some(self.lookup_target_table(table).await?)
        } else {
            None
        };
        let inferred_format = target_metadata.as_ref().map(|x| x.format.clone());
        let target_path = target_metadata.and_then(|x| x.path);

        let procedure_parts: Vec<String> = procedure.into();
        let (explicit_format, requested_name) = self.parse_requested_procedure(&procedure_parts)?;
        let format = explicit_format.or(inferred_format).ok_or_else(|| {
            PlanError::invalid(
                "cannot infer table format for CALL; provide a target table or a format-qualified procedure",
            )
        })?;

        let capabilities = self.load_procedure_capabilities(&format)?;
        let capability = resolve_procedure_alias(&requested_name, &capabilities).ok_or_else(|| {
            let supported = capabilities
                .iter()
                .map(|c| c.canonical_name.clone())
                .collect::<Vec<_>>()
                .join(", ");
            PlanError::unsupported(format!(
                "procedure '{requested_name}' is not supported for format '{format}'. supported: [{supported}]"
            ))
        })?;

        validate_procedure_arguments(&capability, &named_arguments, target_table.is_some())?;
        if capability.is_destructive {
            validate_destructive_procedure(&capability, &named_arguments)?;
        }

        let positional_arguments = positional_arguments
            .into_iter()
            .map(serialize_argument)
            .collect::<PlanResult<Vec<_>>>()?;
        let named_arguments = named_arguments
            .into_iter()
            .map(|(name, expr)| Ok((name, serialize_argument(expr)?)))
            .collect::<PlanResult<Vec<_>>>()?;

        let procedure_name = capability
            .canonical_name
            .split('.')
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        let options = ProcedureOptions {
            format,
            procedure_name,
            positional_arguments,
            named_arguments,
            target_table,
            target_path,
        };
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(ProcedureNode::new(options)),
        }))
    }

    fn parse_requested_procedure(&self, parts: &[String]) -> PlanResult<(Option<String>, String)> {
        if parts.is_empty() {
            return Err(PlanError::invalid("missing procedure name"));
        }
        let mut index = 0;
        let mut explicit_format = None;
        let first = parts[0].to_ascii_lowercase();
        if self.table_format_exists(&first)? {
            explicit_format = Some(first);
            index = 1;
        }
        if parts.len() > index && parts[index].eq_ignore_ascii_case("system") {
            index += 1;
        }
        if parts.len() <= index {
            return Err(PlanError::invalid("missing procedure name after namespace"));
        }
        let requested = parts[index..]
            .iter()
            .map(|x| x.to_ascii_lowercase())
            .collect::<Vec<_>>()
            .join(".");
        Ok((explicit_format, requested))
    }

    fn table_format_exists(&self, format: &str) -> PlanResult<bool> {
        let registry = self.ctx.extension::<TableFormatRegistry>()?;
        Ok(registry.get(format).is_ok())
    }

    fn load_procedure_capabilities(&self, format: &str) -> PlanResult<Vec<ProcedureCapability>> {
        let registry = self.ctx.extension::<TableFormatRegistry>()?;
        let table_format = registry.get(format)?;
        let capabilities = table_format.procedure_capabilities();
        if capabilities.is_empty() {
            Ok(default_capabilities(format))
        } else {
            Ok(capabilities)
        }
    }

    async fn lookup_target_table(&self, table: &[String]) -> PlanResult<TargetMetadata> {
        let manager = self.ctx.extension::<CatalogManager>()?;
        let status = manager.get_table_or_view(table).await?;
        match status.kind {
            TableKind::Table {
                format, location, ..
            } => Ok(TargetMetadata {
                format,
                path: location,
            }),
            _ => Err(PlanError::invalid(
                "procedure target must resolve to a physical table",
            )),
        }
    }
}

fn default_capabilities(format: &str) -> Vec<ProcedureCapability> {
    let cap = |canonical_name: &str,
               aliases: &[&str],
               supports_dry_run: bool,
               is_destructive: bool| ProcedureCapability {
        canonical_name: canonical_name.to_string(),
        aliases: aliases.iter().map(|x| x.to_string()).collect(),
        supports_dry_run,
        is_destructive,
        parameters: vec![ProcedureParameter {
            name: "table".to_string(),
            required: true,
        }],
    };
    match format.to_ascii_lowercase().as_str() {
        "delta" => vec![
            cap("system.optimize", &["optimize"], false, false),
            cap(
                "system.vacuum",
                &["vacuum", "remove_orphan_files", "expire_snapshots"],
                true,
                true,
            ),
            cap("system.expire_snapshots", &["expire_snapshots"], true, true),
            cap(
                "system.remove_orphan_files",
                &["remove_orphan_files"],
                true,
                true,
            ),
        ],
        "iceberg" => vec![
            cap(
                "system.optimize",
                &["optimize", "rewrite_data_files"],
                false,
                false,
            ),
            cap("system.vacuum", &["vacuum"], true, true),
            cap("system.expire_snapshots", &["expire_snapshots"], true, true),
            cap(
                "system.remove_orphan_files",
                &["remove_orphan_files"],
                true,
                true,
            ),
        ],
        _ => vec![
            cap("system.optimize", &["optimize"], false, false),
            cap("system.vacuum", &["vacuum"], true, true),
        ],
    }
}

fn resolve_procedure_alias(
    requested_name: &str,
    capabilities: &[ProcedureCapability],
) -> Option<ProcedureCapability> {
    let requested_name = requested_name.to_ascii_lowercase();
    capabilities.iter().find_map(|capability| {
        let canonical = capability.canonical_name.to_ascii_lowercase();
        let canonical_leaf = canonical.rsplit('.').next().unwrap_or(canonical.as_str());
        let alias_hit = capability
            .aliases
            .iter()
            .any(|alias| alias.eq_ignore_ascii_case(requested_name.as_str()));
        if canonical == requested_name || canonical_leaf == requested_name || alias_hit {
            Some(capability.clone())
        } else {
            None
        }
    })
}

fn parse_table_argument(named_arguments: &[(String, spec::Expr)]) -> Option<Vec<String>> {
    named_arguments.iter().find_map(|(name, expr)| {
        if !name.eq_ignore_ascii_case("table") && !name.eq_ignore_ascii_case("target") {
            return None;
        }
        match expr {
            spec::Expr::Literal(spec::Literal::Utf8 { value })
            | spec::Expr::Literal(spec::Literal::LargeUtf8 { value })
            | spec::Expr::Literal(spec::Literal::Utf8View { value }) => {
                value.as_ref().and_then(|s| {
                    let parts = s
                        .split('.')
                        .map(str::trim)
                        .filter(|x| !x.is_empty())
                        .map(ToString::to_string)
                        .collect::<Vec<_>>();
                    (!parts.is_empty()).then_some(parts)
                })
            }
            _ => None,
        }
    })
}

fn validate_procedure_arguments(
    capability: &ProcedureCapability,
    named_arguments: &[(String, spec::Expr)],
    has_target_table: bool,
) -> PlanResult<()> {
    for parameter in capability.parameters.iter().filter(|x| x.required) {
        if parameter.name.eq_ignore_ascii_case("table") && !has_target_table {
            return Err(PlanError::missing(format!(
                "procedure '{}' requires a target table",
                capability.canonical_name
            )));
        }
    }
    let dry_run = named_arguments
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("dry_run"))
        .and_then(|(_, expr)| parse_bool_literal(expr));
    if dry_run == Some(true) && !capability.supports_dry_run {
        return Err(PlanError::invalid(format!(
            "procedure '{}' does not support dry_run",
            capability.canonical_name
        )));
    }
    Ok(())
}

fn validate_destructive_procedure(
    capability: &ProcedureCapability,
    named_arguments: &[(String, spec::Expr)],
) -> PlanResult<()> {
    if capability
        .canonical_name
        .eq_ignore_ascii_case("system.vacuum")
    {
        if let Some(retain_hours) = named_arguments
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("retain"))
            .and_then(|(_, expr)| parse_i64_literal(expr))
        {
            if retain_hours < MIN_VACUUM_RETAIN_HOURS {
                return Err(PlanError::invalid(format!(
                    "retain must be at least {MIN_VACUUM_RETAIN_HOURS} hours for VACUUM"
                )));
            }
        }
    }
    Ok(())
}

fn parse_bool_literal(expr: &spec::Expr) -> Option<bool> {
    match expr {
        spec::Expr::Literal(spec::Literal::Boolean { value }) => *value,
        _ => None,
    }
}

fn parse_i64_literal(expr: &spec::Expr) -> Option<i64> {
    match expr {
        spec::Expr::Literal(spec::Literal::Int8 { value }) => value.map(i64::from),
        spec::Expr::Literal(spec::Literal::Int16 { value }) => value.map(i64::from),
        spec::Expr::Literal(spec::Literal::Int32 { value }) => value.map(i64::from),
        spec::Expr::Literal(spec::Literal::Int64 { value }) => *value,
        spec::Expr::Literal(spec::Literal::UInt8 { value }) => value.map(i64::from),
        spec::Expr::Literal(spec::Literal::UInt16 { value }) => value.map(i64::from),
        spec::Expr::Literal(spec::Literal::UInt32 { value }) => value.map(i64::from),
        spec::Expr::Literal(spec::Literal::UInt64 { value }) => {
            value.and_then(|x| i64::try_from(x).ok())
        }
        _ => None,
    }
}

fn serialize_argument(expr: spec::Expr) -> PlanResult<String> {
    serde_json::to_string(&expr)
        .map_err(|e| PlanError::internal(format!("failed to serialize procedure argument: {e}")))
}

#[cfg(test)]
mod tests {
    use sail_common::spec;

    use super::{
        default_capabilities, resolve_procedure_alias, validate_destructive_procedure,
        MIN_VACUUM_RETAIN_HOURS,
    };

    #[test]
    fn test_iceberg_rewrite_data_files_alias() {
        let capabilities = default_capabilities("iceberg");
        #[allow(clippy::expect_used)]
        let resolved = resolve_procedure_alias("rewrite_data_files", &capabilities)
            .expect("rewrite_data_files should resolve");
        assert_eq!(resolved.canonical_name, "system.optimize");
    }

    #[test]
    fn test_vacuum_retain_minimum_guard() {
        #[allow(clippy::expect_used)]
        let capability = default_capabilities("delta")
            .into_iter()
            .find(|x| x.canonical_name == "system.vacuum")
            .expect("vacuum capability missing");
        let args = vec![(
            "retain".to_string(),
            spec::Expr::Literal(spec::Literal::Int64 {
                value: Some(MIN_VACUUM_RETAIN_HOURS - 1),
            }),
        )];
        let validation_result = validate_destructive_procedure(&capability, &args);
        assert!(
            validation_result.is_err(),
            "retain below minimum should fail"
        );
        assert!(
            validation_result
                .as_ref()
                .err()
                .is_some_and(|err| err.to_string().contains("retain must be at least")),
            "unexpected error: {validation_result:?}"
        );
    }
}
