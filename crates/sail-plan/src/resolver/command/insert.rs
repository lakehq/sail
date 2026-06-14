use datafusion_expr::LogicalPlan;
use sail_catalog::error::CatalogError;
use sail_catalog::manager::CatalogManager;
use sail_common::spec;
use sail_common_datafusion::catalog::{TableColumnStatus, TableKind, TemporaryViewSource};
use sail_common_datafusion::extension::SessionExtensionAccessor;

use crate::error::{PlanError, PlanResult};
use crate::resolver::command::write::{
    TableInfo, WriteColumnMatch, WriteMode, WritePlanBuilder, WriteTarget,
};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) async fn resolve_command_insert_overwrite_directory(
        &self,
        input: spec::QueryPlan,
        // TODO: `local` is ignored for now since the object store can be inferred from
        //   the URL scheme in `location`. But we may want to validate it in the future
        //   and return an error if `local` does not match the type of `location`.
        _local: bool,
        location: Option<String>,
        file_format: Option<spec::TableFileFormat>,
        row_format: Option<spec::TableRowFormat>,
        options: Vec<(String, String)>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let Some(location) = location else {
            return Err(PlanError::invalid(
                "missing location for INSERT OVERWRITE DIRECTORY",
            ));
        };
        if row_format.is_some() {
            return Err(PlanError::todo("row format for INSERT OVERWRITE DIRECTORY"));
        }
        let format = match file_format {
            Some(spec::TableFileFormat::General { format }) => format,
            Some(spec::TableFileFormat::Table { .. }) => {
                return Err(PlanError::todo(
                    "table file format for INSERT OVERWRITE DIRECTORY",
                ));
            }
            None => {
                return Err(PlanError::invalid(
                    "missing file format for INSERT OVERWRITE DIRECTORY",
                ));
            }
        };
        let builder = WritePlanBuilder::new()
            .with_mode(WriteMode::Replace {
                error_if_absent: false,
            })
            .with_target(WriteTarget::DataSource)
            .with_format(format)
            .with_options(options)
            .with_options(vec![("path".to_string(), location)]);
        let input = self.resolve_write_input(input, state).await?;
        self.resolve_write_with_builder(input, builder, state).await
    }

    pub(super) async fn resolve_command_insert_into(
        &self,
        input: spec::QueryPlan,
        table: spec::ObjectName,
        mode: spec::InsertMode,
        partition: Vec<(spec::Identifier, Option<spec::Expr>)>,
        if_not_exists: bool,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        use spec::InsertMode;

        if if_not_exists {
            return Err(PlanError::todo(
                "INSERT INTO ... IF NOT EXISTS is not supported",
            ));
        }

        let input = self.resolve_write_input(input, state).await?;

        if !partition.is_empty() {
            return Err(PlanError::todo("PARTITION for write"));
        }

        let (mode, column_match) = match mode {
            InsertMode::InsertByPosition { overwrite } => (
                Self::insert_write_mode(overwrite),
                WriteColumnMatch::ByPosition,
            ),
            InsertMode::InsertByName { overwrite } => {
                (Self::insert_write_mode(overwrite), WriteColumnMatch::ByName)
            }
            InsertMode::InsertByColumns { columns, overwrite } => (
                Self::insert_write_mode(overwrite),
                WriteColumnMatch::ByColumns { columns },
            ),
            InsertMode::Replace { condition } => (
                WriteMode::TruncateIf { condition },
                WriteColumnMatch::ByPosition,
            ),
        };

        // A temporary view created with `USING` is backed by a data source.
        // `INSERT INTO/OVERWRITE` writes to the same location, as Spark treats it as a table.
        if let Some((source, columns)) = self.resolve_temporary_view_insert_target(&table).await? {
            return self
                .resolve_insert_into_temporary_view(
                    input,
                    source,
                    columns,
                    mode,
                    column_match,
                    state,
                )
                .await;
        }

        let builder = WritePlanBuilder::new()
            .with_mode(mode)
            .with_target(WriteTarget::Table {
                table,
                column_match,
            });
        self.resolve_write_with_builder(input, builder, state).await
    }

    /// Maps the `OVERWRITE` flag of an `INSERT` statement to the write mode for
    /// a target that is required to already exist.
    fn insert_write_mode(overwrite: bool) -> WriteMode {
        if overwrite {
            WriteMode::Truncate
        } else {
            WriteMode::Append {
                error_if_absent: true,
            }
        }
    }

    /// Resolves the data source backing an `INSERT` target when the target is a
    /// (global) temporary view created with `USING`. Returns `None` when the
    /// target is not such a view, so the caller falls back to the table path.
    async fn resolve_temporary_view_insert_target(
        &self,
        table: &spec::ObjectName,
    ) -> PlanResult<Option<(TemporaryViewSource, Vec<TableColumnStatus>)>> {
        let manager = self.ctx.extension::<CatalogManager>()?;
        let status = match manager.get_table_or_view(table.parts()).await {
            Ok(status) => status,
            Err(CatalogError::NotFound(_, _)) => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        match status.kind {
            TableKind::TemporaryView {
                source, columns, ..
            }
            | TableKind::GlobalTemporaryView {
                source, columns, ..
            } => Ok(source.map(|source| (source, columns))),
            _ => Ok(None),
        }
    }

    /// Builds the write plan for `INSERT` into a data source temporary view.
    /// The input columns are first conformed to the view schema (so the written
    /// files carry the view's column names and types), then written to the data
    /// source location carried in the view options.
    async fn resolve_insert_into_temporary_view(
        &self,
        input: LogicalPlan,
        source: TemporaryViewSource,
        columns: Vec<TableColumnStatus>,
        mode: WriteMode,
        column_match: WriteColumnMatch,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let TemporaryViewSource { format, options } = source;
        let info = TableInfo {
            catalog_table: None,
            columns,
            location: None,
            format: format.clone(),
            partition_by: vec![],
            sort_by: vec![],
            bucket_by: None,
            properties: vec![],
        };
        let input = self
            .rewrite_write_input(input, column_match, &info, state)
            .await?;
        let builder = WritePlanBuilder::new()
            .with_mode(mode)
            .with_target(WriteTarget::DataSource)
            .with_format(format)
            .with_options(options);
        self.resolve_write_with_builder(input, builder, state).await
    }
}
