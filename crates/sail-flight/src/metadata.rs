use std::collections::HashSet;
use std::sync::Arc;

use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_flight::sql::metadata::{SqlInfoDataBuilder, XdbcTypeInfo, XdbcTypeInfoDataBuilder};
use arrow_flight::sql::{
    Command, Nullable, Searchable, SqlInfo, XdbcDataType, XdbcDatetimeSubcode,
};
use sail_catalog::error::CatalogError;
use sail_catalog::manager::CatalogManager;
use sail_catalog::utils::quote_names_if_needed;
use sail_common_datafusion::catalog::{DatabaseStatus, TableKind};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use tonic::Status;

use crate::query::internal;
use crate::state::FlightSession;

async fn all_databases(
    catalogs: &CatalogManager,
    catalog: &str,
) -> Result<Vec<DatabaseStatus>, Status> {
    let mut pending = catalogs
        .list_databases(&[catalog], None)
        .await
        .map_err(internal)?;
    let mut seen = HashSet::new();
    let mut databases = Vec::new();
    while let Some(database) = pending.pop() {
        if !seen.insert(database.database.clone()) {
            continue;
        }
        let mut namespace = vec![catalog.to_string()];
        namespace.extend(database.database.clone());
        match catalogs.list_databases(&namespace, None).await {
            Ok(children) => pending.extend(children),
            // Some catalogs reject a leaf namespace as a listing prefix. Only
            // tolerate this when probing children of a successfully listed DB.
            Err(CatalogError::InvalidArgument(_) | CatalogError::NotSupported(_)) => {}
            Err(error) => return Err(internal(error)),
        }
        databases.push(database);
    }
    Ok(databases)
}

fn matching_catalogs(
    catalogs: &CatalogManager,
    filter: Option<&str>,
) -> Result<Vec<Arc<str>>, Status> {
    Ok(catalogs
        .list_catalogs(None)
        .map_err(internal)?
        .into_iter()
        .filter(|name| filter.is_none_or(|filter| name.as_ref() == filter))
        .collect())
}

/// Metadata builders implement Flight SQL's LIKE filters and ordering. Do not
/// pass those filters to Sail's catalog APIs, which use Spark glob patterns.
pub async fn metadata_batch(
    session: &FlightSession,
    command: Command,
) -> Result<RecordBatch, Status> {
    let catalogs = session
        .context
        .extension::<CatalogManager>()
        .map_err(internal)?;
    match command {
        Command::CommandGetSqlInfo(query) => {
            let mut builder = SqlInfoDataBuilder::new();
            builder.append(SqlInfo::FlightSqlServerName, "Sail");
            builder.append(SqlInfo::FlightSqlServerVersion, env!("CARGO_PKG_VERSION"));
            builder.append(SqlInfo::FlightSqlServerArrowVersion, "1.3");
            builder.append(SqlInfo::FlightSqlServerReadOnly, false);
            builder.append(SqlInfo::FlightSqlServerSql, true);
            builder.append(SqlInfo::FlightSqlServerSubstrait, false);
            builder.append(SqlInfo::FlightSqlServerTransaction, 0_i32);
            builder.append(SqlInfo::FlightSqlServerCancel, false);
            builder.append(SqlInfo::FlightSqlServerBulkIngestion, false);
            builder.append(SqlInfo::SqlTransactionsSupported, false);
            builder.append(SqlInfo::SqlIdentifierQuoteChar, "`");
            builder.append(SqlInfo::SqlIdentifierCase, 1_i32);
            builder.append(SqlInfo::SqlQuotedIdentifierCase, 1_i32);
            builder.append(SqlInfo::SqlSearchStringEscape, "\\");
            builder.append(SqlInfo::SqlCatalogTerm, "catalog");
            builder.append(SqlInfo::SqlSchemaTerm, "schema");
            let data = builder.build().map_err(internal)?;
            query.into_builder(&data).build().map_err(internal)
        }
        Command::CommandGetXdbcTypeInfo(query) => {
            let mut builder = XdbcTypeInfoDataBuilder::new();
            for (name, data_type, size) in [
                ("BOOLEAN", XdbcDataType::XdbcBit, Some(1)),
                ("TINYINT", XdbcDataType::XdbcTinyint, Some(3)),
                ("SMALLINT", XdbcDataType::XdbcSmallint, Some(5)),
                ("INT", XdbcDataType::XdbcInteger, Some(10)),
                ("BIGINT", XdbcDataType::XdbcBigint, Some(19)),
                ("FLOAT", XdbcDataType::XdbcReal, Some(24)),
                ("DOUBLE", XdbcDataType::XdbcDouble, Some(53)),
                ("DECIMAL", XdbcDataType::XdbcDecimal, Some(38)),
                ("STRING", XdbcDataType::XdbcVarchar, None),
                ("BINARY", XdbcDataType::XdbcVarbinary, None),
                ("DATE", XdbcDataType::XdbcDate, Some(10)),
                ("TIMESTAMP", XdbcDataType::XdbcTimestamp, Some(26)),
            ] {
                let quoted = matches!(
                    data_type,
                    XdbcDataType::XdbcVarchar
                        | XdbcDataType::XdbcDate
                        | XdbcDataType::XdbcTimestamp
                );
                let datetime_subcode = match data_type {
                    // Arrow's enum names SQL_CODE_DATE (1) as XDBC_SUBCODE_YEAR.
                    XdbcDataType::XdbcDate => Some(XdbcDatetimeSubcode::XdbcSubcodeYear),
                    XdbcDataType::XdbcTimestamp => Some(XdbcDatetimeSubcode::XdbcSubcodeTimestamp),
                    _ => None,
                };
                let num_prec_radix = match data_type {
                    XdbcDataType::XdbcReal | XdbcDataType::XdbcDouble => Some(2),
                    XdbcDataType::XdbcTinyint
                    | XdbcDataType::XdbcSmallint
                    | XdbcDataType::XdbcInteger
                    | XdbcDataType::XdbcBigint
                    | XdbcDataType::XdbcDecimal => Some(10),
                    _ => None,
                };
                builder.append(XdbcTypeInfo {
                    type_name: name.into(),
                    data_type,
                    sql_data_type: if datetime_subcode.is_some() {
                        XdbcDataType::XdbcDatetime
                    } else {
                        data_type
                    },
                    datetime_subcode,
                    num_prec_radix,
                    column_size: size,
                    nullable: Nullable::NullabilityNullable,
                    searchable: Searchable::Full,
                    case_sensitive: data_type == XdbcDataType::XdbcVarchar,
                    literal_prefix: quoted.then(|| "'".into()),
                    literal_suffix: quoted.then(|| "'".into()),
                    unsigned_attribute: Some(false),
                    auto_increment: Some(false),
                    maximum_scale: (data_type == XdbcDataType::XdbcDecimal).then_some(38),
                    minimum_scale: (data_type == XdbcDataType::XdbcDecimal).then_some(0),
                    create_params: (data_type == XdbcDataType::XdbcDecimal)
                        .then(|| vec!["precision".into(), "scale".into()]),
                    ..Default::default()
                });
            }
            let data = builder.build().map_err(internal)?;
            data.record_batch(query.data_type).map_err(internal)
        }
        Command::CommandGetCatalogs(query) => {
            let mut builder = query.into_builder();
            for catalog in catalogs.list_catalogs(None).map_err(internal)? {
                builder.append(catalog.to_string());
            }
            builder.build().map_err(internal)
        }
        Command::CommandGetDbSchemas(query) => {
            let names = matching_catalogs(&catalogs, query.catalog.as_deref())?;
            let mut builder = query.into_builder();
            for catalog in names {
                for db in all_databases(&catalogs, &catalog).await? {
                    builder.append(catalog.as_ref(), quote_names_if_needed(&db.database));
                }
            }
            builder.build().map_err(internal)
        }
        Command::CommandGetTableTypes(query) => {
            let mut builder = query.into_builder();
            builder.append("TABLE");
            builder.append("VIEW");
            builder.build().map_err(internal)
        }
        Command::CommandGetTables(query) => {
            let names = matching_catalogs(&catalogs, query.catalog.as_deref())?;
            let mut builder = query.into_builder();
            for catalog in names {
                for db in all_databases(&catalogs, &catalog).await? {
                    let mut namespace = vec![catalog.to_string()];
                    namespace.extend(db.database.clone());
                    for table in catalogs
                        .list_tables_and_views(&namespace, None)
                        .await
                        .map_err(internal)?
                    {
                        // list_tables_and_views also returns session temp views. Expose
                        // them only under the current catalog/schema, without duplicates.
                        if table.catalog.is_none()
                            && (catalog != catalogs.default_catalog().map_err(internal)?
                                || db.database
                                    != Vec::<String>::from(
                                        catalogs.default_database().map_err(internal)?,
                                    ))
                        {
                            continue;
                        }
                        let table_type = if matches!(&table.kind, TableKind::Table { .. }) {
                            "TABLE"
                        } else {
                            "VIEW"
                        };
                        let schema = Schema::new(
                            table
                                .kind
                                .columns()
                                .into_iter()
                                .map(|column| {
                                    Field::new(column.name, column.data_type, column.nullable)
                                })
                                .collect::<Vec<_>>(),
                        );
                        builder
                            .append(
                                catalog.as_ref(),
                                quote_names_if_needed(&db.database),
                                &table.name,
                                table_type,
                                &schema,
                            )
                            .map_err(internal)?;
                    }
                }
            }
            builder.build().map_err(internal)
        }
        _ => Err(Status::unimplemented(
            "unsupported Flight SQL metadata command",
        )),
    }
}
