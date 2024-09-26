use sail_common::spec;
use sail_common::spec::AlterTableLocation;
use sqlparser::ast;

use crate::error::{SqlError, SqlResult};
use crate::expression::common::{from_ast_ident, from_ast_object_name_normalized};

pub(crate) fn alter_table_statement_to_plan(alter_table: ast::Statement) -> SqlResult<spec::Plan> {
    let (name, if_exists, operations, location) = match alter_table {
        ast::Statement::AlterTable {
            name,
            if_exists,
            only: _,
            operations,
            location,
            on_cluster: _,
        } => (name, if_exists, operations, location),
        _ => return Err(SqlError::invalid("Expected an ALTER TABLE statement")),
    };
    let table = from_ast_object_name_normalized(&name)?;
    let location = location
        .map(|location| -> SqlResult<_> {
            Ok(AlterTableLocation {
                location: from_ast_ident(&location.location, true)?,
                set_location: location.has_set,
            })
        })
        .transpose()?;
    let node = spec::CommandNode::AlterTable {
        table,
        if_exists,
        // TODO: When we implement a `AlterTable` Extension in DataFusion, create spec equivalent of `AlterTableOperation`
        operations,
        location,
    };
    Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
}
