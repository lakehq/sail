// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! The logical write node and the planner that turns it into a physical write.
//!
//! Sail plans writes through user defined logical nodes rather than
//! DataFusion's DML statements, which its lint configuration disallows, so the
//! Lance write follows the same shape: a
//! [`LanceWriteNode`] produced by the table format, and a [`LancePhysicalPlanner`]
//! registered alongside Sail's other extension planners.

use std::fmt::Formatter;
use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use datafusion::datasource::sink::DataSinkExec;
use datafusion::execution::session_state::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion_common::{DFSchema, DFSchemaRef, Result, internal_err};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore};

use crate::options::{LanceWriteMode, LanceWriteOptions};
use crate::sink::LanceDataSink;

/// A write command's plan produces no rows, which is how Sail's other write
/// nodes describe themselves as well.
static EMPTY_SCHEMA: LazyLock<DFSchemaRef> = LazyLock::new(|| Arc::new(DFSchema::empty()));

/// Writes the rows of its input into a Lance dataset.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd)]
pub struct LanceWriteNode {
    input: Arc<LogicalPlan>,
    uri: String,
    mode: LanceWriteMode,
    options: LanceWriteOptions,
}

impl LanceWriteNode {
    pub fn new(
        input: Arc<LogicalPlan>,
        uri: String,
        mode: LanceWriteMode,
        options: LanceWriteOptions,
    ) -> Self {
        Self {
            input,
            uri,
            mode,
            options,
        }
    }
}

impl UserDefinedLogicalNodeCore for LanceWriteNode {
    fn name(&self) -> &str {
        "LanceWrite"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.input.as_ref()]
    }

    fn schema(&self) -> &DFSchemaRef {
        &EMPTY_SCHEMA
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "LanceWrite: uri={}, mode={:?}", self.uri, self.mode)
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        let ([], [input]) = (exprs.as_slice(), inputs.as_slice()) else {
            return internal_err!(
                "LanceWrite takes one input and no expressions, got {} inputs and {} expressions",
                inputs.len(),
                exprs.len()
            );
        };
        Ok(Self {
            input: Arc::new(input.clone()),
            uri: self.uri.clone(),
            mode: self.mode,
            options: self.options.clone(),
        })
    }

    fn necessary_children_exprs(&self, _output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        // Every column of the input is written.
        Some(vec![(0..self.input.schema().fields().len()).collect()])
    }
}

/// Plans [`LanceWriteNode`] into a physical write.
///
/// Add it to the extension planners of a Sail session to enable
/// `df.write.format("lance")`.
#[derive(Debug, Default, Clone, Copy)]
pub struct LancePhysicalPlanner;

#[async_trait]
impl ExtensionPlanner for LancePhysicalPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(node) = node.as_any().downcast_ref::<LanceWriteNode>() else {
            return Ok(None);
        };
        let [input] = physical_inputs else {
            return internal_err!(
                "LanceWrite requires exactly one physical input, got {}",
                physical_inputs.len()
            );
        };
        let sink = Arc::new(LanceDataSink::new(
            node.uri.clone(),
            node.mode,
            node.options.clone(),
            input.schema(),
        ));
        Ok(Some(Arc::new(DataSinkExec::new(
            Arc::clone(input),
            sink,
            None,
        ))))
    }
}
