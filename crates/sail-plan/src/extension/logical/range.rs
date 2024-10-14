use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::{DFSchema, DFSchemaRef, Result};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion_common::plan_err;

use crate::utils::ItemTaker;

#[derive(Clone, Debug, Eq, PartialEq, Hash, PartialOrd)]
pub struct Range {
    pub start: i64,
    pub end: i64,
    pub step: i64,
}

impl Range {
    pub fn partition(&self, partition: usize, num_partitions: usize) -> Self {
        let start = self.start as i128;
        let end = self.end as i128;
        let step = self.step as i128;
        let num_elements = if (end > start) != (step > 0) {
            0
        } else {
            let diff = (end - start).abs();
            let step = step.abs();
            diff / step + if diff % step == 0 { 0 } else { 1 }
        };
        let num_partitions = num_partitions as i128;
        let partition = partition as i128;
        let partition_start = partition * num_elements / num_partitions * step + start;
        let partition_end = (partition + 1) * num_elements / num_partitions * step + start;
        Range {
            start: partition_start as i64,
            end: partition_end as i64,
            step: step as i64,
        }
    }
}

impl IntoIterator for Range {
    type Item = i64;
    type IntoIter = RangeIterator;

    fn into_iter(self) -> Self::IntoIter {
        let start = self.start;
        RangeIterator {
            range: self,
            current: start,
        }
    }
}

pub struct RangeIterator {
    range: Range,
    current: i64,
}

impl Iterator for RangeIterator {
    type Item = i64;

    fn next(&mut self) -> Option<Self::Item> {
        if (self.range.step > 0 && self.current < self.range.end)
            || (self.range.step < 0 && self.current > self.range.end)
        {
            let current = self.current;
            self.current = current
                .checked_add(self.range.step)
                .unwrap_or(self.range.end);
            Some(current)
        } else {
            None
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct RangeNode {
    range: Range,
    num_partitions: usize,
    schema: DFSchemaRef,
}

#[derive(PartialEq, PartialOrd)]
struct RangeNodeOrd<'a> {
    range: &'a Range,
    num_partitions: usize,
}

impl<'a> From<&'a RangeNode> for RangeNodeOrd<'a> {
    fn from(node: &'a RangeNode) -> Self {
        Self {
            range: &node.range,
            num_partitions: node.num_partitions,
        }
    }
}

impl PartialOrd for RangeNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        RangeNodeOrd::from(self).partial_cmp(&other.into())
    }
}

impl RangeNode {
    pub fn try_new(
        name: String,
        start: i64,
        end: i64,
        step: i64,
        num_partitions: usize,
    ) -> Result<Self> {
        if step == 0 {
            return plan_err!("the range step must not be 0");
        }
        if num_partitions == 0 {
            return plan_err!("the number of partitions must be greater than 0");
        }
        let fields = vec![Field::new(name, DataType::Int64, false)];
        let schema = Arc::new(DFSchema::from_unqualified_fields(
            fields.into(),
            HashMap::new(),
        )?);
        Ok(Self {
            range: Range { start, end, step },
            num_partitions,
            schema,
        })
    }

    pub fn range(&self) -> &Range {
        &self.range
    }

    pub fn num_partitions(&self) -> usize {
        self.num_partitions
    }
}

impl UserDefinedLogicalNodeCore for RangeNode {
    fn name(&self) -> &str {
        "Range"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "Range: start={}, end={}, step={}, num_partitions={}",
            self.range.start, self.range.end, self.range.step, self.num_partitions
        )
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        exprs.zero()?;
        inputs.zero()?;
        Ok(self.clone())
    }
}
