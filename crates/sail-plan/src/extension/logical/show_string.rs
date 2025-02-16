use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::hash::Hash;
use std::sync::Arc;

use comfy_table::{Cell, CellAlignment, ColumnConstraint, Table, Width};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion_common::{DFSchema, DFSchemaRef, Result};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use sail_common::string::escape_meta_characters;
use sail_common_datafusion::display::{ArrayFormatter, FormatOptions};

use crate::utils::ItemTaker;

fn truncate_string(s: &str, n: usize) -> String {
    if n == 0 || s.len() <= n {
        s.to_string()
    } else if n < 4 {
        s.chars().take(n).collect::<String>()
    } else {
        format!("{}...", s.chars().take(n - 3).collect::<String>())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, PartialOrd)]
pub enum ShowStringStyle {
    Default,
    Vertical,
    Html,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, PartialOrd)]
pub struct ShowStringFormat {
    style: ShowStringStyle,
    truncate: usize,
}

impl ShowStringFormat {
    pub fn new(style: ShowStringStyle, truncate: usize) -> Self {
        Self { style, truncate }
    }

    pub fn style(&self) -> ShowStringStyle {
        self.style
    }

    pub fn truncate(&self) -> usize {
        self.truncate
    }
}

impl ShowStringFormat {
    pub fn show(&self, batch: &RecordBatch, has_more: bool) -> Result<String> {
        match self.style {
            ShowStringStyle::Default => self.show_string(batch, has_more),
            ShowStringStyle::Vertical => self.show_vertical_string(batch, has_more),
            ShowStringStyle::Html => self.show_html(batch, has_more),
        }
    }

    fn get_formatters<'a>(&'a self, batch: &'a RecordBatch) -> Result<Vec<ArrayFormatter<'a>>> {
        let options = FormatOptions::default().with_null("NULL");
        Ok(batch
            .columns()
            .iter()
            .map(|c| ArrayFormatter::try_new(c.as_ref(), &options))
            .collect::<std::result::Result<Vec<_>, _>>()?)
    }

    fn show_footer(&self, num_rows: usize, has_more: bool) -> String {
        match (has_more, num_rows) {
            (true, 1) => "only showing top 1 row\n".to_string(),
            (true, n) => format!("only showing top {} rows\n", n),
            _ => "".to_string(),
        }
    }

    fn show_string(&self, batch: &RecordBatch, has_more: bool) -> Result<String> {
        const MIN_COLUMN_WIDTH: u16 = 3;
        const PADDING: u16 = 0;

        let mut table = Table::new();
        table.load_preset("||--+-++|    ++++++");

        let header = batch
            .schema()
            .fields
            .iter()
            .map(|f| Cell::new(escape_meta_characters(f.name())))
            .collect::<Vec<_>>();
        table.set_header(header);

        let alignment = match self.truncate {
            0 => CellAlignment::Left,
            _ => CellAlignment::Right,
        };
        table.column_iter_mut().for_each(|c| {
            c.set_padding((PADDING, PADDING))
                .set_constraint(ColumnConstraint::LowerBoundary(Width::Fixed(
                    MIN_COLUMN_WIDTH,
                )))
                .set_cell_alignment(alignment);
        });

        let formatters = self.get_formatters(batch)?;
        for row in 0..batch.num_rows() {
            let row = formatters
                .iter()
                .map(|f| {
                    f.value(row)
                        .try_to_string()
                        .map(|s| escape_meta_characters(&s))
                        .map(|s| truncate_string(&s, self.truncate))
                })
                .collect::<std::result::Result<Vec<_>, _>>()?;
            table.add_row(row);
        }
        let footer = self.show_footer(batch.num_rows(), has_more);
        let table = format!("{}\n{}", table, footer);
        Ok(table)
    }

    fn show_vertical_string(&self, batch: &RecordBatch, has_more: bool) -> Result<String> {
        const MIN_COLUMN_WIDTH: u16 = 3;
        const PADDING: u16 = 1;

        let mut table = Table::new();
        table.load_preset("        |          ");
        let formatters = self.get_formatters(batch)?;
        for row in 0..batch.num_rows() {
            for (formatter, field) in formatters.iter().zip(batch.schema().fields.iter()) {
                let value = formatter
                    .value(row)
                    .try_to_string()
                    .map(|s| escape_meta_characters(&s))
                    .map(|s| truncate_string(&s, self.truncate))?;
                table.add_row(vec![field.name().clone(), value]);
            }
        }
        table.column_iter_mut().for_each(|c| {
            c.set_padding((PADDING, PADDING))
                .set_constraint(ColumnConstraint::LowerBoundary(Width::Fixed(
                    MIN_COLUMN_WIDTH,
                )))
                .set_cell_alignment(CellAlignment::Left);
        });

        fn header(i: usize, width: usize) -> String {
            let value = format!("-RECORD {i}");
            format!("{value:-<width$}")
        }

        let lines = table.lines().collect::<Vec<_>>();
        let mut table = vec![];
        let num_fields = batch.schema().fields.len();
        if num_fields > 0 {
            let width = lines.iter().map(|l| l.len()).max().unwrap_or(0);
            for (i, line) in lines.into_iter().enumerate() {
                if i % num_fields == 0 {
                    table.push(header(i / num_fields, width));
                }
                table.push(line);
            }
        } else {
            let width =
                PADDING + MIN_COLUMN_WIDTH + PADDING + 1 + PADDING + MIN_COLUMN_WIDTH + PADDING;
            for i in 0..batch.num_rows() {
                table.push(header(i, width as usize));
            }
        }
        if batch.num_rows() == 0 {
            table.push("(0 rows)".to_string());
        }
        let footer = self.show_footer(batch.num_rows(), has_more);
        let table = format!("{}\n{}", table.join("\n"), footer);
        Ok(table)
    }

    fn show_html(&self, batch: &RecordBatch, has_more: bool) -> Result<String> {
        let formatters = self.get_formatters(batch)?;
        let mut table = vec!["<table border='1'>".to_string()];
        let header = batch
            .schema()
            .fields
            .iter()
            .map(|f| format!("<th>{}</th>", html_escape::encode_text(f.name())))
            .collect::<Vec<_>>()
            .join("");
        table.push(format!("<tr>{}</tr>", header));
        for row in 0..batch.num_rows() {
            let row = formatters
                .iter()
                .map(|f| {
                    f.value(row).try_to_string().map(|s| {
                        let s = truncate_string(&s, self.truncate);
                        format!("<td>{}</td>", html_escape::encode_text(s.as_str()))
                    })
                })
                .collect::<std::result::Result<Vec<_>, _>>()?;
            table.push(format!("<tr>{}</tr>", row.join("")));
        }
        table.push("</table>".to_string());
        let footer = self.show_footer(batch.num_rows(), has_more);
        let table = format!("{}\n{}", table.join("\n"), footer);
        Ok(table)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct ShowStringNode {
    input: Arc<LogicalPlan>,
    names: Vec<String>,
    schema: DFSchemaRef,
    limit: usize,
    format: ShowStringFormat,
}

#[derive(PartialEq, PartialOrd)]
struct ShowStringNodeOrd<'a> {
    // names is part of schema so we skip that
    input: &'a Arc<LogicalPlan>,
    limit: usize,
    format: &'a ShowStringFormat,
}

impl<'a> From<&'a ShowStringNode> for ShowStringNodeOrd<'a> {
    fn from(node: &'a ShowStringNode) -> Self {
        Self {
            input: &node.input,
            limit: node.limit,
            format: &node.format,
        }
    }
}

impl PartialOrd for ShowStringNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        ShowStringNodeOrd::from(self).partial_cmp(&other.into())
    }
}

impl ShowStringNode {
    pub fn try_new(
        input: Arc<LogicalPlan>,
        names: Vec<String>,
        limit: usize,
        format: ShowStringFormat,
        output_name: String,
    ) -> Result<Self> {
        let fields = vec![Field::new(output_name, DataType::Utf8, false)];
        Ok(Self {
            input,
            names,
            limit,
            format: format.clone(),
            schema: Arc::new(DFSchema::from_unqualified_fields(
                fields.into(),
                HashMap::new(),
            )?),
        })
    }

    pub fn names(&self) -> &[String] {
        &self.names
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    pub fn format(&self) -> &ShowStringFormat {
        &self.format
    }
}

impl UserDefinedLogicalNodeCore for ShowStringNode {
    fn name(&self) -> &str {
        "ShowString"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.input.as_ref()]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ShowString")
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        exprs.zero()?;
        Ok(Self {
            input: Arc::new(inputs.one()?),
            names: self.names.clone(),
            limit: self.limit,
            format: self.format.clone(),
            schema: self.schema.clone(),
        })
    }
}
