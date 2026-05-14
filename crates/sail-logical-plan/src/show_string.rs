use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::hash::Hash;
use std::sync::Arc;

use comfy_table::{Cell, CellAlignment, ColumnConstraint, Table, Width};
use datafusion::arrow::array::{Array, PrimitiveArray, RecordBatch};
use datafusion::arrow::datatypes::{
    DataType, DurationMicrosecondType, Field, IntervalUnit, IntervalYearMonthType, TimeUnit,
};
use datafusion_common::{DFSchema, DFSchemaRef, Result};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use educe::Educe;
use sail_common::interval::{
    format_day_time_interval, format_year_month_interval, IntervalQualifierMetadata,
    SparkIntervalKind,
};
use sail_common::spec::{EXTENSION_TYPE_METADATA_KEY, SAIL_INTERVAL_EXTENSION_NAME};
use sail_common::string::escape_meta_characters;
use sail_common_datafusion::display::{ArrayFormatter, FormatOptions};
use sail_common_datafusion::utils::items::ItemTaker;

/// Read the Spark interval extension metadata off a field and resolve the
/// `(start_field, end_field)` pair to a `SparkIntervalKind`. Returns `None`
/// when the field isn't a Spark interval column or the metadata is missing /
/// invalid — both cases fall back to the default `ArrayFormatter`.
fn interval_qualifier_kind(field: &Field) -> Option<SparkIntervalKind> {
    eprintln!(
        "[DBG interval_qualifier_kind] field.name={} type={:?} ext_name={:?} metadata={:?}",
        field.name(),
        field.data_type(),
        field.extension_type_name(),
        field.metadata()
    );
    if field.extension_type_name() != Some(SAIL_INTERVAL_EXTENSION_NAME) {
        return None;
    }
    let meta: IntervalQualifierMetadata =
        serde_json::from_str(field.metadata().get(EXTENSION_TYPE_METADATA_KEY)?).ok()?;
    let (start, end) = (meta.start_field?, meta.end_field?);
    eprintln!("[DBG interval_qualifier_kind] parsed start={start} end={end}");
    match field.data_type() {
        DataType::Interval(IntervalUnit::YearMonth) => {
            SparkIntervalKind::from_year_month_fields(start, end)
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            SparkIntervalKind::from_day_time_fields(start, end)
        }
        _ => None,
    }
}

/// If `field` is a Spark interval column with qualifier metadata, pre-format
/// every row of `array`. Null cells use `null_repr` so they match the same
/// representation other columns use via `FormatOptions`. Returns `None` to
/// signal "use the default `ArrayFormatter` for this column".
fn try_format_interval_column(
    field: &Field,
    array: &dyn Array,
    null_repr: &str,
) -> Option<Vec<String>> {
    let kind = interval_qualifier_kind(field)?;
    match field.data_type() {
        DataType::Interval(IntervalUnit::YearMonth) => {
            let a = array
                .as_any()
                .downcast_ref::<PrimitiveArray<IntervalYearMonthType>>()?;
            Some(
                (0..a.len())
                    .map(|i| {
                        if a.is_null(i) {
                            null_repr.to_string()
                        } else {
                            format_year_month_interval(a.value(i), kind).unwrap_or_default()
                        }
                    })
                    .collect(),
            )
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            let a = array
                .as_any()
                .downcast_ref::<PrimitiveArray<DurationMicrosecondType>>()?;
            Some(
                (0..a.len())
                    .map(|i| {
                        if a.is_null(i) {
                            null_repr.to_string()
                        } else {
                            format_day_time_interval(a.value(i), kind).unwrap_or_default()
                        }
                    })
                    .collect(),
            )
        }
        _ => None,
    }
}

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

    fn get_formatters<'a>(
        &'a self,
        batch: &'a RecordBatch,
        options: &'a FormatOptions<'a>,
    ) -> Result<Vec<ArrayFormatter<'a>>> {
        Ok(batch
            .columns()
            .iter()
            .map(|c| ArrayFormatter::try_new(c.as_ref(), options))
            .collect::<std::result::Result<Vec<_>, _>>()?)
    }

    /// Per-column override strings for Spark interval columns that carry the
    /// qualifier extension metadata. `None` at index i means "use the default
    /// `ArrayFormatter` for column i".
    fn get_interval_overrides(
        &self,
        batch: &RecordBatch,
        options: &FormatOptions<'_>,
    ) -> Vec<Option<Vec<String>>> {
        batch
            .schema()
            .fields()
            .iter()
            .zip(batch.columns().iter())
            .map(|(f, c)| try_format_interval_column(f, c.as_ref(), options.null()))
            .collect()
    }

    fn show_footer(&self, num_rows: usize, has_more: bool) -> String {
        match (has_more, num_rows) {
            (true, 1) => "only showing top 1 row\n".to_string(),
            (true, n) => format!("only showing top {n} rows\n"),
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

        let options = FormatOptions::default();
        let formatters = self.get_formatters(batch, &options)?;
        let interval_overrides = self.get_interval_overrides(batch, &options);
        for row in 0..batch.num_rows() {
            let row = formatters
                .iter()
                .enumerate()
                .map(|(col, f)| {
                    if let Some(strs) = &interval_overrides[col] {
                        Ok(truncate_string(
                            &escape_meta_characters(&strs[row]),
                            self.truncate,
                        ))
                    } else {
                        f.value(row)
                            .try_to_string()
                            .map(|s| escape_meta_characters(&s))
                            .map(|s| truncate_string(&s, self.truncate))
                    }
                })
                .collect::<std::result::Result<Vec<_>, _>>()?;
            table.add_row(row);
        }
        let footer = self.show_footer(batch.num_rows(), has_more);
        let table = format!("{table}\n{footer}");
        Ok(table)
    }

    fn show_vertical_string(&self, batch: &RecordBatch, has_more: bool) -> Result<String> {
        const MIN_COLUMN_WIDTH: u16 = 3;
        const PADDING: u16 = 1;

        let mut table = Table::new();
        table.load_preset("        |          ");
        let options = FormatOptions::default();
        let formatters = self.get_formatters(batch, &options)?;
        let interval_overrides = self.get_interval_overrides(batch, &options);
        for row in 0..batch.num_rows() {
            for (col, (formatter, field)) in formatters
                .iter()
                .zip(batch.schema().fields.iter())
                .enumerate()
            {
                let value = if let Some(strs) = &interval_overrides[col] {
                    truncate_string(&escape_meta_characters(&strs[row]), self.truncate)
                } else {
                    formatter
                        .value(row)
                        .try_to_string()
                        .map(|s| escape_meta_characters(&s))
                        .map(|s| truncate_string(&s, self.truncate))?
                };
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
                if i.is_multiple_of(num_fields) {
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
        let options = FormatOptions::default();
        let formatters = self.get_formatters(batch, &options)?;
        let interval_overrides = self.get_interval_overrides(batch, &options);
        let mut table = vec!["<table border='1'>".to_string()];
        let header = batch
            .schema()
            .fields
            .iter()
            .map(|f| format!("<th>{}</th>", html_escape::encode_text(f.name())))
            .collect::<Vec<_>>()
            .join("");
        table.push(format!("<tr>{header}</tr>"));
        for row in 0..batch.num_rows() {
            let row = formatters
                .iter()
                .enumerate()
                .map(|(col, f)| -> std::result::Result<String, datafusion::arrow::error::ArrowError> {
                    let s = if let Some(strs) = &interval_overrides[col] {
                        strs[row].clone()
                    } else {
                        f.value(row).try_to_string()?
                    };
                    let s = truncate_string(&s, self.truncate);
                    Ok(format!("<td>{}</td>", html_escape::encode_text(s.as_str())))
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

#[derive(Clone, Debug, PartialEq, Eq, Hash, Educe)]
#[educe(PartialOrd)]
pub struct ShowStringNode {
    input: Arc<LogicalPlan>,
    // names is part of schema so we skip it in PartialOrd
    #[educe(PartialOrd(ignore))]
    names: Vec<String>,
    #[educe(PartialOrd(ignore))]
    schema: DFSchemaRef,
    limit: usize,
    format: ShowStringFormat,
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

    pub fn input(&self) -> &Arc<LogicalPlan> {
        &self.input
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

    fn necessary_children_exprs(&self, _output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        Some(vec![(0..self.input.schema().fields().len()).collect()])
    }
}
