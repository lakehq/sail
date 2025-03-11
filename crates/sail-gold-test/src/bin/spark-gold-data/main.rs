use std::path::Path;

use clap::Parser;
use sail_gold_test::bootstrap::spark::suites;
use sail_gold_test::bootstrap::spark::writer::TestSuiteWriter;

#[derive(Parser)]
#[clap(name = "spark-gold-data", about = "Generate Spark gold data")]
struct Cli {
    #[clap(long, help = "The input directory")]
    input: String,
    #[clap(long, help = "The output directory")]
    output: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let writer = TestSuiteWriter {
        input_path: &cli.input,
        output_path: &cli.output,
    };

    writer.write_one(
        "DataTypeParserSuite.jsonl",
        "data_type.json",
        suites::parser::build_data_type_parser_suite,
    )?;
    writer.write_one(
        "TableSchemaParserSuite.jsonl",
        "table_schema.json",
        suites::parser::build_table_schema_parser_suite,
    )?;
    writer.write_many(
        "ExpressionParserSuite.jsonl",
        |group| Path::new("expression").join(format!("{group}.json")),
        suites::parser::build_expression_parser_suites,
    )?;
    writer.write_many(
        "DDLParserSuite.jsonl",
        |group| Path::new("plan").join(format!("ddl_{group}.json")),
        suites::parser::build_plan_parser_suites,
    )?;
    writer.write_many(
        "ErrorParserSuite.jsonl",
        |group| Path::new("plan").join(format!("error_{group}.json")),
        suites::parser::build_plan_parser_suites,
    )?;
    writer.write_many(
        "PlanParserSuite.jsonl",
        |group| Path::new("plan").join(format!("plan_{group}.json")),
        suites::parser::build_plan_parser_suites,
    )?;
    writer.write_many(
        "UnpivotParserSuite.jsonl",
        |group| Path::new("plan").join(format!("unpivot_{group}.json")),
        suites::parser::build_plan_parser_suites,
    )?;
    writer.write_many(
        "FunctionCollectorSuite.jsonl",
        |group| Path::new("function").join(format!("{group}.json")),
        suites::function::build_function_suites,
    )?;

    Ok(())
}
