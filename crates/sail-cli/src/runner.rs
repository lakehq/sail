use clap::{Parser, Subcommand};
use sail_common::error::CommonError;

use crate::spark::{
    run_pyspark_shell, run_spark_connect_server, run_spark_mcp_server, McpSettings, McpTransport,
};
use crate::worker::run_worker;

#[derive(Parser)]
#[command(version, name = "sail", about = "Sail CLI")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    #[command(subcommand, about = "Run Spark workloads with Sail")]
    Spark(SparkCommand),
    #[command(about = "Start the Sail worker (internal use only)")]
    Worker,
}

#[derive(Subcommand)]
enum SparkCommand {
    #[command(about = "Start the Spark Connect server")]
    Server {
        #[arg(
            long,
            default_value = "127.0.0.1",
            help = "The IP address that the server binds to"
        )]
        ip: String,
        #[arg(
            long,
            default_value_t = 50051,
            help = "The port number that the server listens on"
        )]
        port: u16,
        #[arg(
            short = 'C',
            long,
            help = "The directory to change to before starting the server"
        )]
        directory: Option<String>,
    },
    #[command(
        about = "Start the PySpark shell with a Spark Connect server running in the background"
    )]
    Shell,
    #[command(about = "Start the Spark MCP (Model Context Protocol) server")]
    McpServer {
        #[arg(
            long,
            default_value = "127.0.0.1",
            help = "The host that the MCP server binds to (ignored for the stdio transport)"
        )]
        host: String,
        #[arg(
            long,
            default_value_t = 8000,
            help = "The port number that the server listens on (ignored for the stdio transport)"
        )]
        port: u16,
        #[arg(
            long,
            default_value_t = McpTransport::Sse,
            help = "The transport to use for the MCP server"
        )]
        transport: McpTransport,
        #[arg(long, help = "The Spark remote address to connect to (if specified)")]
        spark_remote: Option<String>,
        #[arg(
            short = 'C',
            long,
            help = "The directory to change to before starting the server"
        )]
        directory: Option<String>,
    },
}

pub fn main(args: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    if rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .is_err()
    {
        Err(CommonError::InternalError(
            "failed to install crypto provider".to_string(),
        ))?;
    }

    let cli = Cli::parse_from(args);

    match cli.command {
        Command::Worker => run_worker(),
        Command::Spark(command) => match command {
            SparkCommand::Server {
                ip,
                port,
                directory,
            } => {
                if let Some(directory) = directory {
                    std::env::set_current_dir(directory)?;
                }
                run_spark_connect_server(ip.parse()?, port)
            }
            SparkCommand::Shell => {
                // TODO: Why is there warning about leaked semaphore objects
                //   according to the Python multiprocessing resource tracker?
                run_pyspark_shell()
            }
            SparkCommand::McpServer {
                host,
                port,
                transport,
                spark_remote,
                directory,
            } => {
                if let Some(directory) = directory {
                    std::env::set_current_dir(directory)?;
                }
                run_spark_mcp_server(McpSettings {
                    transport,
                    host,
                    port,
                    spark_remote,
                })
            }
        },
    }
}
