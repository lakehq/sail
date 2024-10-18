use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(version, name = "sail")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Worker,
}

pub fn main(args: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse_from(args);

    match cli.command {
        Command::Worker => {
            println!("Worker command");
        }
    }
    Ok(())
}
