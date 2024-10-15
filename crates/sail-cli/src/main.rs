fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().collect();
    sail_cli::runner::main(args)
}
