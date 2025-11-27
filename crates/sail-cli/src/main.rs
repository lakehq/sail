use std::ffi::NulError;

use pyo3::ffi::{PyUnicode_AsWideCharString, PyUnicode_FromString, Py_Main};
use pyo3::Python;
use sail_common::config::{CliConfig, CliConfigEnv};

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = CliConfig::load()?;
    if config.run_python {
        // When the environment variable is set, we are the forked child process.
        // We should run the Python interpreter instead of the Sail CLI.
        run_python_interpreter()
    } else {
        // Set the environment variable so that all forked child processes
        // behave as if they are normal Python interpreters.
        // For example, the Python `multiprocessing` module launches resource tracker
        // in the child process. It uses `sys.executable` to determine the location of
        // the Python interpreter. When the Sail CLI runs as a standalone binary, the
        // Python interpreter is embedded and `sys.executable` points to the Sail binary.
        std::env::set_var(CliConfigEnv::RUN_PYTHON, "true");
        // Initialize the Python interpreter.
        Python::initialize();
        let args = std::env::args().collect();
        match sail_cli::runner::main(args) {
            Ok(()) => {}
            Err(e) => {
                eprintln!("Error: {e}");
                std::process::exit(1);
            }
        }
    }
    Ok(())
}

fn run_python_interpreter() -> ! {
    let args = std::env::args();

    let argc = args.len() as i32;
    let Ok(mut argv) = args
        .into_iter()
        .map(|arg| {
            let arg = std::ffi::CString::new(arg)?;
            let arg = unsafe {
                let obj = PyUnicode_FromString(arg.as_ptr());
                PyUnicode_AsWideCharString(obj, std::ptr::null_mut())
            };
            Ok(arg)
        })
        .collect::<Result<Vec<_>, NulError>>()
    else {
        eprintln!("Error: null bytes found in command line argument strings");
        std::process::exit(1);
    };
    argv.push(std::ptr::null_mut());

    let code = unsafe { Py_Main(argc, argv.as_mut_ptr()) };
    std::process::exit(code)
}
