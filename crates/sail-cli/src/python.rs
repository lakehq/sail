use std::ffi::NulError;

use pyo3::ffi::{PyUnicode_AsWideCharString, PyUnicode_FromString, Py_Main};

pub fn run_python_interpreter() -> ! {
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
