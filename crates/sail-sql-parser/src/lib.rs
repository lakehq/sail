// Define the keywords macros before other modules so that they can use the macros.
// `[macro_use]` extends the macro scope beyond the end of the `keywords` module.
#[macro_use]
mod keywords {
    include!(concat!(env!("OUT_DIR"), "/keywords.rs"));
}

pub mod ast;
mod combinator;
pub mod common;
mod container;
pub mod lexer;
pub mod location;
pub mod options;
pub mod parser;
pub mod span;
pub mod string;
pub mod token;
pub mod tree;
mod utils;
