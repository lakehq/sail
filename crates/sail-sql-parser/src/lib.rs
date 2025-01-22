// Define the keywords macros before other modules so that they can use the macros.
// `[macro_use]` extends the macro scope beyond the end of the `keywords` module.
#[macro_use]
mod keywords {
    include!(concat!(env!("OUT_DIR"), "/keywords.rs"));
}

pub mod ast;
mod combinator;
mod container;
mod lexer;
pub mod location;
mod options;
mod parser;
pub mod token;
pub mod tree;

pub use combinator::Sequence;
pub use lexer::lexer;
pub use options::ParserOptions;
