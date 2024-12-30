// Define the keywords macros before other modules so that they can use the macros.
// `[macro_use]` extends the macro scope beyond the end of the `keywords` module.
#[macro_use]
mod keywords {
    include!(concat!(env!("OUT_DIR"), "/keywords.rs"));
}

mod ast;
mod container;
mod lexer;
mod location;
mod options;
mod token;
mod tree;
