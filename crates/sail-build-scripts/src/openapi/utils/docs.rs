use proc_macro2::TokenStream;
use quote::quote;

pub fn doc_attrs(summary: Option<&str>, description: Option<&str>) -> Vec<TokenStream> {
    let mut lines = Vec::new();
    if let Some(summary) = summary {
        lines.extend(doc_lines(summary));
    }
    if summary.is_some() && description.is_some() {
        lines.push(String::new());
    }
    if let Some(description) = description {
        lines.extend(doc_lines(description));
    }
    lines
        .into_iter()
        .map(|line| format!(" {line}"))
        .map(|line| quote! { #[doc = #line] })
        .collect()
}

fn doc_lines(value: &str) -> impl Iterator<Item = String> + '_ {
    value.lines().map(|line| line.replace('\r', ""))
}
