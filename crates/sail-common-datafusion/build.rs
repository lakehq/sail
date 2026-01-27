use std::path::PathBuf;

use lazy_static::lazy_static;
use quote::{format_ident, quote};
use regex::Regex;
use serde::Deserialize;

struct ProtoBuilder<'a> {
    package: &'a str,
    files: &'a [&'a str],
}

impl<'a> ProtoBuilder<'a> {
    fn new(package: &'a str, files: &'a [&'a str]) -> Self {
        Self { package, files }
    }

    fn build(self) -> Result<(), Box<dyn std::error::Error>> {
        let protos = self
            .files
            .iter()
            .map(|file| format!("proto/sail/{}/{}", self.package, file))
            .collect::<Vec<_>>();

        let proto_paths = protos.iter().map(|s| s.as_str()).collect::<Vec<_>>();

        tonic_prost_build::configure()
            .compile_well_known_types(true)
            .compile_protos(&proto_paths, &["proto"])?;

        Ok(())
    }
}

lazy_static! {
    /// A restricted SQL identifier pattern for database, table, and column names.
    static ref SQL_IDENTIFIER_PATTERN: Regex = {
        #[allow(clippy::unwrap_used)]
        Regex::new(r"^[a-z]+(_[a-z]+)*$").unwrap()
    };
}

/// Converts a SQL identifier in `snake_case` to a Rust type identifier in `PascalCase`.
fn type_identifier(value: &str) -> String {
    value
        .split('_')
        .map(|part| {
            let part = part.to_lowercase();
            let mut part = part.chars();
            match part.next() {
                Some(first) => first.to_uppercase().chain(part).collect::<String>(),
                None => String::new(),
            }
        })
        .collect::<String>()
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct SystemDatabaseEntry {
    /// The name of the database.
    name: String,
    /// The database description in Markdown format.
    description: String,
    /// The tables in the database.
    tables: Vec<SystemTableEntry>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct SystemTableEntry {
    /// The name of the table.
    name: String,
    /// The row name of the table.
    row_name: String,
    /// The table description in Markdown format.
    description: String,
    /// The columns in the table.
    columns: Vec<SystemTableColumnEntry>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct SystemTableColumnEntry {
    /// The name of the column.
    name: String,
    /// The column description in Markdown format.
    description: String,
    /// The Rust type for the column.
    rust_type: String,
    /// The SQL type string for the column.
    /// The string must be in uppercase to ensure consistency in the documentation.
    sql_type: String,
    /// The Arrow data type expression for the column.
    arrow_type: String,
    /// Whether the column is nullable.
    nullable: bool,
}

impl SystemDatabaseEntry {
    fn validate(&self) -> Result<(), Box<dyn std::error::Error>> {
        if !SQL_IDENTIFIER_PATTERN.is_match(&self.name) {
            Err(syn::Error::new(
                proc_macro2::Span::call_site(),
                format!("invalid database name: {}", self.name),
            ))?;
        }
        for table in self.tables.iter() {
            table.validate()?;
        }
        Ok(())
    }
}

impl SystemTableEntry {
    fn validate(&self) -> Result<(), Box<dyn std::error::Error>> {
        if !SQL_IDENTIFIER_PATTERN.is_match(&self.name) {
            Err(syn::Error::new(
                proc_macro2::Span::call_site(),
                format!("invalid table name: {}", self.name),
            ))?;
        }
        if !SQL_IDENTIFIER_PATTERN.is_match(&self.row_name) {
            Err(syn::Error::new(
                proc_macro2::Span::call_site(),
                format!("invalid table row name: {}", self.row_name),
            ))?;
        }
        for column in self.columns.iter() {
            column.validate()?;
        }
        Ok(())
    }
}

impl SystemTableColumnEntry {
    fn validate(&self) -> Result<(), Box<dyn std::error::Error>> {
        if !SQL_IDENTIFIER_PATTERN.is_match(&self.name) {
            Err(syn::Error::new(
                proc_macro2::Span::call_site(),
                format!("invalid column name: {}", self.name),
            ))?;
        }
        Ok(())
    }
}

fn load_system_databases(
    path: &str,
) -> Result<Vec<SystemDatabaseEntry>, Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed={path}");
    let content = std::fs::read_to_string(path)?;
    let databases: Vec<SystemDatabaseEntry> = serde_yaml::from_str(&content)?;
    for database in databases.iter() {
        database.validate()?;
    }
    Ok(databases)
}

/// Lists system databases sorted by name.
fn list_system_databases(databases: &[SystemDatabaseEntry]) -> Vec<SystemDatabaseEntry> {
    let mut databases = databases.to_vec();
    databases.sort_by_key(|db| db.name.clone());
    databases
}

/// Lists system tables sorted by name.
fn list_system_tables(databases: &[SystemDatabaseEntry]) -> Vec<SystemTableEntry> {
    let mut tables = databases
        .iter()
        .flat_map(|db| db.tables.iter().cloned())
        .collect::<Vec<_>>();
    tables.sort_by_key(|x| x.name.clone());
    tables
}

fn build_system_catalog(
    databases: &[SystemDatabaseEntry],
) -> Result<(), Box<dyn std::error::Error>> {
    let databases = list_system_databases(databases);
    let database_variant_names = databases
        .iter()
        .map(|db| format_ident!("{}", type_identifier(&db.name)))
        .collect::<Vec<_>>();

    let tokens = quote! {
        pub struct SystemCatalog;

        impl SystemCatalog {
            pub const fn databases() -> &'static [SystemDatabase] {
                &[#(SystemDatabase::#database_variant_names),*]
            }
        }
    };

    write(tokens, "system.catalog.rs")
}

fn build_system_databases(
    databases: &[SystemDatabaseEntry],
) -> Result<(), Box<dyn std::error::Error>> {
    let databases = list_system_databases(databases);
    let database_variants = databases
        .iter()
        .map(|db| {
            let ident = format_ident!("{}", type_identifier(&db.name));
            let description = &db.description;
            quote! {
                #[doc = #description]
                #ident,
            }
        })
        .collect::<Vec<_>>();

    let database_name_match_arms = databases
        .iter()
        .map(|db| {
            let variant_name = format_ident!("{}", type_identifier(&db.name));
            let database_name = &db.name;
            quote! {
                SystemDatabase::#variant_name => #database_name,
            }
        })
        .collect::<Vec<_>>();

    let database_get_match_arms = databases
        .iter()
        .map(|db| {
            let variant_name = format_ident!("{}", type_identifier(&db.name));
            let database_name = &db.name;
            quote! {
                #database_name => Some(SystemDatabase::#variant_name),
            }
        })
        .collect::<Vec<_>>();

    let database_description_match_arms = databases
        .iter()
        .map(|db| {
            let variant_name = format_ident!("{}", type_identifier(&db.name));
            let description = &db.description;
            quote! {
                SystemDatabase::#variant_name => #description,
            }
        })
        .collect::<Vec<_>>();

    let database_tables_match_arms = databases
        .iter()
        .map(|db| {
            let variant_name = format_ident!("{}", type_identifier(&db.name));
            let table_variants = db
                .tables
                .iter()
                .map(|table| {
                    let table_variant_name = format_ident!("{}", type_identifier(&table.name));
                    quote! { SystemTable::#table_variant_name }
                })
                .collect::<Vec<_>>();
            quote! {
                SystemDatabase::#variant_name => &[#(#table_variants),*],
            }
        })
        .collect::<Vec<_>>();

    let tokens = quote! {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub enum SystemDatabase {
            #(#database_variants)*
        }

        impl SystemDatabase {
            pub const fn name(&self) -> &'static str {
                match self {
                    #(#database_name_match_arms)*
                }
            }

            pub fn get(name: &str) -> Option<Self> {
                match name {
                    #(#database_get_match_arms)*
                    _ => None,
                }
            }

            pub const fn description(&self) -> &'static str {
                match self {
                    #(#database_description_match_arms)*
                }
            }

            pub const fn tables(&self) -> &'static [SystemTable] {
                match self {
                    #(#database_tables_match_arms)*
                }
            }
        }
    };

    write(tokens, "system.database.rs")
}

fn build_system_tables(
    databases: &[SystemDatabaseEntry],
) -> Result<(), Box<dyn std::error::Error>> {
    let tables = list_system_tables(databases);
    let table_variants = tables
        .iter()
        .map(|t| {
            let ident = format_ident!("{}", type_identifier(&t.name));
            let description = &t.description;
            quote! {
                #[doc = #description]
                #ident,
            }
        })
        .collect::<Vec<_>>();

    let table_name_match_arms = tables
        .iter()
        .map(|t| {
            let variant_name = format_ident!("{}", type_identifier(&t.name));
            let table_name = &t.name;
            quote! {
                SystemTable::#variant_name => #table_name,
            }
        })
        .collect::<Vec<_>>();

    let table_description_match_arms = tables
        .iter()
        .map(|t| {
            let variant_name = format_ident!("{}", type_identifier(&t.name));
            let description = &t.description;
            quote! {
                SystemTable::#variant_name => #description,
            }
        })
        .collect::<Vec<_>>();

    let table_columns_match_arms = tables
        .iter()
        .map(|t| {
            let variant_name = format_ident!("{}", type_identifier(&t.name));
            let columns = t
                .columns
                .iter()
                .map(|column| {
                    let name = &column.name;
                    let description = &column.description;
                    // parse arrow type expression
                    let arrow_type: syn::Expr = syn::parse_str(&column.arrow_type)?;
                    let sql_type = &column.sql_type;
                    let nullable = column.nullable;
                    Ok(quote! {
                        SystemTableColumn {
                            name: #name,
                            description: #description,
                            arrow_type: #arrow_type,
                            sql_type: #sql_type,
                            nullable: #nullable,
                        }
                    })
                })
                .collect::<syn::Result<Vec<_>>>()?;
            let columns = quote! { vec![#(#columns),*] };
            Ok(quote! {
                SystemTable::#variant_name => #columns,
            })
        })
        .collect::<syn::Result<Vec<_>>>()?;

    let tokens = quote! {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
        pub enum SystemTable {
            #(#table_variants)*
        }

        pub struct SystemTableColumn {
            pub name: &'static str,
            pub description: &'static str,
            pub arrow_type: datafusion::arrow::datatypes::DataType,
            pub sql_type: &'static str,
            pub nullable: bool,
        }

        impl SystemTable {
            pub const fn name(&self) -> &'static str {
                match self {
                    #(#table_name_match_arms)*
                }
            }

            pub const fn description(&self) -> &'static str {
                match self {
                    #(#table_description_match_arms)*
                }
            }

            pub fn columns(&self) -> Vec<SystemTableColumn> {
                match self {
                    #(#table_columns_match_arms)*
                }
            }
        }
    };

    write(tokens, "system.table.rs")
}

fn build_system_rows(databases: &[SystemDatabaseEntry]) -> Result<(), Box<dyn std::error::Error>> {
    let tables = list_system_tables(databases);
    let row_types = tables
        .iter()
        .map(|t| {
            let row_type_name = format_ident!("{}Row", type_identifier(&t.row_name));
            let columns = t
                .columns
                .iter()
                .map(|column| {
                    let column_name = format_ident!("{}", column.name);
                    let rust_type: syn::Type = syn::parse_str(&column.rust_type)?;
                    let description = &column.description;
                    Ok(quote! {
                        #[doc = #description]
                        pub #column_name: #rust_type,
                    })
                })
                .collect::<syn::Result<Vec<_>>>()?;
            Ok(quote! {
                #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
                pub struct #row_type_name {
                    #(#columns)*
                }
            })
        })
        .collect::<syn::Result<Vec<_>>>()?;

    let tokens = quote! {
        #(#row_types)*
    };

    write(tokens, "system.row.rs")
}

fn write(tokens: proc_macro2::TokenStream, file: &str) -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);
    std::fs::write(
        out_dir.join(file),
        prettyplease::unparse(&syn::parse2(tokens)?),
    )?;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=build.rs");

    ProtoBuilder::new("streaming", &["marker.proto"]).build()?;

    let databases = load_system_databases("data/system/databases.yaml")?;
    build_system_catalog(&databases)?;
    build_system_databases(&databases)?;
    build_system_tables(&databases)?;
    build_system_rows(&databases)?;

    Ok(())
}
