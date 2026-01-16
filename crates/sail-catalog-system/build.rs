use std::path::PathBuf;

use lazy_static::lazy_static;
use quote::{format_ident, quote};
use regex::Regex;
use serde::Deserialize;

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

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DatabaseEntry {
    /// The name of the database.
    name: String,
    /// The database description in Markdown format.
    description: String,
    /// The tables in the database.
    tables: Vec<TableEntry>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TableEntry {
    /// The name of the table.
    name: String,
    /// The row name of the table.
    row_name: String,
    /// The table description in Markdown format.
    description: String,
    /// The columns in the table.
    columns: Vec<TableColumnEntry>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TableColumnEntry {
    /// The name of the column.
    name: String,
    /// The column description in Markdown format.
    description: String,
    /// The Rust type for the column.
    rust_type: String,
    /// The SQL type string for the column.
    /// The string must be in uppercase to ensure consistency in the documentation.
    sql_type: String,
    /// Whether the column is nullable.
    sql_nullable: bool,
}

impl DatabaseEntry {
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

impl TableEntry {
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

impl TableColumnEntry {
    fn validate(&self) -> Result<(), Box<dyn std::error::Error>> {
        if !SQL_IDENTIFIER_PATTERN.is_match(&self.name) {
            Err(syn::Error::new(
                proc_macro2::Span::call_site(),
                format!("invalid column name: {}", self.name),
            ))?;
        }
        if self.sql_type.to_uppercase() != self.sql_type {
            Err(syn::Error::new(
                proc_macro2::Span::call_site(),
                format!("SQL type must be uppercase: {}", self.sql_type),
            ))?;
        }
        Ok(())
    }
}

fn load_databases(names: &[&str]) -> Result<Vec<DatabaseEntry>, Box<dyn std::error::Error>> {
    let mut databases = vec![];
    for name in names {
        let path = format!("data/databases/{name}.yaml");
        println!("cargo:rerun-if-changed={path}");
        let content = std::fs::read_to_string(path)?;
        let database: DatabaseEntry = serde_yaml::from_str(&content)?;
        database.validate()?;
        databases.push(database);
    }
    Ok(databases)
}

fn write(tokens: proc_macro2::TokenStream, file: &str) -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);
    std::fs::write(
        out_dir.join(file),
        prettyplease::unparse(&syn::parse2(tokens)?),
    )?;
    Ok(())
}

fn build_catalog(databases: &[DatabaseEntry]) -> Result<(), Box<dyn std::error::Error>> {
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

    write(tokens, "catalog.rs")
}

fn build_databases(databases: &[DatabaseEntry]) -> Result<(), Box<dyn std::error::Error>> {
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

    write(tokens, "database.rs")
}

fn build_tables(databases: &[DatabaseEntry]) -> Result<(), Box<dyn std::error::Error>> {
    let table_variants = databases
        .iter()
        .flat_map(|db| {
            db.tables.iter().map(|table| {
                let ident = format_ident!("{}", type_identifier(&table.name));
                let description = &table.description;
                quote! {
                    #[doc = #description]
                    #ident,
                }
            })
        })
        .collect::<Vec<_>>();

    let table_name_match_arms = databases
        .iter()
        .flat_map(|db| {
            db.tables.iter().map(|table| {
                let variant_name = format_ident!("{}", type_identifier(&table.name));
                let table_name = &table.name;
                quote! {
                    SystemTable::#variant_name => #table_name,
                }
            })
        })
        .collect::<Vec<_>>();

    let table_description_match_arms = databases
        .iter()
        .flat_map(|db| {
            db.tables.iter().map(|table| {
                let variant_name = format_ident!("{}", type_identifier(&table.name));
                let description = &table.description;
                quote! {
                    SystemTable::#variant_name => #description,
                }
            })
        })
        .collect::<Vec<_>>();

    let table_columns_match_arms = databases
        .iter()
        .flat_map(|db| {
            db.tables.iter().map(|table| {
                let variant_name = format_ident!("{}", type_identifier(&table.name));
                let columns = table
                    .columns
                    .iter()
                    .map(|column| {
                        let name = &column.name;
                        let description = &column.description;
                        let sql_type = &column.sql_type;
                        let nullable = column.sql_nullable;
                        quote! {
                            SystemTableColumn {
                                name: #name,
                                description: #description,
                                #[cfg(test)]
                                sql_type: #sql_type,
                                nullable: #nullable,
                            }
                        }
                    })
                    .collect::<Vec<_>>();
                let columns = quote! { &[#(#columns),*] };
                quote! {
                    SystemTable::#variant_name => #columns,
                }
            })
        })
        .collect::<Vec<_>>();

    let table_schema_match_arms = databases
        .iter()
        .flat_map(|db| {
            db.tables.iter().map(|table| {
                let variant_name = format_ident!("{}", type_identifier(&table.name));
                let row_type_name = format_ident!("{}Row", type_identifier(&table.row_name));
                quote! {
                    SystemTable::#variant_name => {
                        serializer.schema::<#row_type_name>()
                    },
                }
            })
        })
        .collect::<Vec<_>>();

    let tokens = quote! {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub enum SystemTable {
            #(#table_variants)*
        }

        pub struct SystemTableColumn {
            pub name: &'static str,
            pub description: &'static str,
            #[cfg(test)]
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

            pub const fn columns(&self) -> &'static [SystemTableColumn] {
                match self {
                    #(#table_columns_match_arms)*
                }
            }

            pub fn schema(&self) -> datafusion::common::Result<datafusion::arrow::datatypes::SchemaRef> {
                let options = crate::SYSTEM_TRACING_OPTIONS.clone();
                let serializer = sail_common_datafusion::array::serde::ArrowSerializer::new(options);
                match self {
                    #(#table_schema_match_arms)*
                }
            }
        }
    };

    write(tokens, "table.rs")
}

fn build_rows(databases: &[DatabaseEntry]) -> Result<(), Box<dyn std::error::Error>> {
    let row_types = databases
        .iter()
        .flat_map(|db| {
            db.tables.iter().map(|table| {
                let row_type_name = format_ident!("{}Row", type_identifier(&table.row_name));
                let columns = table
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
        })
        .collect::<syn::Result<Vec<_>>>()?;

    let tokens = quote! {
        #(#row_types)*
    };

    write(tokens, "row.rs")
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=build.rs");
    let databases = load_databases(&["session"])?;
    build_catalog(&databases)?;
    build_databases(&databases)?;
    build_tables(&databases)?;
    build_rows(&databases)?;
    Ok(())
}
