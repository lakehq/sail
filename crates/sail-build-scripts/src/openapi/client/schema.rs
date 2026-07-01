use std::collections::{BTreeMap, BTreeSet};

use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote};

use crate::error::BuildError;
use crate::openapi::types::{AdditionalProperties, Schema, SchemaType};

use super::context::Context;
use super::core::{to_snake_case, type_ident, type_name_text, value_ident, RustType, TypePosition};

pub(super) fn generate_schemas(context: &Context<'_>) -> Result<Vec<TokenStream>, BuildError> {
    context
        .openapi
        .components
        .schemas
        .iter()
        .filter(|(name, _)| !context.excluded_schemas.contains(name.as_str()))
        .map(|(name, schema)| generate_schema(context, name, schema))
        .collect()
}

pub(super) fn schema_type(
    context: &Context<'_>,
    schema: &Schema,
    position: TypePosition,
) -> Result<RustType, BuildError> {
    let mut rust_type = schema_type_inner(context, schema, position)?;
    if is_nullable(schema) {
        rust_type = rust_type.option();
    }
    Ok(rust_type)
}

pub(super) fn has_schema_type(schema: &Schema, schema_type: SchemaType) -> bool {
    schema
        .r#type
        .as_ref()
        .is_some_and(|types| types.contains(&schema_type))
}

pub(super) fn integer_type(schema: &Schema) -> Ident {
    match schema.format.as_deref() {
        Some("int64") => format_ident!("i64"),
        _ => format_ident!("i32"),
    }
}

fn generate_schema(
    context: &Context<'_>,
    name: &str,
    schema: &Schema,
) -> Result<TokenStream, BuildError> {
    let type_name = type_ident(name);
    let mut inline = InlineSchemas::new(name);
    let schema =
        generate_schema_definition(context, &type_name, Some(name), schema, &mut inline, false)?;
    let module = inline.module();
    Ok(quote! {
        #module
        #schema
    })
}

fn generate_schema_definition(
    context: &Context<'_>,
    type_name: &Ident,
    serde_name: Option<&str>,
    schema: &Schema,
    inline: &mut InlineSchemas,
    in_module: bool,
) -> Result<TokenStream, BuildError> {
    let docs = doc_attrs(schema.summary.as_deref(), schema.description.as_deref());
    let serde_type = serde_name
        .and_then(|name| context.serde_types.get(name))
        .map(|from_type| {
            quote! {
                #[serde(try_from = #from_type)]
            }
        });
    let derives = quote! { #[derive(Clone, Debug, serde::Deserialize, serde::Serialize)] };

    if let Some(variants) = string_enum_variants(schema)? {
        return Ok(quote! {
            #(#docs)*
            #derives
            #serde_type
            pub enum #type_name {
                #(#variants)*
            }
        });
    }

    if let Some(tag) = discriminator_tag(schema) {
        let variants = discriminator_variants(context, schema, inline)?;
        return Ok(quote! {
            #(#docs)*
            #derives
            #[serde(tag = #tag)]
            #serde_type
            pub enum #type_name {
                #(#variants)*
            }
        });
    }

    if let Some(tag) = any_of_tag(schema)? {
        let variants = schema
            .any_of
            .iter()
            .enumerate()
            .map(|(index, schema)| enum_schema_variant(context, index, schema, true, inline))
            .collect::<Result<Vec<_>, _>>()?;
        return Ok(quote! {
            #(#docs)*
            #derives
            #[serde(tag = #tag)]
            #serde_type
            pub enum #type_name {
                #(#variants)*
            }
        });
    }

    if !schema.one_of.is_empty() || !schema.any_of.is_empty() {
        // OpenAPI `anyOf` can legitimately match more than one variant, so a discriminator is not
        // always enough to make it equivalent to a Rust enum. Keep this fallback untagged unless a
        // schema uses the narrower `oneOf` form or an explicit discriminator mapping.
        let variants = schema
            .one_of
            .iter()
            .chain(schema.any_of.iter())
            .enumerate()
            .map(|(index, schema)| enum_schema_variant(context, index, schema, false, inline))
            .collect::<Result<Vec<_>, _>>()?;
        return Ok(quote! {
            #(#docs)*
            #derives
            #[serde(untagged)]
            #serde_type
            pub enum #type_name {
                #(#variants)*
            }
        });
    }

    let fields = collect_object_fields(context, schema, inline, in_module)?;
    if !fields.is_empty() {
        let fields = fields
            .into_iter()
            .map(|field| {
                let attrs = field.attrs;
                let name = field.identifier;
                let rust_type = field.rust_type.tokens;
                quote! {
                    #(#attrs)*
                    pub #name: #rust_type,
                }
            })
            .collect::<Vec<_>>();
        return Ok(quote! {
            #(#docs)*
            #derives
            #serde_type
            pub struct #type_name {
                #(#fields)*
            }
        });
    }

    if let Some(map_type) = map_type(context, schema, inline, in_module, "Value")? {
        let rust_type = map_type.tokens;
        return Ok(quote! {
            #(#docs)*
            #derives
            #[serde(transparent)]
            #serde_type
            pub struct #type_name(pub #rust_type);
        });
    }

    let rust_type = schema_type_inner_with_inline(
        context,
        schema,
        TypePosition::Nested,
        inline,
        in_module,
        "Item",
    )?
    .tokens;
    Ok(quote! {
        #(#docs)*
        #derives
        #[serde(transparent)]
        #serde_type
        pub struct #type_name(pub #rust_type);
    })
}

fn schema_type_inner(
    context: &Context<'_>,
    schema: &Schema,
    position: TypePosition,
) -> Result<RustType, BuildError> {
    if let Some(reference) = &schema.reference {
        let (name, _) = context.resolve_schema_ref(reference)?;
        let type_name = type_ident(name);
        return Ok(match position {
            TypePosition::Normal => RustType::new(quote! { #type_name }),
            TypePosition::Nested => RustType::new(quote! { Box<#type_name> }),
        });
    }
    if is_transparent_all_of(schema) {
        let item = &schema.all_of[0];
        return schema_type(context, item, position);
    }
    if has_schema_type(schema, SchemaType::Array) {
        let items = schema
            .items
            .as_deref()
            .ok_or_else(|| BuildError::InvalidInput("array schema is missing items".to_owned()))?;
        let item = schema_type(context, items, TypePosition::Normal)?.tokens;
        return Ok(RustType::new(quote! { Vec<#item> }));
    }
    if let Some(map_type) = map_type_plain(context, schema)? {
        return Ok(map_type);
    }
    if has_schema_type(schema, SchemaType::Boolean) {
        return Ok(RustType::new(quote! { bool }));
    }
    if has_schema_type(schema, SchemaType::Integer) {
        let ident = integer_type(schema);
        return Ok(RustType::new(quote! { #ident }));
    }
    if has_schema_type(schema, SchemaType::Number) {
        return Ok(RustType::new(quote! { f64 }));
    }
    if has_schema_type(schema, SchemaType::String) {
        return Ok(RustType::new(quote! { String }));
    }
    if has_schema_type(schema, SchemaType::Object) {
        return Ok(RustType::new(quote! { serde_json::Value }));
    }
    Ok(RustType::unit())
}

fn schema_type_with_inline(
    context: &Context<'_>,
    schema: &Schema,
    position: TypePosition,
    inline: &mut InlineSchemas,
    in_module: bool,
    suggested_name: &str,
) -> Result<RustType, BuildError> {
    let mut rust_type = schema_type_inner_with_inline(
        context,
        schema,
        position,
        inline,
        in_module,
        suggested_name,
    )?;
    if is_nullable(schema) {
        rust_type = rust_type.option();
    }
    Ok(rust_type)
}

fn schema_type_inner_with_inline(
    context: &Context<'_>,
    schema: &Schema,
    position: TypePosition,
    inline: &mut InlineSchemas,
    in_module: bool,
    suggested_name: &str,
) -> Result<RustType, BuildError> {
    if let Some(reference) = &schema.reference {
        let (name, _) = context.resolve_schema_ref(reference)?;
        let type_name = type_ident(name);
        return Ok(match position {
            TypePosition::Normal => RustType::new(quote! { #type_name }),
            TypePosition::Nested => RustType::new(quote! { Box<#type_name> }),
        });
    }
    if is_transparent_all_of(schema) {
        let item = &schema.all_of[0];
        return schema_type_with_inline(context, item, position, inline, in_module, suggested_name);
    }
    if has_schema_type(schema, SchemaType::Array) {
        let items = schema
            .items
            .as_deref()
            .ok_or_else(|| BuildError::InvalidInput("array schema is missing items".to_owned()))?;
        let item_name = if suggested_name == "Item" {
            "Item".to_owned()
        } else {
            format!("{suggested_name}Item")
        };
        let item = schema_type_with_inline(
            context,
            items,
            TypePosition::Normal,
            inline,
            in_module,
            &item_name,
        )?
        .tokens;
        return Ok(RustType::new(quote! { Vec<#item> }));
    }
    if should_generate_inline_schema(schema)? {
        return inline.define(context, schema, suggested_name, in_module);
    }
    if let Some(map_type) = map_type(context, schema, inline, in_module, suggested_name)? {
        return Ok(map_type);
    }
    if has_schema_type(schema, SchemaType::Boolean) {
        return Ok(RustType::new(quote! { bool }));
    }
    if has_schema_type(schema, SchemaType::Integer) {
        let ident = integer_type(schema);
        return Ok(RustType::new(quote! { #ident }));
    }
    if has_schema_type(schema, SchemaType::Number) {
        return Ok(RustType::new(quote! { f64 }));
    }
    if has_schema_type(schema, SchemaType::String) {
        return Ok(RustType::new(quote! { String }));
    }
    if has_schema_type(schema, SchemaType::Object) {
        return Ok(RustType::new(quote! { serde_json::Value }));
    }
    Ok(RustType::unit())
}

fn map_type_plain(context: &Context<'_>, schema: &Schema) -> Result<Option<RustType>, BuildError> {
    let Some(additional_properties) = &schema.additional_properties else {
        return Ok(None);
    };
    let value_type = match additional_properties {
        AdditionalProperties::Bool(true) => RustType::new(quote! { serde_json::Value }),
        AdditionalProperties::Bool(false) => return Ok(None),
        AdditionalProperties::Schema(schema) => schema_type(context, schema, TypePosition::Normal)?,
    };
    let value_type = value_type.tokens;
    Ok(Some(RustType::new(
        quote! { std::collections::BTreeMap<String, #value_type> },
    )))
}

fn map_type(
    context: &Context<'_>,
    schema: &Schema,
    inline: &mut InlineSchemas,
    in_module: bool,
    suggested_name: &str,
) -> Result<Option<RustType>, BuildError> {
    let Some(additional_properties) = &schema.additional_properties else {
        return Ok(None);
    };
    let value_type = match additional_properties {
        AdditionalProperties::Bool(true) => RustType::new(quote! { serde_json::Value }),
        AdditionalProperties::Bool(false) => return Ok(None),
        AdditionalProperties::Schema(schema) => schema_type_with_inline(
            context,
            schema,
            TypePosition::Normal,
            inline,
            in_module,
            &format!("{suggested_name}Value"),
        )?,
    };
    let value_type = value_type.tokens;
    Ok(Some(RustType::new(
        quote! { std::collections::BTreeMap<String, #value_type> },
    )))
}

fn collect_object_fields(
    context: &Context<'_>,
    schema: &Schema,
    inline: &mut InlineSchemas,
    in_module: bool,
) -> Result<Vec<SchemaField>, BuildError> {
    let mut properties = BTreeMap::new();
    let mut required = BTreeSet::new();
    collect_object_fields_inner(context, schema, &mut properties, &mut required)?;
    let mut output = Vec::new();
    for (name, schema) in properties {
        let is_required = required.contains(name.as_str());
        let mut attrs = Vec::new();
        let identifier = value_ident(&to_snake_case(&name));
        if identifier.to_string().trim_start_matches("r#") != name {
            attrs.push(quote! { #[serde(rename = #name)] });
        }
        let rust_type = schema_type_with_inline(
            context,
            schema,
            TypePosition::Nested,
            inline,
            in_module,
            &type_name_text(&name),
        )?;
        let rust_type = if is_required {
            rust_type
        } else {
            attrs.push(quote! { #[serde(default, skip_serializing_if = "Option::is_none")] });
            rust_type.option()
        };
        output.push(SchemaField {
            name,
            attrs,
            identifier,
            rust_type,
        });
    }
    Ok(output)
}

fn collect_object_fields_inner<'a>(
    context: &Context<'a>,
    schema: &'a Schema,
    properties: &mut BTreeMap<String, &'a Schema>,
    required: &mut BTreeSet<String>,
) -> Result<(), BuildError> {
    for item in &schema.all_of {
        let schema = if let Some(reference) = &item.reference {
            context.resolve_schema_ref(reference)?.1
        } else {
            item
        };
        collect_object_fields_inner(context, schema, properties, required)?;
    }
    for value in &schema.required {
        required.insert(value.clone());
    }
    for (name, schema) in &schema.properties {
        properties.insert(name.clone(), schema);
    }
    Ok(())
}

fn string_enum_variants(schema: &Schema) -> Result<Option<Vec<TokenStream>>, BuildError> {
    if schema.enum_values.is_empty() || !has_schema_type(schema, SchemaType::String) {
        return Ok(None);
    }
    let mut used = BTreeSet::new();
    let mut variants = Vec::new();
    for (index, value) in schema.enum_values.iter().enumerate() {
        let variant = unique_ident(&type_name_text(value), index, &mut used);
        variants.push(quote! {
            #[serde(rename = #value)]
            #variant,
        });
    }
    Ok(Some(variants))
}

fn enum_schema_variant(
    context: &Context<'_>,
    index: usize,
    schema: &Schema,
    rename: bool,
    inline: &mut InlineSchemas,
) -> Result<TokenStream, BuildError> {
    let (variant, rust_type, rename_value) = if let Some(reference) = &schema.reference {
        let (name, _) = context.resolve_schema_ref(reference)?;
        (
            type_ident(name),
            schema_type(context, schema, TypePosition::Nested)?,
            Some(to_snake_case(name).replace('_', "-")),
        )
    } else {
        (
            format_ident!("Value{index}"),
            schema_type_with_inline(
                context,
                schema,
                TypePosition::Nested,
                inline,
                true,
                &format!("Value{index}"),
            )?,
            None,
        )
    };
    let rust_type = rust_type.tokens;
    let rename_attr = rename
        .then_some(rename_value)
        .flatten()
        .map(|value| quote! { #[serde(rename = #value)] });
    Ok(quote! {
        #rename_attr
        #variant(#rust_type),
    })
}

fn discriminator_tag(schema: &Schema) -> Option<&str> {
    let discriminator = schema.discriminator.as_ref()?;
    if !schema.one_of.is_empty() || !discriminator.mapping.is_empty() {
        Some(discriminator.property_name.as_str())
    } else {
        None
    }
}

fn discriminator_variants(
    context: &Context<'_>,
    schema: &Schema,
    inline: &mut InlineSchemas,
) -> Result<Vec<TokenStream>, BuildError> {
    let discriminator = schema
        .discriminator
        .as_ref()
        .ok_or_else(|| BuildError::InvalidInput("schema is missing discriminator".to_owned()))?;
    let variants = if schema.one_of.is_empty() {
        discriminator_variants_from_mapping(context, discriminator, inline)?
    } else {
        schema
            .one_of
            .iter()
            .enumerate()
            .map(|(index, schema)| {
                discriminator_variant_from_schema(context, index, schema, discriminator, inline)
            })
            .collect::<Result<Vec<_>, _>>()?
    };
    Ok(variants)
}

fn discriminator_variants_from_mapping(
    context: &Context<'_>,
    discriminator: &crate::openapi::types::Discriminator,
    inline: &mut InlineSchemas,
) -> Result<Vec<TokenStream>, BuildError> {
    let mut references = BTreeMap::<String, Vec<String>>::new();
    for (value, reference) in &discriminator.mapping {
        references
            .entry(reference.clone())
            .or_default()
            .push(value.clone());
    }
    references
        .into_iter()
        .map(|(reference, values)| {
            discriminator_variant(
                context,
                &reference,
                values,
                &discriminator.property_name,
                inline,
            )
        })
        .collect()
}

fn discriminator_variant_from_schema(
    context: &Context<'_>,
    index: usize,
    schema: &Schema,
    discriminator: &crate::openapi::types::Discriminator,
    inline: &mut InlineSchemas,
) -> Result<TokenStream, BuildError> {
    let Some(reference) = &schema.reference else {
        return enum_schema_variant(context, index, schema, false, inline);
    };
    let values = discriminator
        .mapping
        .iter()
        .filter_map(|(value, candidate)| (candidate == reference).then_some(value.clone()))
        .collect::<Vec<_>>();
    let values = if values.is_empty() {
        let (name, _) = context.resolve_schema_ref(reference)?;
        vec![to_snake_case(name).replace('_', "-")]
    } else {
        values
    };
    discriminator_variant(
        context,
        reference,
        values,
        &discriminator.property_name,
        inline,
    )
}

fn discriminator_variant(
    context: &Context<'_>,
    reference: &str,
    values: Vec<String>,
    tag: &str,
    inline: &mut InlineSchemas,
) -> Result<TokenStream, BuildError> {
    let (name, schema) = context.resolve_schema_ref(reference)?;
    let variant = type_ident(name);
    let rename = values.first().ok_or_else(|| {
        BuildError::InvalidInput(format!(
            "discriminator mapping for {reference} must have at least one value"
        ))
    })?;
    let aliases = values
        .iter()
        .skip(1)
        .map(|value| quote! { #[serde(alias = #value)] })
        .collect::<Vec<_>>();
    let fields = collect_object_fields(context, schema, inline, true)?
        .into_iter()
        .filter(|field| field.name != tag)
        .map(|field| {
            let attrs = field.attrs;
            let name = field.identifier;
            let rust_type = field.rust_type.tokens;
            quote! {
                #(#attrs)*
                #name: #rust_type,
            }
        })
        .collect::<Vec<_>>();
    if fields.is_empty() {
        return Ok(quote! {
            #[serde(rename = #rename)]
            #(#aliases)*
            #variant,
        });
    }
    Ok(quote! {
        #[serde(rename = #rename)]
        #(#aliases)*
        #variant {
            #(#fields)*
        },
    })
}

fn should_generate_inline_schema(schema: &Schema) -> Result<bool, BuildError> {
    Ok(schema.reference.is_none()
        && !is_transparent_all_of(schema)
        && (!schema.enum_values.is_empty() && has_schema_type(schema, SchemaType::String)
            || discriminator_tag(schema).is_some()
            || any_of_tag(schema)?.is_some()
            || !schema.one_of.is_empty()
            || !schema.any_of.is_empty()
            || !schema.properties.is_empty()
            || schema
                .all_of
                .iter()
                .any(|schema| !schema.properties.is_empty() || !schema.required.is_empty())))
}

fn any_of_tag(schema: &Schema) -> Result<Option<&str>, BuildError> {
    if schema.any_of.is_empty() || !schema.one_of.is_empty() || schema.properties.is_empty() {
        return Ok(None);
    }
    if schema.properties.len() != 1 {
        return Err(BuildError::InvalidInput(
            "anyOf schemas with sibling properties must have exactly one property".to_owned(),
        ));
    }
    Ok(schema.properties.keys().next().map(String::as_str))
}

fn is_nullable(schema: &Schema) -> bool {
    schema.nullable == Some(true) || has_schema_type(schema, SchemaType::Null)
}

fn is_transparent_all_of(schema: &Schema) -> bool {
    schema.all_of.len() == 1
        && schema.reference.is_none()
        && schema.r#type.is_none()
        && schema.properties.is_empty()
        && schema.required.is_empty()
        && schema.items.is_none()
        && schema.additional_properties.is_none()
        && schema.one_of.is_empty()
        && schema.any_of.is_empty()
        && schema.enum_values.is_empty()
        && schema.const_value.is_none()
}

struct SchemaField {
    name: String,
    attrs: Vec<TokenStream>,
    identifier: Ident,
    rust_type: RustType,
}

struct InlineSchemas {
    module_ident: Ident,
    used_names: BTreeSet<String>,
    definitions: Vec<TokenStream>,
}

impl InlineSchemas {
    fn new(name: &str) -> Self {
        Self {
            module_ident: value_ident(&to_snake_case(name)),
            used_names: BTreeSet::new(),
            definitions: Vec::new(),
        }
    }

    fn define(
        &mut self,
        context: &Context<'_>,
        schema: &Schema,
        suggested_name: &str,
        in_module: bool,
    ) -> Result<RustType, BuildError> {
        let type_name = self.unique_type_ident(suggested_name);
        let definition = generate_schema_definition(context, &type_name, None, schema, self, true)?;
        self.definitions.push(definition);
        let module = &self.module_ident;
        let tokens = if in_module {
            quote! { #type_name }
        } else {
            quote! { #module::#type_name }
        };
        Ok(RustType::new(tokens))
    }

    fn module(&self) -> Option<TokenStream> {
        if self.definitions.is_empty() {
            return None;
        }
        let module = &self.module_ident;
        let definitions = &self.definitions;
        Some(quote! {
            pub mod #module {
                #[allow(unused_imports)]
                use super::*;

                #(#definitions)*
            }
        })
    }

    fn unique_type_ident(&mut self, suggested_name: &str) -> Ident {
        let mut name = type_name_text(suggested_name);
        if !self.used_names.insert(name.clone()) {
            let base = name;
            let mut index = 2;
            loop {
                name = format!("{base}{index}");
                if self.used_names.insert(name.clone()) {
                    break;
                }
                index += 1;
            }
        }
        format_ident!("{name}")
    }
}

fn unique_ident(value: &str, index: usize, used: &mut BTreeSet<String>) -> Ident {
    let mut name = type_name_text(value);
    if name.is_empty() {
        name = format!("Value{index}");
    }
    if !used.insert(name.clone()) {
        name = format!("{name}{index}");
        used.insert(name.clone());
    }
    format_ident!("{name}")
}

fn doc_attrs(summary: Option<&str>, description: Option<&str>) -> Vec<TokenStream> {
    let mut lines = Vec::new();
    if let Some(summary) = summary {
        push_doc_lines(summary, &mut lines);
    }
    if summary.is_some() && description.is_some() {
        lines.push(String::new());
    }
    if let Some(description) = description {
        push_doc_lines(description, &mut lines);
    }
    lines
        .into_iter()
        .map(|line| format!(" {line}"))
        .map(|line| quote! { #[doc = #line] })
        .collect()
}

fn push_doc_lines(value: &str, lines: &mut Vec<String>) {
    lines.extend(value.replace('\r', "").lines().map(str::to_owned));
}
