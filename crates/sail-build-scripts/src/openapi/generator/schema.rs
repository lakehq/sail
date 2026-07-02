use std::collections::{BTreeMap, BTreeSet};

use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote};

use super::core::OpenApiGenerator;
use crate::error::{BuildError, BuildResult};
use crate::openapi::spec::{AdditionalProperties, Schema, SchemaType};
use crate::openapi::utils::docs::doc_attrs;
use crate::openapi::utils::name::{to_snake_case, type_ident, type_name_text, value_ident};
use crate::openapi::utils::types::{RustType, TypePosition};

impl<'a> OpenApiGenerator<'a> {
    pub(super) fn schema_definitions(&self) -> BuildResult<Vec<SchemaDefinition>> {
        self.openapi
            .components
            .schemas
            .iter()
            .filter(|(name, _)| !self.config.excluded_schemas.contains(name.as_str()))
            .map(|(name, schema)| self.schema_definition(name, schema))
            .collect()
    }

    fn schema_definition(&self, name: &str, schema: &'a Schema) -> BuildResult<SchemaDefinition> {
        let mut used_inline_type_names = BTreeSet::new();
        let mut inline = InlineSchemas::new(name);
        let schema = self.schema_definition_inner(
            name.to_owned(),
            Some(name),
            schema,
            &mut inline,
            &mut used_inline_type_names,
            false,
        )?;
        Ok(SchemaDefinition {
            module_name: inline.module_name(),
            inline_definitions: inline.definitions,
            ..schema
        })
    }

    fn schema_definition_inner(
        &self,
        type_name: String,
        serde_name: Option<&str>,
        schema: &'a Schema,
        inline: &mut InlineSchemas,
        used_inline_type_names: &mut BTreeSet<String>,
        in_module: bool,
    ) -> BuildResult<SchemaDefinition> {
        let serde_type = serde_name
            .and_then(|name| self.config.serde_types.get(name))
            .cloned();

        if let Some(variants) = string_enum_variants(schema)? {
            return Ok(SchemaDefinition {
                type_name,
                summary: schema.summary.clone(),
                description: schema.description.clone(),
                serde_type,
                module_name: None,
                inline_definitions: Vec::new(),
                kind: SchemaKind::StringEnum { variants },
            });
        }

        if let Some(tag) = discriminator_tag(schema) {
            let variants = self.discriminator_variants(schema, inline, used_inline_type_names)?;
            return Ok(SchemaDefinition {
                type_name,
                summary: schema.summary.clone(),
                description: schema.description.clone(),
                serde_type,
                module_name: None,
                inline_definitions: Vec::new(),
                kind: SchemaKind::Enum {
                    tag: Some(tag.to_owned()),
                    untagged: false,
                    variants,
                },
            });
        }

        if let Some(tag) = any_of_tag(schema)? {
            let variants = schema
                .any_of
                .iter()
                .enumerate()
                .map(|(index, schema)| {
                    self.enum_schema_variant(index, schema, true, inline, used_inline_type_names)
                })
                .collect::<Result<Vec<_>, _>>()?;
            return Ok(SchemaDefinition {
                type_name,
                summary: schema.summary.clone(),
                description: schema.description.clone(),
                serde_type,
                module_name: None,
                inline_definitions: Vec::new(),
                kind: SchemaKind::Enum {
                    tag: Some(tag.to_owned()),
                    untagged: false,
                    variants,
                },
            });
        }

        if !schema.one_of.is_empty() || !schema.any_of.is_empty() {
            // OpenAPI `anyOf` can legitimately match more than one variant, so a discriminator is
            // not always enough to make it equivalent to a Rust enum. Keep this fallback untagged
            // unless a schema uses the narrower `oneOf` form or an explicit discriminator mapping.
            let variants = schema
                .one_of
                .iter()
                .chain(schema.any_of.iter())
                .enumerate()
                .map(|(index, schema)| {
                    self.enum_schema_variant(index, schema, false, inline, used_inline_type_names)
                })
                .collect::<Result<Vec<_>, _>>()?;
            return Ok(SchemaDefinition {
                type_name,
                summary: schema.summary.clone(),
                description: schema.description.clone(),
                serde_type,
                module_name: None,
                inline_definitions: Vec::new(),
                kind: SchemaKind::Enum {
                    tag: None,
                    untagged: true,
                    variants,
                },
            });
        }

        let fields =
            self.collect_object_fields(schema, inline, used_inline_type_names, in_module)?;
        if !fields.is_empty() {
            return Ok(SchemaDefinition {
                type_name,
                summary: schema.summary.clone(),
                description: schema.description.clone(),
                serde_type,
                module_name: None,
                inline_definitions: Vec::new(),
                kind: SchemaKind::Struct { fields },
            });
        }

        if let Some(map_type) =
            self.map_type(schema, inline, used_inline_type_names, in_module, "Value")?
        {
            return Ok(SchemaDefinition {
                type_name,
                summary: schema.summary.clone(),
                description: schema.description.clone(),
                serde_type,
                module_name: None,
                inline_definitions: Vec::new(),
                kind: SchemaKind::Transparent {
                    rust_type: map_type,
                },
            });
        }

        let rust_type = self.schema_type_inner_with_inline(
            schema,
            TypePosition::Nested,
            inline,
            used_inline_type_names,
            in_module,
            "Item",
        )?;
        Ok(SchemaDefinition {
            type_name,
            summary: schema.summary.clone(),
            description: schema.description.clone(),
            serde_type,
            module_name: None,
            inline_definitions: Vec::new(),
            kind: SchemaKind::Transparent { rust_type },
        })
    }

    pub(super) fn schema_type(
        &self,
        schema: &'a Schema,
        position: TypePosition,
    ) -> BuildResult<RustType> {
        if let Some(rust_type) = self.schema_reference_type(schema, position)? {
            return Ok(rust_type);
        }
        let mut rust_type = self.schema_type_inner(schema, position)?;
        if is_nullable(schema) {
            rust_type = RustType::Option(Box::new(rust_type));
        }
        Ok(rust_type)
    }

    fn schema_type_inner(
        &self,
        schema: &'a Schema,
        position: TypePosition,
    ) -> BuildResult<RustType> {
        if is_transparent_all_of(schema) {
            let item = &schema.all_of[0];
            return self.schema_type(item, position);
        }
        if has_schema_type(schema, SchemaType::Array) {
            let items = schema.items.as_deref().ok_or_else(|| {
                BuildError::InvalidInput("array schema is missing items".to_owned())
            })?;
            let item = self.schema_type(items, TypePosition::Normal)?;
            return Ok(RustType::Vec(Box::new(item)));
        }
        if let Some(map_type) = self.map_type_plain(schema)? {
            return Ok(map_type);
        }
        if has_schema_type(schema, SchemaType::Boolean) {
            return Ok(RustType::Bool);
        }
        if has_schema_type(schema, SchemaType::Integer) {
            return Ok(match schema.format.as_deref() {
                Some("int64") => RustType::I64,
                _ => RustType::I32,
            });
        }
        if has_schema_type(schema, SchemaType::Number) {
            return Ok(RustType::F64);
        }
        if has_schema_type(schema, SchemaType::String) {
            return Ok(RustType::String);
        }
        if has_schema_type(schema, SchemaType::Object) {
            return Ok(RustType::JsonValue);
        }
        Ok(RustType::Unit)
    }

    fn schema_type_with_inline(
        &self,
        schema: &'a Schema,
        position: TypePosition,
        inline: &mut InlineSchemas,
        used_inline_type_names: &mut BTreeSet<String>,
        in_module: bool,
        suggested_name: &str,
    ) -> BuildResult<RustType> {
        if let Some(rust_type) = self.schema_reference_type(schema, position)? {
            return Ok(rust_type);
        }
        let mut rust_type = self.schema_type_inner_with_inline(
            schema,
            position,
            inline,
            used_inline_type_names,
            in_module,
            suggested_name,
        )?;
        if is_nullable(schema) {
            rust_type = RustType::Option(Box::new(rust_type));
        }
        Ok(rust_type)
    }

    fn schema_type_inner_with_inline(
        &self,
        schema: &'a Schema,
        position: TypePosition,
        inline: &mut InlineSchemas,
        used_inline_type_names: &mut BTreeSet<String>,
        in_module: bool,
        suggested_name: &str,
    ) -> BuildResult<RustType> {
        if is_transparent_all_of(schema) {
            let item = &schema.all_of[0];
            return self.schema_type_with_inline(
                item,
                position,
                inline,
                used_inline_type_names,
                in_module,
                suggested_name,
            );
        }
        if has_schema_type(schema, SchemaType::Array) {
            let items = schema.items.as_deref().ok_or_else(|| {
                BuildError::InvalidInput("array schema is missing items".to_owned())
            })?;
            let item_name = if suggested_name == "Item" {
                "Item".to_owned()
            } else {
                format!("{suggested_name}Item")
            };
            let item = self.schema_type_with_inline(
                items,
                TypePosition::Normal,
                inline,
                used_inline_type_names,
                in_module,
                &item_name,
            )?;
            return Ok(RustType::Vec(Box::new(item)));
        }
        if should_generate_inline_schema(schema)? {
            return inline.define(
                self,
                schema,
                suggested_name,
                used_inline_type_names,
                in_module,
            );
        }
        if let Some(map_type) = self.map_type(
            schema,
            inline,
            used_inline_type_names,
            in_module,
            suggested_name,
        )? {
            return Ok(map_type);
        }
        if has_schema_type(schema, SchemaType::Boolean) {
            return Ok(RustType::Bool);
        }
        if has_schema_type(schema, SchemaType::Integer) {
            return Ok(match schema.format.as_deref() {
                Some("int64") => RustType::I64,
                _ => RustType::I32,
            });
        }
        if has_schema_type(schema, SchemaType::Number) {
            return Ok(RustType::F64);
        }
        if has_schema_type(schema, SchemaType::String) {
            return Ok(RustType::String);
        }
        if has_schema_type(schema, SchemaType::Object) {
            return Ok(RustType::JsonValue);
        }
        Ok(RustType::Unit)
    }

    fn schema_reference_type(
        &self,
        schema: &'a Schema,
        position: TypePosition,
    ) -> BuildResult<Option<RustType>> {
        let Some(reference) = &schema.reference else {
            return Ok(None);
        };
        let (name, _) = self.resolve_schema(reference)?;
        let rust_type = RustType::Named {
            qualifier: Vec::new(),
            name: type_name_text(name),
        };
        Ok(Some(match position {
            TypePosition::Normal => rust_type,
            TypePosition::Nested => RustType::Box(Box::new(rust_type)),
        }))
    }

    fn map_type_plain(&self, schema: &'a Schema) -> BuildResult<Option<RustType>> {
        let Some(additional_properties) = &schema.additional_properties else {
            return Ok(None);
        };
        let value_type = match additional_properties {
            AdditionalProperties::Bool(true) => RustType::JsonValue,
            AdditionalProperties::Bool(false) => return Ok(None),
            AdditionalProperties::Schema(schema) => {
                self.schema_type(schema, TypePosition::Normal)?
            }
        };
        Ok(Some(RustType::Map(Box::new(value_type))))
    }

    fn map_type(
        &self,
        schema: &'a Schema,
        inline: &mut InlineSchemas,
        used_inline_type_names: &mut BTreeSet<String>,
        in_module: bool,
        suggested_name: &str,
    ) -> BuildResult<Option<RustType>> {
        let Some(additional_properties) = &schema.additional_properties else {
            return Ok(None);
        };
        let value_type = match additional_properties {
            AdditionalProperties::Bool(true) => RustType::JsonValue,
            AdditionalProperties::Bool(false) => return Ok(None),
            AdditionalProperties::Schema(schema) => self.schema_type_with_inline(
                schema,
                TypePosition::Normal,
                inline,
                used_inline_type_names,
                in_module,
                &format!("{suggested_name}Value"),
            )?,
        };
        Ok(Some(RustType::Map(Box::new(value_type))))
    }

    fn collect_object_fields(
        &self,
        schema: &'a Schema,
        inline: &mut InlineSchemas,
        used_inline_type_names: &mut BTreeSet<String>,
        in_module: bool,
    ) -> BuildResult<Vec<SchemaField>> {
        let mut properties = BTreeMap::new();
        let mut required = BTreeSet::new();
        self.collect_object_fields_inner(schema, &mut properties, &mut required)?;
        let mut output = Vec::new();
        for (name, schema) in properties {
            let is_required = required.contains(name.as_str());
            let identifier = value_ident(&to_snake_case(&name));
            let serde_rename =
                (identifier.to_string().trim_start_matches("r#") != name).then(|| name.clone());
            let rust_type = self.schema_type_with_inline(
                schema,
                TypePosition::Nested,
                inline,
                used_inline_type_names,
                in_module,
                &type_name_text(&name),
            )?;
            let rust_type = if is_required {
                rust_type
            } else {
                RustType::Option(Box::new(rust_type))
            };
            output.push(SchemaField {
                name,
                identifier: identifier.to_string(),
                serde_rename,
                is_optional: !is_required,
                rust_type,
            });
        }
        Ok(output)
    }

    fn collect_object_fields_inner(
        &self,
        schema: &'a Schema,
        properties: &mut BTreeMap<String, &'a Schema>,
        required: &mut BTreeSet<String>,
    ) -> BuildResult<()> {
        for item in &schema.all_of {
            let schema = if let Some(reference) = &item.reference {
                self.resolve_schema(reference)?.1
            } else {
                item
            };
            self.collect_object_fields_inner(schema, properties, required)?;
        }
        for value in &schema.required {
            required.insert(value.clone());
        }
        for (name, schema) in &schema.properties {
            properties.insert(name.clone(), schema);
        }
        Ok(())
    }

    fn enum_schema_variant(
        &self,
        index: usize,
        schema: &'a Schema,
        rename: bool,
        inline: &mut InlineSchemas,
        used_inline_type_names: &mut BTreeSet<String>,
    ) -> BuildResult<EnumVariant> {
        let (variant, rust_type, rename_value) = if let Some(reference) = &schema.reference {
            let (name, _) = self.resolve_schema(reference)?;
            (
                type_ident(name),
                self.schema_type(schema, TypePosition::Nested)?,
                Some(to_snake_case(name).replace('_', "-")),
            )
        } else {
            let mut rust_type = self.schema_type_inner_with_inline(
                schema,
                TypePosition::Nested,
                inline,
                used_inline_type_names,
                true,
                &format!("Value{index}"),
            )?;
            if is_nullable(schema) {
                rust_type = RustType::Option(Box::new(rust_type));
            }
            (format_ident!("Value{index}"), rust_type, None)
        };
        Ok(EnumVariant {
            name: variant.to_string(),
            rename: rename.then_some(rename_value).flatten(),
            aliases: Vec::new(),
            rust_type: Some(rust_type),
            fields: Vec::new(),
        })
    }

    fn discriminator_variants(
        &self,
        schema: &'a Schema,
        inline: &mut InlineSchemas,
        used_inline_type_names: &mut BTreeSet<String>,
    ) -> BuildResult<Vec<EnumVariant>> {
        let discriminator = schema.discriminator.as_ref().ok_or_else(|| {
            BuildError::InvalidInput("schema is missing discriminator".to_owned())
        })?;
        let variants = if schema.one_of.is_empty() {
            self.discriminator_variants_from_mapping(discriminator, inline, used_inline_type_names)?
        } else {
            schema
                .one_of
                .iter()
                .enumerate()
                .map(|(index, schema)| {
                    self.discriminator_variant_from_schema(
                        index,
                        schema,
                        discriminator,
                        inline,
                        used_inline_type_names,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?
        };
        Ok(variants)
    }

    fn discriminator_variants_from_mapping(
        &self,
        discriminator: &crate::openapi::spec::Discriminator,
        inline: &mut InlineSchemas,
        used_inline_type_names: &mut BTreeSet<String>,
    ) -> BuildResult<Vec<EnumVariant>> {
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
                self.discriminator_variant(
                    &reference,
                    values,
                    &discriminator.property_name,
                    inline,
                    used_inline_type_names,
                )
            })
            .collect()
    }

    fn discriminator_variant_from_schema(
        &self,
        index: usize,
        schema: &'a Schema,
        discriminator: &crate::openapi::spec::Discriminator,
        inline: &mut InlineSchemas,
        used_inline_type_names: &mut BTreeSet<String>,
    ) -> BuildResult<EnumVariant> {
        let Some(reference) = &schema.reference else {
            return self.enum_schema_variant(index, schema, false, inline, used_inline_type_names);
        };
        let values = discriminator
            .mapping
            .iter()
            .filter_map(|(value, candidate)| (candidate == reference).then_some(value.clone()))
            .collect::<Vec<_>>();
        let values = if values.is_empty() {
            let (name, _) = self.resolve_schema(reference)?;
            vec![to_snake_case(name).replace('_', "-")]
        } else {
            values
        };
        self.discriminator_variant(
            reference,
            values,
            &discriminator.property_name,
            inline,
            used_inline_type_names,
        )
    }

    fn discriminator_variant(
        &self,
        reference: &str,
        values: Vec<String>,
        tag: &str,
        inline: &mut InlineSchemas,
        used_inline_type_names: &mut BTreeSet<String>,
    ) -> BuildResult<EnumVariant> {
        let (name, schema) = self.resolve_schema(reference)?;
        let variant = type_ident(name);
        let rename = values
            .first()
            .ok_or_else(|| {
                BuildError::InvalidInput(format!(
                    "discriminator mapping for {reference} must have at least one value"
                ))
            })?
            .clone();
        let aliases = values.iter().skip(1).cloned().collect::<Vec<_>>();
        let fields = self
            .collect_object_fields(schema, inline, used_inline_type_names, true)?
            .into_iter()
            .filter(|field| field.name != tag)
            .collect::<Vec<_>>();
        Ok(EnumVariant {
            name: variant.to_string(),
            rename: Some(rename),
            aliases,
            rust_type: None,
            fields,
        })
    }
}

pub(super) fn has_schema_type(schema: &Schema, schema_type: SchemaType) -> bool {
    schema
        .r#type
        .as_ref()
        .is_some_and(|types| types.contains(&schema_type))
}

fn string_enum_variants(schema: &Schema) -> BuildResult<Option<Vec<StringEnumVariant>>> {
    if schema.enum_values.is_empty() || !has_schema_type(schema, SchemaType::String) {
        return Ok(None);
    }
    let mut used = BTreeSet::new();
    let mut variants = Vec::new();
    for (index, value) in schema.enum_values.iter().enumerate() {
        let variant = unique_ident(&type_name_text(value), index, &mut used);
        variants.push(StringEnumVariant {
            name: variant.to_string(),
            rename: value.clone(),
        });
    }
    Ok(Some(variants))
}

fn discriminator_tag(schema: &Schema) -> Option<&str> {
    let discriminator = schema.discriminator.as_ref()?;
    if !schema.one_of.is_empty() || !discriminator.mapping.is_empty() {
        Some(discriminator.property_name.as_str())
    } else {
        None
    }
}

fn should_generate_inline_schema(schema: &Schema) -> BuildResult<bool> {
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

fn any_of_tag(schema: &Schema) -> BuildResult<Option<&str>> {
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

pub(super) struct SchemaDefinition {
    type_name: String,
    summary: Option<String>,
    description: Option<String>,
    serde_type: Option<String>,
    module_name: Option<String>,
    inline_definitions: Vec<SchemaDefinition>,
    kind: SchemaKind,
}

enum SchemaKind {
    StringEnum {
        variants: Vec<StringEnumVariant>,
    },
    Enum {
        tag: Option<String>,
        untagged: bool,
        variants: Vec<EnumVariant>,
    },
    Struct {
        fields: Vec<SchemaField>,
    },
    Transparent {
        rust_type: RustType,
    },
}

struct StringEnumVariant {
    name: String,
    rename: String,
}

struct EnumVariant {
    name: String,
    rename: Option<String>,
    aliases: Vec<String>,
    rust_type: Option<RustType>,
    fields: Vec<SchemaField>,
}

struct SchemaField {
    name: String,
    identifier: String,
    serde_rename: Option<String>,
    is_optional: bool,
    rust_type: RustType,
}

struct InlineSchemas {
    module_name: String,
    definitions: Vec<SchemaDefinition>,
}

impl InlineSchemas {
    fn new(name: &str) -> Self {
        Self {
            module_name: value_ident(&to_snake_case(name)).to_string(),
            definitions: Vec::new(),
        }
    }

    fn define(
        &mut self,
        generator: &OpenApiGenerator<'_>,
        schema: &Schema,
        suggested_name: &str,
        used_inline_type_names: &mut BTreeSet<String>,
        in_module: bool,
    ) -> BuildResult<RustType> {
        let type_name = unique_type_name(suggested_name, used_inline_type_names);
        let definition = generator.schema_definition_inner(
            type_name.clone(),
            None,
            schema,
            self,
            used_inline_type_names,
            true,
        )?;
        self.definitions.push(definition);
        if in_module {
            Ok(RustType::Named {
                qualifier: Vec::new(),
                name: type_name,
            })
        } else {
            Ok(RustType::Named {
                qualifier: vec![self.module_name.clone()],
                name: type_name,
            })
        }
    }

    fn module_name(&self) -> Option<String> {
        if self.definitions.is_empty() {
            return None;
        }
        Some(self.module_name.clone())
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

fn unique_type_name(suggested_name: &str, used_names: &mut BTreeSet<String>) -> String {
    let mut name = type_name_text(suggested_name);
    if !used_names.insert(name.clone()) {
        let base = name;
        let mut index = 2;
        loop {
            name = format!("{base}{index}");
            if used_names.insert(name.clone()) {
                break;
            }
            index += 1;
        }
    }
    name
}

impl SchemaDefinition {
    pub(super) fn tokens(&self) -> BuildResult<TokenStream> {
        let module = if let Some(module_name) = &self.module_name {
            let module = format_ident!("{module_name}");
            let definitions = self
                .inline_definitions
                .iter()
                .map(SchemaDefinition::single_tokens)
                .collect::<BuildResult<Vec<_>>>()?;
            Some(quote! {
                pub mod #module {
                    use super::*;

                    #(#definitions)*
                }
            })
        } else {
            None
        };
        let schema = self.single_tokens()?;
        Ok(quote! {
            #module
            #schema
        })
    }

    fn single_tokens(&self) -> BuildResult<TokenStream> {
        let type_name = type_ident(&self.type_name);
        let docs = doc_attrs(self.summary.as_deref(), self.description.as_deref());
        let serde_type = self.serde_type.as_ref().map(|from_type| {
            quote! {
                #[serde(try_from = #from_type)]
            }
        });
        let derives = quote! { #[derive(Clone, Debug, serde::Deserialize, serde::Serialize)] };
        match &self.kind {
            SchemaKind::StringEnum { variants } => {
                let variants = variants.iter().map(generate_string_enum_variant);
                Ok(quote! {
                    #(#docs)*
                    #derives
                    #serde_type
                    pub enum #type_name {
                        #(#variants)*
                    }
                })
            }
            SchemaKind::Enum {
                tag,
                untagged,
                variants,
            } => {
                let serde_tag = tag
                    .as_ref()
                    .map(|tag| quote! { #[serde(tag = #tag)] })
                    .or_else(|| untagged.then(|| quote! { #[serde(untagged)] }));
                let variants = variants.iter().map(generate_enum_variant);
                Ok(quote! {
                    #(#docs)*
                    #derives
                    #serde_tag
                    #serde_type
                    pub enum #type_name {
                        #(#variants)*
                    }
                })
            }
            SchemaKind::Struct { fields } => {
                let fields = fields.iter().map(generate_schema_field);
                Ok(quote! {
                    #(#docs)*
                    #derives
                    #serde_type
                    pub struct #type_name {
                        #(#fields)*
                    }
                })
            }
            SchemaKind::Transparent { rust_type } => Ok(quote! {
                #(#docs)*
                #derives
                #[serde(transparent)]
                #serde_type
                pub struct #type_name(pub #rust_type);
            }),
        }
    }
}

fn generate_string_enum_variant(variant: &StringEnumVariant) -> TokenStream {
    let name = format_ident!("{}", variant.name);
    let rename = &variant.rename;
    quote! {
        #[serde(rename = #rename)]
        #name,
    }
}

fn generate_enum_variant(variant: &EnumVariant) -> TokenStream {
    let name = format_ident!("{}", variant.name);
    let rename = variant
        .rename
        .as_ref()
        .map(|value| quote! { #[serde(rename = #value)] });
    let aliases = variant
        .aliases
        .iter()
        .map(|value| quote! { #[serde(alias = #value)] });
    if let Some(rust_type) = &variant.rust_type {
        quote! {
            #rename
            #(#aliases)*
            #name(#rust_type),
        }
    } else if variant.fields.is_empty() {
        quote! {
            #rename
            #(#aliases)*
            #name,
        }
    } else {
        let fields = variant.fields.iter().map(generate_variant_field);
        quote! {
            #rename
            #(#aliases)*
            #name {
                #(#fields)*
            },
        }
    }
}

fn generate_schema_field(field: &SchemaField) -> TokenStream {
    let name = format_ident!("{}", field.identifier);
    let rust_type = &field.rust_type;
    let rename = field
        .serde_rename
        .as_ref()
        .map(|value| quote! { #[serde(rename = #value)] });
    let optional = field
        .is_optional
        .then(|| quote! { #[serde(default, skip_serializing_if = "Option::is_none")] });
    quote! {
        #rename
        #optional
        pub #name: #rust_type,
    }
}

fn generate_variant_field(field: &SchemaField) -> TokenStream {
    let name = format_ident!("{}", field.identifier);
    let rust_type = &field.rust_type;
    let rename = field
        .serde_rename
        .as_ref()
        .map(|value| quote! { #[serde(rename = #value)] });
    let optional = field
        .is_optional
        .then(|| quote! { #[serde(default, skip_serializing_if = "Option::is_none")] });
    quote! {
        #rename
        #optional
        #name: #rust_type,
    }
}
