use std::collections::{BTreeMap, BTreeSet};

use proc_macro2::TokenStream;
use quote::quote;

use super::core::OpenApiGenerator;
use crate::error::{BuildError, BuildResult};
use crate::openapi::spec::{AdditionalProperties, MaybeRef, Schema, SchemaReference, SchemaType};
use crate::openapi::utils::docs::doc_attrs;
use crate::openapi::utils::name::{RustName, type_name, value_name};
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

    fn schema_definition(
        &self,
        name: &str,
        schema: &'a MaybeRef<Schema, SchemaReference>,
    ) -> BuildResult<SchemaDefinition> {
        let mut inline = InlineSchemas::new(name);
        let schema = self.resolve_schema(schema)?;
        let schema =
            self.schema_definition_inner(type_name(name), Some(name), schema, &mut inline, false)?;
        Ok(SchemaDefinition {
            module_name: inline.module_name(),
            inline_definitions: inline.definitions,
            ..schema
        })
    }

    fn schema_definition_inner(
        &self,
        type_name: RustName,
        serde_name: Option<&str>,
        schema: &'a Schema,
        inline: &mut InlineSchemas,
        in_module: bool,
    ) -> BuildResult<SchemaDefinition> {
        let serde_type = serde_name
            .and_then(|name| self.config.serde_types.get(name))
            .cloned();

        if let Some(variants) = string_enum_variants(schema)? {
            return Ok(SchemaDefinition::new(
                type_name,
                schema,
                serde_type,
                SchemaKind::Enum {
                    tag: None,
                    untagged: false,
                    variants,
                },
            ));
        }

        if let Some(tag) = discriminator_tag(schema) {
            let variants = self.discriminator_variants(schema, inline)?;
            return Ok(SchemaDefinition::new(
                type_name,
                schema,
                serde_type,
                SchemaKind::Enum {
                    tag: Some(tag.to_owned()),
                    untagged: false,
                    variants,
                },
            ));
        }

        if let (Some(tag), None) = (any_of_tag(schema)?, serde_type.as_ref()) {
            return Err(BuildError::InvalidInput(format!(
                "tagged anyOf schema {type_name} using {tag} must define discriminator mapping"
            )));
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
                .map(|(index, schema)| self.enum_schema_variant(index, schema, None, inline))
                .collect::<Result<Vec<_>, _>>()?;
            return Ok(SchemaDefinition::new(
                type_name,
                schema,
                serde_type,
                SchemaKind::Enum {
                    tag: None,
                    untagged: true,
                    variants,
                },
            ));
        }

        let fields = self.collect_object_fields(schema, inline, in_module)?;
        if !fields.is_empty() {
            return Ok(SchemaDefinition::new(
                type_name,
                schema,
                serde_type,
                SchemaKind::Struct { fields },
            ));
        }

        if let Some(map_type) = self.map_type(schema, inline, in_module, "Value")? {
            return Ok(SchemaDefinition::new(
                type_name,
                schema,
                serde_type,
                SchemaKind::Transparent {
                    rust_type: map_type,
                },
            ));
        }

        let rust_type = self.schema_type_inner_with_inline(
            schema,
            TypePosition::Nested,
            inline,
            in_module,
            "Item",
        )?;
        Ok(SchemaDefinition::new(
            type_name,
            schema,
            serde_type,
            SchemaKind::Transparent { rust_type },
        ))
    }

    pub(super) fn schema_type(
        &self,
        schema: &'a MaybeRef<Schema, SchemaReference>,
        position: TypePosition,
    ) -> BuildResult<RustType> {
        let schema = match schema {
            MaybeRef::Value(schema) => schema,
            MaybeRef::Ref(reference) => {
                return self.schema_reference_type(&reference.reference, position);
            }
        };
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
        if let Some(item) = transparent_all_of(schema) {
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
        if let Some(rust_type) = primitive_schema_type(schema) {
            return Ok(rust_type);
        }
        if has_schema_type(schema, SchemaType::Object) {
            return Ok(RustType::JsonValue);
        }
        Ok(RustType::Unit)
    }

    fn schema_type_with_inline(
        &self,
        schema: &'a MaybeRef<Schema, SchemaReference>,
        position: TypePosition,
        inline: &mut InlineSchemas,
        in_module: bool,
        suggested_name: &str,
    ) -> BuildResult<RustType> {
        let schema = match schema {
            MaybeRef::Value(schema) => schema,
            MaybeRef::Ref(reference) => {
                return self.schema_reference_type(&reference.reference, position);
            }
        };
        let mut rust_type = self.schema_type_inner_with_inline(
            schema,
            position,
            inline,
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
        in_module: bool,
        suggested_name: &str,
    ) -> BuildResult<RustType> {
        if let Some(item) = transparent_all_of(schema) {
            return self.schema_type_with_inline(item, position, inline, in_module, suggested_name);
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
                in_module,
                &item_name,
            )?;
            return Ok(RustType::Vec(Box::new(item)));
        }
        if should_generate_inline_schema(schema)? {
            return self.define_inline_schemas(schema, inline, suggested_name, in_module);
        }
        if let Some(map_type) = self.map_type(schema, inline, in_module, suggested_name)? {
            return Ok(map_type);
        }
        if let Some(rust_type) = primitive_schema_type(schema) {
            return Ok(rust_type);
        }
        if has_schema_type(schema, SchemaType::Object) {
            return Ok(RustType::JsonValue);
        }
        Ok(RustType::Unit)
    }

    fn define_inline_schemas(
        &self,
        schema: &'a Schema,
        inline: &mut InlineSchemas,
        suggested_name: &str,
        in_module: bool,
    ) -> BuildResult<RustType> {
        let type_name = type_name(suggested_name);
        let definition =
            self.schema_definition_inner(type_name.clone(), None, schema, inline, true)?;
        inline.definitions.push(definition);
        if in_module {
            Ok(RustType::Named {
                qualifier: Vec::new(),
                name: type_name,
            })
        } else {
            Ok(RustType::Named {
                qualifier: vec![inline.module_name.clone()],
                name: type_name,
            })
        }
    }

    fn schema_reference_type(
        &self,
        reference: &str,
        position: TypePosition,
    ) -> BuildResult<RustType> {
        let (name, _) = self.resolve_schema_reference(reference)?;
        let rust_type = RustType::Named {
            qualifier: Vec::new(),
            name: type_name(name),
        };
        Ok(match position {
            TypePosition::Normal => rust_type,
            TypePosition::Nested => RustType::Box(Box::new(rust_type)),
        })
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
        in_module: bool,
    ) -> BuildResult<Vec<SchemaField>> {
        let mut properties = BTreeMap::new();
        let mut required = BTreeSet::new();
        self.collect_object_fields_inner(schema, &mut properties, &mut required)?;
        let mut output = Vec::new();
        for (name, schema) in properties {
            let is_required = required.contains(name.as_str());
            let identifier = value_name(&name);
            let rename =
                (identifier.to_string().trim_start_matches("r#") != name).then(|| name.clone());
            let rust_type = self.schema_type_with_inline(
                schema,
                TypePosition::Nested,
                inline,
                in_module,
                &type_name(&name).to_string(),
            )?;
            let rust_type = if is_required {
                rust_type
            } else {
                RustType::Option(Box::new(rust_type))
            };
            output.push(SchemaField {
                name,
                identifier,
                rename,
                is_required,
                rust_type,
            });
        }
        Ok(output)
    }

    fn collect_object_fields_inner(
        &self,
        schema: &'a Schema,
        properties: &mut BTreeMap<String, &'a MaybeRef<Schema, SchemaReference>>,
        required: &mut BTreeSet<String>,
    ) -> BuildResult<()> {
        for item in &schema.all_of {
            let schema = self.resolve_schema(item)?;
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
        schema: &'a MaybeRef<Schema, SchemaReference>,
        rename: Option<String>,
        inline: &mut InlineSchemas,
    ) -> BuildResult<EnumVariant> {
        let (variant, rust_type) = match schema {
            MaybeRef::Ref(reference) => {
                let (name, _) = self.resolve_schema_reference(&reference.reference)?;
                (
                    type_name(name),
                    self.schema_type(schema, TypePosition::Nested)?,
                )
            }
            MaybeRef::Value(schema) => {
                let mut rust_type = self.schema_type_inner_with_inline(
                    schema,
                    TypePosition::Nested,
                    inline,
                    true,
                    &format!("Value{index}"),
                )?;
                if is_nullable(schema) {
                    rust_type = RustType::Option(Box::new(rust_type));
                }
                (RustName::new(format!("Value{index}")), rust_type)
            }
        };
        Ok(EnumVariant {
            name: variant,
            rename,
            aliases: Vec::new(),
            kind: EnumVariantKind::Tuple { rust_type },
        })
    }

    fn discriminator_variants(
        &self,
        schema: &'a Schema,
        inline: &mut InlineSchemas,
    ) -> BuildResult<Vec<EnumVariant>> {
        let discriminator = schema.discriminator.as_ref().ok_or_else(|| {
            BuildError::InvalidInput("schema is missing discriminator".to_owned())
        })?;
        let variants = if !schema.one_of.is_empty() {
            schema
                .one_of
                .iter()
                .map(|schema| self.discriminator_variant_from_schema(schema, discriminator, inline))
                .collect::<Result<Vec<_>, _>>()?
        } else if !schema.any_of.is_empty() {
            schema
                .any_of
                .iter()
                .enumerate()
                .map(|(index, schema)| {
                    self.discriminator_any_of_variant(index, schema, discriminator, inline)
                })
                .collect::<Result<Vec<_>, _>>()?
        } else {
            self.discriminator_variants_from_mapping(discriminator, inline)?
        };
        Ok(variants)
    }

    fn discriminator_variants_from_mapping(
        &self,
        discriminator: &crate::openapi::spec::Discriminator,
        inline: &mut InlineSchemas,
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
                self.discriminator_variant(&reference, values, &discriminator.property_name, inline)
            })
            .collect()
    }

    fn discriminator_variant_from_schema(
        &self,
        schema: &'a MaybeRef<Schema, SchemaReference>,
        discriminator: &crate::openapi::spec::Discriminator,
        inline: &mut InlineSchemas,
    ) -> BuildResult<EnumVariant> {
        let MaybeRef::Ref(reference) = schema else {
            return Err(BuildError::InvalidInput(
                "discriminator variants must be schema references".to_owned(),
            ));
        };
        let reference = &reference.reference;
        let values = self.discriminator_values(reference, discriminator)?;
        self.discriminator_variant(reference, values, &discriminator.property_name, inline)
    }

    fn discriminator_any_of_variant(
        &self,
        index: usize,
        schema: &'a MaybeRef<Schema, SchemaReference>,
        discriminator: &crate::openapi::spec::Discriminator,
        inline: &mut InlineSchemas,
    ) -> BuildResult<EnumVariant> {
        let MaybeRef::Ref(reference) = schema else {
            return Err(BuildError::InvalidInput(
                "discriminator anyOf variants must be schema references".to_owned(),
            ));
        };
        let values = self.discriminator_values(&reference.reference, discriminator)?;
        let rename = values.into_iter().next().ok_or_else(|| {
            BuildError::InvalidInput(format!(
                "discriminator mapping is missing an entry for {}",
                reference.reference
            ))
        })?;
        self.enum_schema_variant(index, schema, Some(rename), inline)
    }

    fn discriminator_values(
        &self,
        reference: &str,
        discriminator: &crate::openapi::spec::Discriminator,
    ) -> BuildResult<Vec<String>> {
        let values = discriminator
            .mapping
            .iter()
            .filter_map(|(value, candidate)| (candidate == reference).then_some(value.clone()))
            .collect::<Vec<_>>();
        if values.is_empty() {
            return Err(BuildError::InvalidInput(format!(
                "discriminator mapping is missing an entry for {reference}"
            )));
        }
        Ok(values)
    }

    fn discriminator_variant(
        &self,
        reference: &str,
        values: Vec<String>,
        tag: &str,
        inline: &mut InlineSchemas,
    ) -> BuildResult<EnumVariant> {
        let (name, schema) = self.resolve_schema_reference(reference)?;
        let variant = type_name(name);
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
            .collect_object_fields(schema, inline, true)?
            .into_iter()
            .filter(|field| field.name != tag)
            .collect::<Vec<_>>();
        let kind = if fields.is_empty() {
            EnumVariantKind::Unit
        } else {
            EnumVariantKind::Struct { fields }
        };
        Ok(EnumVariant {
            name: variant,
            rename: Some(rename),
            aliases,
            kind,
        })
    }
}

pub(super) fn has_schema_type(schema: &Schema, schema_type: SchemaType) -> bool {
    schema
        .r#type
        .as_ref()
        .is_some_and(|types| types.contains(&schema_type))
}

fn primitive_schema_type(schema: &Schema) -> Option<RustType> {
    if has_schema_type(schema, SchemaType::Boolean) {
        return Some(RustType::Bool);
    }
    if has_schema_type(schema, SchemaType::Integer) {
        return Some(match schema.format.as_deref() {
            Some("int64") => RustType::I64,
            _ => RustType::I32,
        });
    }
    if has_schema_type(schema, SchemaType::Number) {
        return Some(RustType::F64);
    }
    if has_schema_type(schema, SchemaType::String) {
        return Some(RustType::String);
    }
    None
}

fn string_enum_variants(schema: &Schema) -> BuildResult<Option<Vec<EnumVariant>>> {
    if schema.enum_values.is_empty() || !has_schema_type(schema, SchemaType::String) {
        return Ok(None);
    }
    let mut variants = Vec::new();
    for value in &schema.enum_values {
        variants.push(EnumVariant {
            name: type_name(value),
            rename: Some(value.clone()),
            aliases: Vec::new(),
            kind: EnumVariantKind::Unit,
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
    Ok(transparent_all_of(schema).is_none()
        && (!schema.enum_values.is_empty() && has_schema_type(schema, SchemaType::String)
            || discriminator_tag(schema).is_some()
            || any_of_tag(schema)?.is_some()
            || !schema.one_of.is_empty()
            || !schema.any_of.is_empty()
            || !schema.properties.is_empty()
            || schema.all_of.iter().any(|schema| match schema {
                MaybeRef::Value(schema) => {
                    !schema.properties.is_empty() || !schema.required.is_empty()
                }
                MaybeRef::Ref(_) => false,
            })))
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

fn transparent_all_of(schema: &Schema) -> Option<&MaybeRef<Schema, SchemaReference>> {
    let [item] = schema.all_of.as_slice() else {
        return None;
    };
    (schema.r#type.is_none()
        && schema.properties.is_empty()
        && schema.required.is_empty()
        && schema.items.is_none()
        && schema.additional_properties.is_none()
        && schema.one_of.is_empty()
        && schema.any_of.is_empty()
        && schema.enum_values.is_empty()
        && schema.const_value.is_none())
    .then_some(item)
}

pub(super) struct SchemaDefinition {
    type_name: RustName,
    summary: Option<String>,
    description: Option<String>,
    serde_type: Option<String>,
    module_name: Option<RustName>,
    inline_definitions: Vec<SchemaDefinition>,
    kind: SchemaKind,
}

enum SchemaKind {
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

struct EnumVariant {
    name: RustName,
    rename: Option<String>,
    aliases: Vec<String>,
    kind: EnumVariantKind,
}

enum EnumVariantKind {
    Unit,
    Tuple { rust_type: RustType },
    Struct { fields: Vec<SchemaField> },
}

struct SchemaField {
    name: String,
    identifier: RustName,
    rename: Option<String>,
    is_required: bool,
    rust_type: RustType,
}

struct InlineSchemas {
    module_name: RustName,
    definitions: Vec<SchemaDefinition>,
}

impl InlineSchemas {
    fn new(name: &str) -> Self {
        Self {
            module_name: value_name(name),
            definitions: Vec::new(),
        }
    }

    fn module_name(&self) -> Option<RustName> {
        if self.definitions.is_empty() {
            return None;
        }
        Some(self.module_name.clone())
    }
}

impl SchemaDefinition {
    fn new(
        type_name: RustName,
        schema: &Schema,
        serde_type: Option<String>,
        kind: SchemaKind,
    ) -> Self {
        Self {
            type_name,
            summary: schema.summary.clone(),
            description: schema.description.clone(),
            serde_type,
            module_name: None,
            inline_definitions: Vec::new(),
            kind,
        }
    }

    pub(super) fn tokens(&self) -> BuildResult<TokenStream> {
        let module = if let Some(module_name) = &self.module_name {
            let definitions = self
                .inline_definitions
                .iter()
                .map(SchemaDefinition::non_inline_tokens)
                .collect::<BuildResult<Vec<_>>>()?;
            Some(quote! {
                pub mod #module_name {
                    use super::*;

                    #(#definitions)*
                }
            })
        } else {
            None
        };
        let schema = self.non_inline_tokens()?;
        Ok(quote! {
            #module
            #schema
        })
    }

    fn non_inline_tokens(&self) -> BuildResult<TokenStream> {
        let type_name = &self.type_name;
        let docs = doc_attrs(self.summary.as_deref(), self.description.as_deref());
        let serde_type = self.serde_type.as_ref().map(|from_type| {
            quote! {
                #[serde(try_from = #from_type)]
            }
        });
        let derives = quote! { #[derive(Clone, Debug, serde::Deserialize, serde::Serialize)] };
        match &self.kind {
            SchemaKind::Enum {
                tag,
                untagged,
                variants,
            } => {
                let serde_tag = if serde_type.is_none() {
                    tag.as_ref()
                        .map(|tag| quote! { #[serde(tag = #tag)] })
                        .or_else(|| untagged.then(|| quote! { #[serde(untagged)] }))
                } else {
                    None
                };
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
            SchemaKind::Transparent { rust_type } => {
                let serde_transparent = serde_type
                    .is_none()
                    .then(|| quote! { #[serde(transparent)] });
                Ok(quote! {
                    #(#docs)*
                    #derives
                    #serde_transparent
                    #serde_type
                    pub struct #type_name(pub #rust_type);
                })
            }
        }
    }
}

fn generate_enum_variant(variant: &EnumVariant) -> TokenStream {
    let name = &variant.name;
    let rename = variant
        .rename
        .as_ref()
        .map(|value| quote! { #[serde(rename = #value)] });
    let aliases = variant
        .aliases
        .iter()
        .map(|value| quote! { #[serde(alias = #value)] });
    match &variant.kind {
        EnumVariantKind::Unit => {
            quote! {
                #rename
                #(#aliases)*
                #name,
            }
        }
        EnumVariantKind::Tuple { rust_type } => {
            quote! {
                #rename
                #(#aliases)*
                #name(#rust_type),
            }
        }
        EnumVariantKind::Struct { fields } => {
            let fields = fields.iter().map(generate_variant_field);
            quote! {
                #rename
                #(#aliases)*
                #name {
                    #(#fields)*
                },
            }
        }
    }
}

fn generate_schema_field(field: &SchemaField) -> TokenStream {
    generate_field(field, quote! { pub })
}

fn generate_variant_field(field: &SchemaField) -> TokenStream {
    generate_field(field, quote! {})
}

fn generate_field(field: &SchemaField, visibility: TokenStream) -> TokenStream {
    let name = &field.identifier;
    let rust_type = &field.rust_type;
    let rename = field
        .rename
        .as_ref()
        .map(|value| quote! { #[serde(rename = #value)] });
    let optional = (!field.is_required && field.rust_type.is_option())
        .then(|| quote! { #[serde(default, skip_serializing_if = "Option::is_none")] });
    quote! {
        #rename
        #optional
        #visibility #name: #rust_type,
    }
}
