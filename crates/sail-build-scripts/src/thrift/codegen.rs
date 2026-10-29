//! Rust syntax generation. IDL names and literals become tokens, never Rust source fragments.

use std::collections::{BTreeMap, BTreeSet};

use proc_macro2::{Ident, Literal as RustLiteral, TokenStream};
use quote::{format_ident, quote};
use syn::{Expr, parse_quote};

use super::{Definition, Field, Literal, Method, Schema, Type, invalid};
use crate::error::BuildResult;

fn snake(name: &str) -> String {
    let chars = name.chars().collect::<Vec<_>>();
    let mut output = String::new();
    for (i, c) in chars.iter().enumerate() {
        if c.is_ascii_uppercase()
            && i > 0
            && (chars[i - 1].is_ascii_lowercase()
                || (chars[i - 1].is_ascii_uppercase()
                    && chars.get(i + 1).is_some_and(char::is_ascii_lowercase)))
        {
            output.push('_');
        }
        output.push(c.to_ascii_lowercase());
    }
    output
}

fn field_name(name: &str) -> Ident {
    let name = snake(name);
    match name.as_str() {
        "self" | "super" | "crate" => format_ident!("{name}_"),
        _ if syn::parse_str::<Ident>(&name).is_ok() => format_ident!("{name}"),
        _ => format_ident!("r#{name}"),
    }
}

fn upper_camel(name: &str) -> String {
    let mut result = String::new();
    for part in name.split('_') {
        let mut chars = part.chars();
        if let Some(c) = chars.next() {
            result.extend(c.to_uppercase());
            result.extend(chars);
        }
    }
    result
}

impl Type {
    fn rust(&self) -> TokenStream {
        match self {
            Self::Named(n) => match n.as_str() {
                "void" => quote!(()),
                "string" => quote!(String),
                "binary" => quote!(Vec<u8>),
                "byte" => quote!(i8),
                "double" => quote!(f64),
                _ => {
                    let name = format_ident!("{n}");
                    quote!(#name)
                }
            },
            Self::List(t) => {
                let t = t.rust();
                quote!(Vec<#t>)
            }
            Self::Set(t) => {
                let t = t.rust();
                quote!(std::collections::HashSet<#t>)
            }
            Self::Map(k, v) => {
                let (k, v) = (k.rust(), v.rust());
                quote!(std::collections::HashMap<#k, #v>)
            }
        }
    }

    fn wire(&self, defs: &BTreeMap<String, Definition>) -> BuildResult<Ident> {
        let name = match self {
            Self::Named(n) => match n.as_str() {
                "bool" => "Bool",
                "byte" | "i8" => "I08",
                "i16" => "I16",
                "i32" => "I32",
                "i64" => "I64",
                "double" => "Double",
                "string" | "binary" => "String",
                _ => match defs.get(n) {
                    Some(Definition::Enum(_)) => "I32",
                    Some(Definition::Struct(_)) | Some(Definition::Exception(_)) => "Struct",
                    _ => return Err(invalid(format!("unsupported reachable Thrift type {n}"))),
                },
            },
            Self::List(_) => "List",
            Self::Set(_) => "Set",
            Self::Map(_, _) => "Map",
        };

        Ok(format_ident!("{name}"))
    }
}

struct Emitter<'a> {
    defs: &'a BTreeMap<String, Definition>,
    service: &'a str,
}

impl Emitter<'_> {
    fn read(&self, ty: &Type) -> BuildResult<TokenStream> {
        Ok(match ty {
            Type::Named(n) => match n.as_str() {
                "bool" | "i16" | "i32" | "i64" | "double" | "string" | "byte" | "i8" | "binary" => {
                    let suffix = match n.as_str() {
                        "byte" => "i8",
                        "binary" => "bytes",
                        _ => n,
                    };

                    let read = format_ident!("read_{suffix}");
                    quote!(p.#read()?)
                }
                _ => {
                    let name = format_ident!("{n}");
                    quote!(#name::read_from_in_protocol(p)?)
                }
            },
            Type::List(t) | Type::Set(t) => {
                let (begin, end, add) = if matches!(ty, Type::List(_)) {
                    (
                        format_ident!("read_list_begin"),
                        format_ident!("read_list_end"),
                        format_ident!("push"),
                    )
                } else {
                    (
                        format_ident!("read_set_begin"),
                        format_ident!("read_set_end"),
                        format_ident!("insert"),
                    )
                };

                let wire = t.wire(self.defs)?;
                let rust = ty.rust();
                let read = self.read(t)?;
                quote!({
                    let h = p.#begin()?;
                    ::sail_thrift::codec::check_container(h.size, h.element_type, TType::#wire)?;
                    let mut values = <#rust>::new();
                    for _ in 0..h.size {
                        values.#add(#read);
                    }

                    p.#end()?;
                    values
                })
            }
            Type::Map(k, v) => {
                let (kw, vw) = (k.wire(self.defs)?, v.wire(self.defs)?);
                let (kr, vr) = (self.read(k)?, self.read(v)?);
                quote!({
                    let h = p.read_map_begin()?;
                    ::sail_thrift::codec::check_map(&h, TType::#kw, TType::#vw)?;
                    let mut values = std::collections::HashMap::new();
                    for _ in 0..h.size {
                        let key = #kr;
                        let value = #vr;
                        values.insert(key, value);
                    }

                    p.read_map_end()?;
                    values
                })
            }
        })
    }

    /// `value` is always an expression borrowing the value to serialize.
    fn write(&self, ty: &Type, value: Expr) -> BuildResult<TokenStream> {
        let (receiver, scalar) = match &value {
            Expr::Reference(reference) => {
                let expr = &reference.expr;
                (quote!(#expr), quote!(#expr))
            }
            _ => (quote!(#value), quote!(*#value)),
        };

        Ok(match ty {
            Type::Named(n) => match n.as_str() {
                "bool" | "i16" | "i32" | "i64" | "double" | "byte" | "i8" => {
                    let suffix = if n == "byte" { "i8" } else { n };
                    let write = format_ident!("write_{suffix}");
                    quote!(p.#write(#scalar)?;)
                }
                "string" => quote!(p.write_string(#value)?;),
                "binary" => quote!(p.write_bytes(#value)?;),
                _ => quote!((#receiver).write_to_out_protocol(p)?;),
            },
            Type::List(t) | Type::Set(t) => {
                let (begin, end, header) = if matches!(ty, Type::List(_)) {
                    (
                        format_ident!("write_list_begin"),
                        format_ident!("write_list_end"),
                        format_ident!("TListIdentifier"),
                    )
                } else {
                    (
                        format_ident!("write_set_begin"),
                        format_ident!("write_set_end"),
                        format_ident!("TSetIdentifier"),
                    )
                };

                let wire = t.wire(self.defs)?;
                let write = self.write(t, parse_quote!(item))?;
                quote! {
                    p.#begin(&::thrift::protocol::#header::new(
                        TType::#wire, ::sail_thrift::codec::wire_len((#receiver).len())?
                    ))?;
                    for item in #value {
                        #write
                    }

                    p.#end()?;
                }
            }
            Type::Map(k, v) => {
                let (kw, vw) = (k.wire(self.defs)?, v.wire(self.defs)?);
                let (ks, vs) = (
                    self.write(k, parse_quote!(key))?,
                    self.write(v, parse_quote!(value))?,
                );
                quote! {
                    p.write_map_begin(&::thrift::protocol::TMapIdentifier::new(
                        TType::#kw, TType::#vw, ::sail_thrift::codec::wire_len((#receiver).len())?
                    ))?;
                    for (key, value) in #value {
                        #ks
                        #vs
                    }

                    p.write_map_end()?;
                }
            }
        })
    }

    fn write_field(&self, f: &Field, value: Expr) -> BuildResult<TokenStream> {
        let wire = f.ty.wire(self.defs)?;
        let id = RustLiteral::i16_unsuffixed(f.id);
        let name = &f.name;
        let write = self.write(&f.ty, value)?;
        Ok(quote! {
            p.write_field_begin(&::thrift::protocol::TFieldIdentifier::new(#name, TType::#wire, #id))?;
            #write
            p.write_field_end()?;
        })
    }

    fn default(&self, field: &Field) -> BuildResult<TokenStream> {
        let Some(literal) = &field.default else {
            return Ok(quote!(Default::default()));
        };

        let value = match literal {
            Literal::String(s) => quote!(#s.to_string()),
            Literal::Bool(b) => quote!(#b),
            Literal::Integer(i) => {
                let literal = RustLiteral::i64_unsuffixed(*i);
                quote!(#literal)
            }
            Literal::Double(f) => {
                let literal = RustLiteral::f64_unsuffixed(*f);
                quote!(#literal)
            }
            Literal::Reference(path) => {
                let path = path
                    .iter()
                    .map(|name| format_ident!("{name}"))
                    .collect::<Vec<_>>();
                quote!(#(#path)::* )
            }
        };

        Ok(if field.required {
            value
        } else {
            quote!(Some(#value))
        })
    }

    fn definition(&self, name: &str, definition: &Definition) -> BuildResult<TokenStream> {
        let rust_name = format_ident!("{name}");
        let fields = match definition {
            Definition::Enum(values) => {
                let constants = values.iter().map(|(name, value)| {
                    let name = format_ident!("{name}");
                    let value = RustLiteral::i32_unsuffixed(*value);
                    quote!(pub const #name: Self = Self(#value);)
                });
                let first = values
                    .first()
                    .ok_or_else(|| invalid(format!("empty Thrift enum {name}")))?
                    .1;
                let first = RustLiteral::i32_unsuffixed(first);
                return Ok(quote! {
                    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
                    pub struct #rust_name(pub i32);

                    impl #rust_name {
                        #(#constants)*
                    }

                    impl Default for #rust_name {
                        fn default() -> Self {
                            Self(#first)
                        }
                    }

                    impl std::fmt::Display for #rust_name {
                        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                            self.0.fmt(f)
                        }
                    }

                    impl TSerializable for #rust_name {
                        fn read_from_in_protocol(p: &mut dyn TInputProtocol) -> ::thrift::Result<Self> {
                            Ok(Self(p.read_i32()?))
                        }

                        fn write_to_out_protocol(&self, p: &mut dyn TOutputProtocol) -> ::thrift::Result<()> {
                            p.write_i32(self.0)
                        }
                    }
                });
            }
            Definition::Struct(fields) | Definition::Exception(fields) => fields,
            _ => return Err(invalid(format!("unsupported reachable definition {name}"))),
        };

        let mut members = Vec::new();
        let mut defaults = Vec::new();
        let mut locals = Vec::new();
        let mut readers = Vec::new();
        let mut initializers = Vec::new();
        let mut writers = Vec::new();
        for (index, field) in fields.iter().enumerate() {
            let member = field_name(&field.name);
            let ty = field.ty.rust();
            let ty = if field.required {
                ty
            } else {
                quote!(Option<#ty>)
            };
            members.push(quote!(pub #member: #ty,));
            let default = self.default(field)?;
            defaults.push(quote!(#member: #default,));
            // Local names are independent of wire IDs (which may be negative).
            let local = format_ident!("field_{index}");
            locals.push(quote!(let mut #local = None;));
            let id = RustLiteral::i16_unsuffixed(field.id);
            let wire = field.ty.wire(self.defs)?;
            let read = self.read(&field.ty)?;
            readers.push(quote!((Some(#id), TType::#wire) => { #local = Some(#read); },));
            let value = if field.required {
                let message = format!("missing required {name}.{}", field.name);
                quote!(#local.ok_or_else(|| ::sail_thrift::codec::invalid_data(#message))?)
            } else if field.default.is_some() {
                quote!(#local.or(#default))
            } else {
                quote!(#local)
            };
            initializers.push(quote!(#member: #value,));
            let write = self.write_field(field, parse_quote!(value))?;
            writers.push(if field.required {
                let write = self.write_field(field, parse_quote!(&self.#member))?;
                quote!(#write)
            } else {
                quote!(if let Some(value) = &self.#member { #write })
            });
        }

        let has_defaults = fields.iter().any(|f| f.default.is_some());
        let default_derive = if has_defaults {
            quote!()
        } else {
            quote!(, Default)
        };

        let default_impl = has_defaults.then(|| {
            quote! {
                impl Default for #rust_name {
                    fn default() -> Self {
                        Self { #(#defaults)* }
                    }
                }
            }
        });
        let exception_impl = matches!(definition, Definition::Exception(_)).then(|| {
            quote! {
                impl std::fmt::Display for #rust_name {
                    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(f, "{self:?}")
                    }
                }

                impl std::error::Error for #rust_name {}
            }
        });
        Ok(quote! {
            #[derive(Debug, Clone, PartialEq #default_derive)]
            pub struct #rust_name {
                #(#members)*
            }

            #default_impl

            impl TSerializable for #rust_name {
                fn read_from_in_protocol(p: &mut dyn TInputProtocol) -> ::thrift::Result<Self> {
                    #(#locals)*
                    p.read_struct_begin()?;
                    loop {
                        let field = p.read_field_begin()?;
                        if field.field_type == TType::Stop { break; }
                        match (field.id, field.field_type) {
                            #(#readers)*
                            _ => p.skip(field.field_type)?,
                        }

                        p.read_field_end()?;
                    }

                    p.read_struct_end()?;
                    Ok(Self { #(#initializers)* })
                }

                fn write_to_out_protocol(&self, p: &mut dyn TOutputProtocol) -> ::thrift::Result<()> {
                    p.write_struct_begin(&::thrift::protocol::TStructIdentifier::new(#name))?;
                    #(#writers)*
                    p.write_field_stop()?;
                    p.write_struct_end()
                }
            }
            #exception_impl
        })
    }

    fn method(&self, method: &Method) -> BuildResult<(TokenStream, TokenStream)> {
        let exception = format_ident!("{}{}Exception", self.service, upper_camel(&method.name));
        let mut variants = Vec::new();
        let mut arguments = Vec::new();
        let mut writers = Vec::new();
        let mut readers = Vec::new();
        for field in &method.args {
            let arg = field_name(&field.name);
            let ty = field.ty.rust();
            arguments.push(quote!(#arg: #ty,));
            writers.push(self.write_field(field, parse_quote!(&#arg))?);
        }

        let is_void = matches!(&method.result, Type::Named(n) if n == "void");
        let success_local = (!is_void).then(|| quote!(let mut success = None;));
        let occupied = if is_void {
            quote!(exception.is_some())
        } else {
            quote!(exception.is_some() || success.is_some())
        };

        if !is_void {
            let wire = method.result.wire(self.defs)?;
            let read = self.read(&method.result)?;
            readers.push(quote! {
                (Some(0), TType::#wire) => {
                    if #occupied { return Err(::sail_thrift::codec::invalid_data("multiple RPC results")); }
                    success = Some(#read);
                },
            });
        }

        for field in &method.exceptions {
            let variant = format_ident!("{}", upper_camel(&field.name));
            let ty = field.ty.rust();
            variants.push(quote!(#variant(#ty),));
            let id = RustLiteral::i16_unsuffixed(field.id);
            let wire = field.ty.wire(self.defs)?;
            let read = self.read(&field.ty)?;
            readers.push(quote! {
                (Some(#id), TType::#wire) => {
                    if #occupied { return Err(::sail_thrift::codec::invalid_data("multiple RPC results")); }
                    exception = Some(#exception::#variant(#read));
                },
            });
        }

        let finish = if is_void {
            quote!(Ok(::sail_thrift::MaybeException::Ok(())))
        } else {
            quote! {
                success.map(::sail_thrift::MaybeException::Ok).ok_or_else(|| {
                    ::thrift::ApplicationError::new(::thrift::ApplicationErrorKind::MissingResult, "missing RPC result").into()
                })
            }
        };

        let result = method.result.rust();
        let wire_name = &method.name;
        let name = field_name(wire_name);
        Ok((
            quote! {
                #[derive(Debug, Clone, PartialEq)]
                pub enum #exception {
                    #(#variants)*
                }
            },
            quote! {
                pub async fn #name(&self, #(#arguments)*) -> Result<
                    ::sail_thrift::MaybeException<#result, #exception>, ::sail_thrift::ClientError
                > {
                    self.0.call(#wire_name, |p| {
                        p.write_struct_begin(&::thrift::protocol::TStructIdentifier::new("args"))?;
                        #(#writers)*
                        p.write_field_stop()?;
                        p.write_struct_end()
                    }, |p| {
                        #success_local
                        let mut exception: Option<#exception> = None;
                        p.read_struct_begin()?;
                        loop {
                            let field = p.read_field_begin()?;
                            if field.field_type == TType::Stop { break; }
                            match (field.id, field.field_type) {
                                #(#readers)*
                                _ => p.skip(field.field_type)?,
                            }

                            p.read_field_end()?;
                        }

                        p.read_struct_end()?;
                        if let Some(e) = exception { return Ok(::sail_thrift::MaybeException::Exception(e)); }
                        #finish
                    }).await
                }
            },
        ))
    }
}

pub(super) fn generate(
    schema: &Schema,
    needed: &BTreeSet<String>,
    selected: &BTreeSet<&str>,
) -> BuildResult<TokenStream> {
    let emitter = Emitter {
        defs: &schema.definitions,
        service: &schema.service,
    };

    let definitions = needed
        .iter()
        .map(|name| emitter.definition(name, &schema.definitions[name]))
        .collect::<BuildResult<Vec<_>>>()?;
    let (exceptions, methods): (Vec<_>, Vec<_>) = selected
        .iter()
        .map(|name| emitter.method(&schema.methods[*name]))
        .collect::<BuildResult<Vec<_>>>()?
        .into_iter()
        .unzip();
    let client = format_ident!("{}Client", schema.service);
    Ok(quote! {
        use ::thrift::protocol::{TSerializable, TInputProtocol, TOutputProtocol, TType};
        #(#definitions)*

        #(#exceptions)*

        #[derive(Clone)]
        pub struct #client(pub ::sail_thrift::Client);

        impl #client {
            #(#methods)*
        }
    })
}
