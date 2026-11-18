// SPDX-License-Identifier: Apache-2.0

use std::borrow::Cow;
use std::collections::HashMap;

use chumsky::prelude::*;
use quick_xml::events::{BytesRef, BytesStart, BytesText, Event};
use quick_xml::{Reader, Writer};

use super::parser::{Extra, literal, name, space, whitespace};
use super::reader::{XmlResult, reference_value};

/// The Java DOM parser expands internal general entities but disables external
/// entities. quick-xml deliberately leaves DTD handling to the application.
/// Keep references in comments and CDATA untouched, and never load external data.
pub(super) fn expand(xml: &str) -> XmlResult<Cow<'_, str>> {
    if !xml.contains("<!DOCTYPE") {
        return Ok(Cow::Borrowed(xml));
    }
    let mut reader = Reader::from_str(xml);
    let mut collector = DeclarationCollector::default();
    loop {
        match reader.read_event()? {
            Event::DocType(doctype) => {
                let dtd = doctype.xml10_content()?;

                collector.collect(
                    declarations()
                        .parse(dtd.as_ref())
                        .into_result()
                        .map_err(|errors| format!("Invalid XML document type: {errors:?}"))?,
                )?;
            }
            Event::Start(_) | Event::Empty(_) | Event::Eof => break,
            _ => {}
        }
    }
    if collector.entities.is_empty() {
        return Ok(Cow::Borrowed(xml));
    }
    let mut expansion = Expansion {
        entities: &collector.entities,
        limits: collector.limits,
    };
    Ok(Cow::Owned(expansion.content(xml)?))
}

// Character references are decoded when declarations are read. General entity
// references remain literal until the replacement text is parsed in its context.
fn declaration_value(value: &str) -> XmlResult<String> {
    let mut result = String::new();
    for part in entity_parts(value)? {
        match part {
            EntityPart::Reference { name, .. } if name.starts_with('#') => {
                result.push_str(&reference_value(&BytesRef::new(name))?);
            }
            EntityPart::Reference { raw, .. } | EntityPart::Text(raw) => result.push_str(raw),
        }
    }
    Ok(result)
}

#[derive(Default)]
struct DeclarationCollector {
    entities: HashMap<String, String>,
    parameters: HashMap<String, String>,
    limits: ExpansionLimits,
}

impl DeclarationCollector {
    fn collect(&mut self, declarations: Vec<Declaration<'_>>) -> XmlResult<()> {
        for declaration in declarations {
            match declaration {
                Declaration::Entity {
                    parameter,
                    name,
                    value,
                } => {
                    let value = declaration_value(value)?;
                    let entities = if parameter {
                        &mut self.parameters
                    } else {
                        &mut self.entities
                    };
                    entities.entry(name.to_string()).or_insert(value);
                }
                Declaration::Parameter(name) => {
                    // The nonvalidating Java parser ignores undeclared parameter
                    // entities. External declarations have empty replacement text.
                    let Some(value) = self.parameters.get(name) else {
                        continue;
                    };

                    self.limits.enter(name, value.len())?;
                    let value = value.clone();
                    let declarations = declaration_subset()
                        .parse(&value)
                        .into_result()
                        .map_err(|errors| format!("Invalid XML parameter entity: {errors:?}"))?;
                    self.collect(declarations)?;
                    self.limits.active.pop();
                }
            }
        }

        Ok(())
    }
}

#[derive(Default)]
struct ExpansionLimits {
    active: Vec<String>,
    count: usize,
    bytes: usize,
}

impl ExpansionLimits {
    fn enter(&mut self, name: &str, bytes: usize) -> XmlResult<()> {
        self.count += 1;
        if self.count > 64_000
            || self.active.len() >= 100
            || self.active.iter().any(|active| active == name)
        {
            return Err("XML entity expansion limit or recursive entity".into());
        }

        // Charge every reference before allocating its expansion. Parameter and
        // general entities share the document count and replacement byte limits.
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .filter(|bytes| *bytes <= 10_000_000)
            .ok_or("XML entity size limit")?;
        self.active.push(name.to_string());
        Ok(())
    }
}

struct Expansion<'a> {
    entities: &'a HashMap<String, String>,
    limits: ExpansionLimits,
}

impl Expansion<'_> {
    fn entity(&mut self, name: &str, attribute: bool) -> XmlResult<String> {
        let value = self.entities.get(name).ok_or("Unknown XML entity")?;
        self.limits.enter(name, value.len())?;
        let result = if attribute {
            self.attribute(value)
        } else {
            self.content(value)
        };
        self.limits.active.pop();
        result
    }

    fn content(&mut self, xml: &str) -> XmlResult<String> {
        // Normalize each source before joining replacements. A trailing CR in
        // one entity and a leading LF in the next source are two line breaks.
        let xml = xml.replace("\r\n", "\n").replace('\r', "\n");
        let mut reader = Reader::from_str(&xml);
        let mut writer = Writer::new(Vec::new());
        loop {
            match reader.read_event()? {
                Event::GeneralRef(reference) => {
                    let name = reference.decode()?;
                    if self.entities.contains_key(name.as_ref()) {
                        let value = self.entity(&name, false)?;
                        writer.write_event(Event::Text(BytesText::from_escaped(value)))?;
                    } else {
                        writer.write_event(Event::GeneralRef(reference))?;
                    }
                }
                Event::Start(element) => {
                    writer.write_event(Event::Start(self.attributes(element)?))?;
                }
                Event::Empty(element) => {
                    writer.write_event(Event::Empty(self.attributes(element)?))?;
                }
                Event::Eof => break,
                // Parse replacement text as XML so references inside comments,
                // CDATA, and processing instructions are never substituted.
                event => writer.write_event(event)?,
            }
        }
        Ok(String::from_utf8(writer.into_inner())?)
    }

    fn attributes(&mut self, element: BytesStart<'_>) -> XmlResult<BytesStart<'static>> {
        let name = element.name();
        let mut result = BytesStart::new(std::str::from_utf8(name.as_ref())?.to_string());
        for attribute in element.attributes() {
            let attribute = attribute?;
            let value = std::str::from_utf8(&attribute.value)?;
            let value = self.attribute(value)?.replace('"', "&quot;");
            result.push_attribute((attribute.key.as_ref(), value.as_bytes()));
        }
        Ok(result)
    }

    fn attribute(&mut self, value: &str) -> XmlResult<String> {
        // Normalize literal whitespace within each replacement independently.
        // Character-reference spellings remain intact for attribute_value.
        let value = value.replace("\r\n", " ").replace(['\r', '\n', '\t'], " ");
        let mut result = String::new();
        for part in entity_parts(&value)? {
            match part {
                EntityPart::Reference { name, .. } if self.entities.contains_key(name) => {
                    result.push_str(&self.entity(name, true)?);
                }
                EntityPart::Reference { raw, .. } | EntityPart::Text(raw) => {
                    result.push_str(raw);
                }
            }
        }
        Ok(result)
    }
}

fn entity_parts(value: &str) -> XmlResult<Vec<EntityPart<'_>>> {
    references()
        .parse(value)
        .into_result()
        .map_err(|errors| format!("Invalid XML entity value: {errors:?}").into())
}

#[derive(Clone, Debug)]
enum Declaration<'a> {
    Entity {
        parameter: bool,
        name: &'a str,
        value: &'a str,
    },
    Parameter(&'a str),
}

/// Parse the document type payload supplied by quick-xml. Consume quoted values
/// and comments as complete units so text resembling an entity declaration inside
/// them cannot be interpreted as a declaration.
fn declarations<'a>() -> impl Parser<'a, &'a str, Vec<Declaration<'a>>, Extra<'a>> {
    name(true)
        .then_ignore(external_id().padded_by(whitespace()).or_not())
        .ignore_then(
            declaration_subset()
                .delimited_by(just('['), just(']'))
                .padded_by(whitespace())
                .or_not(),
        )
        .then_ignore(whitespace())
        .map(Option::unwrap_or_default)
        .padded_by(whitespace())
}

fn external_id<'a>() -> impl Parser<'a, &'a str, (), Extra<'a>> + Clone {
    choice((
        just("SYSTEM")
            .then_ignore(space())
            .then(literal())
            .ignored(),
        just("PUBLIC")
            .then_ignore(space())
            .then(literal())
            .then_ignore(space())
            .then(literal())
            .ignored(),
    ))
}

fn declaration_subset<'a>() -> impl Parser<'a, &'a str, Vec<Declaration<'a>>, Extra<'a>> {
    let entity_value = literal().or(external_id()
        .then(just("NDATA").padded_by(space()).then(name(true)).or_not())
        .to(""));
    let entity = just("<!ENTITY")
        .ignore_then(space())
        .ignore_then(just('%').then_ignore(space()).or_not())
        .then(name(true).then_ignore(space()).then(entity_value))
        .then_ignore(whitespace())
        .then_ignore(just('>'))
        .map(|(parameter, (name, value))| {
            Some(Declaration::Entity {
                parameter: parameter.is_some(),
                name,
                value,
            })
        });
    let comment = just("<!--")
        .ignore_then(any().and_is(just("-->").not()).repeated())
        .then_ignore(just("-->"));
    let instruction = just("<?")
        .ignore_then(any().and_is(just("?>").not()).repeated())
        .then_ignore(just("?>"));
    let other_declaration = just("<!")
        .ignore_then(choice((just("ELEMENT"), just("ATTLIST"), just("NOTATION"))))
        .then_ignore(space())
        .then_ignore(literal().ignored().or(none_of("\"'>").ignored()).repeated())
        .then_ignore(just('>'));
    let parameter = just('%')
        .ignore_then(name(true))
        .then_ignore(just(';'))
        .map(|name| Some(Declaration::Parameter(name)));
    choice((
        entity,
        comment.to(None),
        instruction.to(None),
        other_declaration.to(None),
        parameter,
    ))
    .padded_by(whitespace())
    .repeated()
    .collect::<Vec<_>>()
    .then_ignore(whitespace())
    .map(|declarations| declarations.into_iter().flatten().collect())
}

#[derive(Debug)]
enum EntityPart<'a> {
    Text(&'a str),
    Reference { name: &'a str, raw: &'a str },
}

fn references<'a>() -> impl Parser<'a, &'a str, Vec<EntityPart<'a>>, Extra<'a>> {
    let character = just('#')
        .then(choice((
            just('x')
                .then(any().filter(char::is_ascii_hexdigit).repeated().at_least(1))
                .ignored(),
            any()
                .filter(char::is_ascii_digit)
                .repeated()
                .at_least(1)
                .ignored(),
        )))
        .to_slice();
    let reference = name(true)
        .or(character)
        .delimited_by(just('&'), just(';'))
        .map_with(|name, extra| EntityPart::Reference {
            name,
            raw: extra.slice(),
        });
    let text = none_of('&')
        .repeated()
        .at_least(1)
        .to_slice()
        .map(EntityPart::Text);
    reference.or(text).repeated().collect()
}
