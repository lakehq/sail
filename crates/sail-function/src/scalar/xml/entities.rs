use std::borrow::Cow;
use std::collections::HashMap;

use chumsky::prelude::*;
use quick_xml::events::{BytesStart, BytesText, Event};
use quick_xml::{Reader, Writer};

use super::parser::{Extra, literal, name, space, whitespace};
use super::reader::XmlResult;

/// The Java DOM parser expands internal general entities but disables external
/// entities. quick-xml deliberately leaves DTD handling to the application.
/// Keep references in comments and CDATA untouched, and never load external data.
pub(super) fn expand(xml: &str) -> XmlResult<Cow<'_, str>> {
    if !xml.contains("<!DOCTYPE") {
        return Ok(Cow::Borrowed(xml));
    }
    let mut reader = Reader::from_str(xml);
    let mut entities = HashMap::new();
    loop {
        match reader.read_event()? {
            Event::DocType(doctype) => {
                let dtd = doctype.decode()?;

                for (name, value) in declarations()
                    .parse(dtd.as_ref())
                    .into_result()
                    .map_err(|errors| format!("Invalid XML document type: {errors:?}"))?
                {
                    entities
                        .entry(name.to_string())
                        .or_insert_with(|| value.to_string());
                }
            }
            Event::Start(_) | Event::Empty(_) | Event::Eof => break,
            _ => {}
        }
    }
    if entities.is_empty() {
        return Ok(Cow::Borrowed(xml));
    }
    let mut reader = Reader::from_str(xml);
    let mut writer = Writer::new(Vec::new());
    let mut expansions = 0;
    loop {
        match reader.read_event()? {
            Event::GeneralRef(reference) => {
                let name = reference.decode()?;
                if let Some(value) = entities.get(name.as_ref()) {
                    let value = substitute(value, &entities, &mut Vec::new(), &mut expansions)?;
                    writer.write_event(Event::Text(BytesText::from_escaped(value)))?;
                } else {
                    writer.write_event(Event::GeneralRef(reference))?;
                }
            }
            Event::Start(element) => {
                writer.write_event(Event::Start(attributes(
                    element,
                    &entities,
                    &mut expansions,
                )?))?;
            }
            Event::Empty(element) => {
                writer.write_event(Event::Empty(attributes(
                    element,
                    &entities,
                    &mut expansions,
                )?))?;
            }
            Event::Eof => break,
            event => writer.write_event(event)?,
        }
    }
    Ok(Cow::Owned(String::from_utf8(writer.into_inner())?))
}

fn attributes(
    element: BytesStart<'_>,
    entities: &HashMap<String, String>,
    expansions: &mut usize,
) -> XmlResult<BytesStart<'static>> {
    let name = element.name();
    let mut result = BytesStart::new(std::str::from_utf8(name.as_ref())?.to_string());
    for attribute in element.attributes() {
        let attribute = attribute?;
        let value = std::str::from_utf8(&attribute.value)?;
        let value =
            substitute(value, entities, &mut Vec::new(), expansions)?.replace('"', "&quot;");
        result.push_attribute((attribute.key.as_ref(), value.as_bytes()));
    }
    Ok(result)
}

fn substitute(
    value: &str,
    entities: &HashMap<String, String>,
    active: &mut Vec<String>,
    expansions: &mut usize,
) -> XmlResult<String> {
    let mut result = String::new();
    let parts = references()
        .parse(value)
        .into_result()
        .map_err(|errors| format!("Invalid XML entity value: {errors:?}"))?;
    for part in parts {
        match part {
            EntityPart::Reference { name, .. } if entities.contains_key(name) => {
                let value = &entities[name];
                *expansions += 1;
                // Bound recursive expansion, as the Java parser does with its entity
                // expansion limit. Also bound depth and bytes to avoid stack exhaustion.
                if *expansions > 64_000 || active.len() >= 100 || active.iter().any(|n| n == name) {
                    return Err("XML entity expansion limit or recursive entity".into());
                }
                active.push(name.to_string());
                result.push_str(&substitute(value, entities, active, expansions)?);
                active.pop();
            }
            EntityPart::Reference { raw, .. } | EntityPart::Text(raw) => result.push_str(raw),
        }
        if result.len() > 10_000_000 {
            return Err("XML entity size limit".into());
        }
    }
    Ok(result)
}

/// Parse the document type payload supplied by quick-xml. Consume quoted values
/// and comments as complete units so text resembling an entity declaration inside
/// them cannot be interpreted as a declaration.
fn declarations<'a>() -> impl Parser<'a, &'a str, Vec<(&'a str, &'a str)>, Extra<'a>> {
    let external = choice((
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
    ));
    let entity_value = literal().or(external
        .clone()
        .then(just("NDATA").padded_by(space()).then(name(true)).or_not())
        .to(""));
    let entity = just("<!ENTITY")
        .ignore_then(space())
        .ignore_then(just('%').then_ignore(space()).or_not())
        .then(name(true).then_ignore(space()).then(entity_value))
        .then_ignore(whitespace())
        .then_ignore(just('>'))
        .map(|(parameter, entity)| parameter.is_none().then_some(entity));
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
    let parameter = just('%').then(name(true)).then_ignore(just(';'));
    let subset = choice((
        entity,
        comment.to(None),
        instruction.to(None),
        other_declaration.to(None),
        parameter.to(None),
    ))
    .padded_by(whitespace())
    .repeated()
    .collect::<Vec<_>>()
    .then_ignore(whitespace())
    .delimited_by(just('['), just(']'));
    name(true)
        .then_ignore(external.padded_by(whitespace()).or_not())
        .ignore_then(subset.padded_by(whitespace()).or_not())
        .then_ignore(whitespace())
        .map(|entities| entities.unwrap_or_default().into_iter().flatten().collect())
        .padded_by(whitespace())
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
