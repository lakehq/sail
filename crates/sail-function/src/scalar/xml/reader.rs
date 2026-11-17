use std::collections::{HashMap, HashSet};

use quick_xml::Reader;
use quick_xml::escape::{resolve_predefined_entity, unescape};
use quick_xml::events::attributes::Attribute;
use quick_xml::events::{BytesRef, Event};
use xot::{Node, Xot};

pub(super) type XmlResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// These names cannot occur in XML. They represent namespace nodes for the
/// namespace axis, which xee does not implement. Ordinary attribute tests hide them.
pub(super) const NAMESPACE_ATTRIBUTE: &str = "{namespace}";

pub(super) fn local_name(name: &str) -> &str {
    name.rsplit_once(':').map_or(name, |(_, local)| local)
}

pub(super) fn attribute_value(attribute: &Attribute<'_>) -> XmlResult<String> {
    // Normalize literal whitespace before unescaping. Character references must
    // retain their original characters, including tabs and carriage returns.
    validate_name(std::str::from_utf8(attribute.key.as_ref())?)?;
    let raw = std::str::from_utf8(&attribute.value)?;
    if raw.contains('<') {
        return Err("Unescaped < in XML attribute".into());
    }
    let normalized = raw.replace("\r\n", " ").replace(['\r', '\n', '\t'], " ");
    let value = unescape(&normalized)?.into_owned();
    if !value.chars().all(valid_character) {
        return Err("Invalid XML attribute character".into());
    }
    Ok(value)
}

pub(super) fn reference_value(reference: &BytesRef<'_>) -> XmlResult<String> {
    if let Some(character) = reference.resolve_char_ref()? {
        if !valid_character(character) {
            return Err("Invalid XML character reference".into());
        }
        Ok(character.to_string())
    } else {
        let name = reference.decode()?;
        Ok(resolve_predefined_entity(&name)
            .ok_or("Unknown XML entity")?
            .to_string())
    }
}

fn valid_character(c: char) -> bool {
    matches!(c, '\t' | '\n' | '\r' | '\u{20}'..='\u{d7ff}' | '\u{e000}'..='\u{fffd}' | '\u{10000}'..='\u{10ffff}')
}

/// Spark parses lexical XML names without namespace validation. Build the arena
/// directly so xot does not reject undeclared prefixes or resolve default namespaces.
/// XPath retains comments and processing instructions. Struct readers use them
/// only to separate text events, just like the filtered StAX reader.
pub(super) struct ParsedDocument {
    pub node: Node,
    pub text_values: HashMap<Node, String>,
}

pub(super) enum ParseMode {
    Struct,
    XPathValue,
    XPathNodes,
}

#[derive(Default)]
struct TextFragments {
    values: HashMap<Node, String>,
    frozen: HashSet<Node>,
}

pub(super) fn parse(xot: &mut Xot, xml: &str, mode: ParseMode) -> XmlResult<ParsedDocument> {
    let xpath = !matches!(mode, ParseMode::Struct);
    if !xml.chars().all(valid_character) {
        return Err("Invalid XML character".into());
    }
    let xml = if xpath {
        super::entities::expand(xml)?
    } else {
        std::borrow::Cow::Borrowed(xml)
    };
    let mut reader = Reader::from_str(xml.trim_start_matches('\u{feff}'));
    reader.config_mut().expand_empty_elements = true;
    reader.config_mut().check_comments = true;
    let document = xot.new_document();
    let mut parent = document;
    let mut root_seen = false;
    let mut declaration_allowed = true;
    let mut doctype_seen = false;
    let mut fragments = matches!(mode, ParseMode::XPathNodes).then(TextFragments::default);
    loop {
        match reader.read_event()? {
            Event::Start(element) => {
                if parent == document {
                    if root_seen {
                        return Err("Multiple XML root elements".into());
                    }
                    root_seen = true;
                }
                let name = element.name();
                let name = std::str::from_utf8(name.as_ref())?;
                validate_name(name)?;
                let name = xot.add_name(name);
                let node = xot.new_element(name);
                xot.append(parent, node)?;
                if xpath {
                    let inherited: Vec<_> = xot
                        .attributes(parent)
                        .iter()
                        .filter(|(name, _)| {
                            xot.local_name_str(*name).starts_with(NAMESPACE_ATTRIBUTE)
                        })
                        .map(|(name, value)| (name, value.clone()))
                        .collect();
                    for (name, value) in inherited {
                        xot.attributes_mut(node).insert(name, value);
                    }
                }
                let mut attributes = element
                    .attributes()
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                if xpath {
                    attributes.sort_by(|a, b| a.key.as_ref().cmp(b.key.as_ref()));
                }
                for attribute in attributes {
                    let name = std::str::from_utf8(attribute.key.as_ref())?;
                    let value = attribute_value(&attribute)?;
                    if xpath && (name == "xmlns" || name.starts_with("xmlns:")) {
                        // In the Java namespace-unaware DOM, namespace declarations
                        // share an expanded type. The last declaration replaces the
                        // preceding binding, including inherited bindings.
                        let old: Vec<_> = xot
                            .attributes(node)
                            .iter()
                            .filter(|(key, _)| {
                                let name = xot.local_name_str(*key);
                                name.starts_with(NAMESPACE_ATTRIBUTE) && name != "{namespace}xml"
                            })
                            .map(|(key, _)| key)
                            .collect();
                        for key in old {
                            xot.attributes_mut(node).remove(key);
                        }
                    }
                    let name = if xpath && name == "xmlns" {
                        NAMESPACE_ATTRIBUTE.to_string()
                    } else if xpath {
                        name.strip_prefix("xmlns:")
                            .map(|prefix| format!("{NAMESPACE_ATTRIBUTE}{prefix}"))
                            .unwrap_or_else(|| name.to_string())
                    } else {
                        name.to_string()
                    };
                    let name = xot.add_name(&name);
                    xot.attributes_mut(node).insert(name, value);
                }
                if xpath {
                    let name = xot.add_name(&format!("{NAMESPACE_ATTRIBUTE}xml"));
                    if xot.attributes(node).get(name).is_none() {
                        xot.attributes_mut(node)
                            .insert(name, "http://www.w3.org/XML/1998/namespace".into());
                    }
                }
                parent = node;
            }
            Event::End(_) => {
                parent = xot.parent(parent).ok_or("Unmatched XML end tag")?;
            }
            Event::Text(text) => {
                let text = text.xml10_content()?;
                if text.contains("]]>") {
                    return Err("Forbidden ]]> in XML text".into());
                }

                if parent == document {
                    if !text.trim_matches([' ', '\t', '\n', '\r']).is_empty() {
                        return Err("Text outside XML root element".into());
                    }
                } else {
                    append_text(xot, parent, &text, false, &mut fragments)?;
                }
            }
            Event::CData(text) if parent != document => {
                append_text(xot, parent, &text.xml10_content()?, true, &mut fragments)?;
            }
            Event::GeneralRef(reference) if parent != document => {
                append_text(
                    xot,
                    parent,
                    &reference_value(&reference)?,
                    false,
                    &mut fragments,
                )?;
            }
            Event::Comment(comment) => {
                xot.append_comment(parent, &comment.xml10_content()?)?;
            }
            Event::PI(instruction) => {
                let target = xot.add_name(std::str::from_utf8(instruction.target())?);
                let content = std::str::from_utf8(instruction.content())?
                    .trim_start_matches([' ', '\t', '\r', '\n'])
                    .replace("\r\n", "\n")
                    .replace('\r', "\n");
                xot.append_processing_instruction(parent, target, Some(&content))?;
            }
            Event::Decl(declaration) if declaration_allowed => {
                declaration.version()?;
            }
            Event::DocType(_) if parent == document && !root_seen && !doctype_seen => {
                doctype_seen = true;
            }
            Event::Eof if parent == document && root_seen => {
                return Ok(ParsedDocument {
                    node: document,
                    text_values: fragments.map(|f| f.values).unwrap_or_default(),
                });
            }
            _ => return Err("Unexpected XML event or incomplete document".into()),
        }

        declaration_allowed = false;
    }
}

// xot coalesces adjacent text. Retain the first DOM fragment for Node.getNodeValue,
// while the coalesced tree supplies the XPath string value used by typed functions.
fn append_text(
    xot: &mut Xot,
    parent: Node,
    text: &str,
    cdata: bool,
    fragments: &mut Option<TextFragments>,
) -> XmlResult<()> {
    if text.is_empty() {
        return Ok(());
    }

    xot.append_text(parent, text)?;
    let Some(TextFragments { values, frozen }) = fragments else {
        return Ok(());
    };

    let node = xot.last_child(parent).ok_or("Missing XML text node")?;
    match values.entry(node) {
        std::collections::hash_map::Entry::Vacant(entry) => {
            entry.insert(text.to_string());
        }
        std::collections::hash_map::Entry::Occupied(mut entry)
            if !cdata && !frozen.contains(&node) =>
        {
            entry.get_mut().push_str(text);
        }
        _ => {}
    }
    if cdata {
        frozen.insert(node);
    }
    Ok(())
}

pub(super) fn name_start(c: char) -> bool {
    matches!(c, ':' | '_' | 'A'..='Z' | 'a'..='z' | '\u{c0}'..='\u{d6}' | '\u{d8}'..='\u{f6}' |
        '\u{f8}'..='\u{2ff}' | '\u{370}'..='\u{37d}' | '\u{37f}'..='\u{1fff}' |
        '\u{200c}'..='\u{200d}' | '\u{2070}'..='\u{218f}' | '\u{2c00}'..='\u{2fef}' |
        '\u{3001}'..='\u{d7ff}' | '\u{f900}'..='\u{fdcf}' | '\u{fdf0}'..='\u{fffd}' |
        '\u{10000}'..='\u{effff}')
}

pub(super) fn name_char(c: char) -> bool {
    name_start(c)
        || matches!(c, '-' | '.' | '0'..='9' | '\u{b7}' | '\u{300}'..='\u{36f}' | '\u{203f}'..='\u{2040}')
}

pub(super) fn validate_name(name: &str) -> XmlResult<()> {
    let mut chars = name.chars();
    if !chars.next().is_some_and(name_start) || !chars.all(name_char) {
        return Err(format!("Invalid XML name: {name}").into());
    }
    Ok(())
}
