use proc_macro2::Ident;

pub fn type_ident(value: &str) -> String {
    type_name_text(value)
}

pub fn value_ident(value: &str) -> String {
    let value = identifier_text(value);
    if syn::parse_str::<Ident>(&value).is_ok() {
        value
    } else if syn::parse_str::<Ident>(&format!("r#{value}")).is_ok() {
        format!("r#{value}")
    } else {
        format!("{value}_")
    }
}

pub fn to_snake_case(value: &str) -> String {
    let mut output = String::new();
    let characters = value.chars().collect::<Vec<_>>();
    for (index, character) in characters.iter().copied().enumerate() {
        if character.is_ascii_uppercase() {
            let previous = index.checked_sub(1).and_then(|index| characters.get(index));
            let next = characters.get(index + 1);
            if previous.is_some_and(|character| {
                character.is_ascii_lowercase()
                    || character.is_ascii_digit()
                    || character.is_ascii_uppercase()
                        && next.is_some_and(|character| character.is_ascii_lowercase())
            }) {
                output.push('_');
            }
            output.push(character.to_ascii_lowercase());
        } else if character.is_ascii_alphanumeric() {
            output.push(character.to_ascii_lowercase());
        } else {
            output.push('_');
        }
    }
    identifier_text(&output)
}

pub fn type_name_text(value: &str) -> String {
    let value = to_snake_case(value);
    let mut output = String::new();
    for part in value.split('_') {
        let mut characters = part.chars();
        if let Some(first) = characters.next() {
            output.push(first.to_ascii_uppercase());
            for character in characters {
                output.push(character.to_ascii_lowercase());
            }
        }
    }
    if output.is_empty() {
        "Value".to_owned()
    } else if output
        .chars()
        .next()
        .is_some_and(|character| character.is_ascii_digit())
    {
        format!("Value{output}")
    } else {
        output
    }
}

fn identifier_text(value: &str) -> String {
    let mut output = String::new();
    for character in value.chars() {
        if character.is_ascii_alphanumeric() || character == '_' {
            output.push(character.to_ascii_lowercase());
        } else {
            output.push('_');
        }
    }
    while output.contains("__") {
        output = output.replace("__", "_");
    }
    let output = output.trim_matches('_').to_owned();
    let starts_with_digit = output
        .chars()
        .next()
        .is_some_and(|character| character.is_ascii_digit());
    if output.is_empty() {
        "value".to_owned()
    } else if starts_with_digit {
        format!("_{output}")
    } else {
        output
    }
}
