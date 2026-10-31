pub(crate) fn literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

pub(crate) fn identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

pub(crate) fn pipeline_name(value: &str, qualified: bool) -> crate::Result<&str> {
    let valid = value.split('.').all(|part| {
        let mut chars = part.chars();
        matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
            && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
    });
    if !valid || (!qualified && value.contains('.')) {
        return Err(crate::Error::Query(
            "pipeline names and columns must be simple SQL identifiers".into(),
        ));
    }
    Ok(value)
}
