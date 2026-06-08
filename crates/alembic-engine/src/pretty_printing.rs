use std::fmt::Display;

pub(crate) fn comma_separated(elems: &[impl Display]) -> String {
    separated(elems, ", ")
}

pub(crate) fn bullet_list(elems: &[impl Display]) -> String {
    format!("- {}", separated(elems, "\n- "))
}

pub(crate) fn separated(elems: &[impl Display], separator: &str) -> String {
    elems
        .iter()
        .map(|e| e.to_string())
        .collect::<Vec<_>>()
        .join(separator)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_comma_separated() {
        assert_eq!(comma_separated(&[1, 2, 3]), "1, 2, 3");
    }

    #[test]
    fn test_bullet_list() {
        assert_eq!(bullet_list(&[1, 2, 3]), "- 1\n- 2\n- 3");
    }
}
