//! Server-side syntax highlighting for the human view page.
//!
//! syntect emits spans with inline colors. It escapes the source and keeps
//! the text exact, so `pre.textContent` and the Copy button stay correct.
//! The theme uses the page palette, and the page supplies the code
//! background.

use std::path::Path;
use std::str::FromStr;
use std::sync::OnceLock;

use syntect::easy::HighlightLines;
use syntect::highlighting::{
    Color, ScopeSelectors, StyleModifier, Theme, ThemeItem, ThemeSettings,
};
use syntect::html::{append_highlighted_html_for_styled_line, IncludeBackground};
use syntect::parsing::{SyntaxReference, SyntaxSet};
use syntect::util::LinesWithEndings;

/// Maximum text size that the view page highlights.
pub const MAX_HIGHLIGHT_BYTES: usize = 256 * 1024;

/// Maximum line count that the view page highlights.
pub const MAX_HIGHLIGHT_LINES: usize = 5_000;

/// How the view page picks a syntax for one request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Choice {
    /// Use the filename extension.
    Auto,
    /// Do not highlight.
    None,
    /// Use the named syntax, extension, or token.
    Language(String),
}

fn syntax_set() -> &'static SyntaxSet {
    static SET: OnceLock<SyntaxSet> = OnceLock::new();
    SET.get_or_init(SyntaxSet::load_defaults_newlines)
}

/// Build one RGB color from a 24-bit value.
const fn rgb(hex: u32) -> Color {
    Color {
        r: ((hex >> 16) & 0xFF) as u8,
        g: ((hex >> 8) & 0xFF) as u8,
        b: (hex & 0xFF) as u8,
        a: 0xFF,
    }
}

fn rule(scope: &str, hex: u32) -> ThemeItem {
    ThemeItem {
        scope: ScopeSelectors::from_str(scope).expect("the built-in theme scope must parse"),
        style: StyleModifier {
            foreground: Some(rgb(hex)),
            background: None,
            font_style: None,
        },
    }
}

fn theme() -> &'static Theme {
    static THEME: OnceLock<Theme> = OnceLock::new();
    THEME.get_or_init(|| Theme {
        name: Some("cyberpunkNeon".to_string()),
        author: Some("dayfly".to_string()),
        settings: ThemeSettings {
            foreground: Some(rgb(0xe3e8fc)),
            background: Some(rgb(0x133e7c)),
            ..ThemeSettings::default()
        },
        scopes: vec![
            rule("comment", 0x9aa9c4),
            rule("string", 0x0abdc6),
            rule("constant.numeric", 0xffb86c),
            rule("constant.language, constant.character.escape", 0xea00d9),
            rule("keyword, storage", 0xea00d9),
            rule(
                "entity.name.function, support.function, meta.function-call",
                0xe5c07b,
            ),
            rule(
                "entity.name.type, support.type, storage.type, entity.other.inherited-class",
                0x0abdc6,
            ),
            rule("variable, variable.parameter", 0xe3e8fc),
            rule("invalid, markup.deleted", 0xff6b6b),
            rule("markup.inserted", 0x0abdc6),
            rule("markup.heading", 0xea00d9),
        ],
    })
}

/// Resolve a syntax choice for one paste.
///
/// `Auto` uses the filename extension. `Language` accepts a syntax name,
/// extension, or token. An unknown language returns the error message for
/// the 400 page.
pub fn resolve(
    choice: &Choice,
    filename: Option<&str>,
) -> Result<Option<&'static SyntaxReference>, String> {
    match choice {
        Choice::None => Ok(None),
        Choice::Auto => Ok(filename
            .and_then(|name| Path::new(name).extension())
            .and_then(|extension| extension.to_str())
            .and_then(|extension| syntax_set().find_syntax_by_extension(extension))),
        Choice::Language(name) => match syntax_set().find_syntax_by_token(name) {
            Some(syntax) => Ok(Some(syntax)),
            None => Err(format!(
                "invalid syntax: unknown language \"{name}\"; expected auto, none, or a \
                 language name such as rust, python, or json"
            )),
        },
    }
}

/// Render a text paste as highlighted HTML.
///
/// Returns `None` when the text is over a cap or the highlighter fails.
/// The caller then renders the escaped text.
pub fn highlight_pre(text: &str, syntax: &SyntaxReference, line_numbers: bool) -> Option<String> {
    if text.len() > MAX_HIGHLIGHT_BYTES {
        return None;
    }
    let lines = text.bytes().filter(|byte| *byte == b'\n').count() + usize::from(!text.is_empty());
    if lines > MAX_HIGHLIGHT_LINES {
        return None;
    }

    let mut highlighter = HighlightLines::new(syntax, theme());
    let mut out = String::with_capacity(text.len().saturating_mul(2));
    out.push_str(if line_numbers {
        "<pre class=\"numbered\">"
    } else {
        "<pre class=\"syntax\">"
    });
    for line in LinesWithEndings::from(text) {
        let ranges = highlighter.highlight_line(line, syntax_set()).ok()?;
        if line_numbers {
            out.push_str("<span class=\"line\"><span class=\"ln\" aria-hidden=\"true\"></span>");
        } else {
            out.push_str("<span class=\"line\">");
        }
        append_highlighted_html_for_styled_line(&ranges, IncludeBackground::No, &mut out).ok()?;
        out.push_str("</span>");
    }
    out.push_str("</pre>");
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Return the text content of the rendered `<pre>` element.
    fn text_content(html: &str) -> String {
        let start = html.find('>').expect("the pre tag must close") + 1;
        let end = html.rfind("</pre>").expect("the pre must close");
        let mut text = String::new();
        let mut in_tag = false;
        for ch in html[start..end].chars() {
            match ch {
                '<' => in_tag = true,
                '>' => in_tag = false,
                _ if !in_tag => text.push(ch),
                _ => {}
            }
        }
        text.replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&quot;", "\"")
            .replace("&#39;", "'")
            .replace("&amp;", "&")
    }

    fn rust() -> &'static SyntaxReference {
        resolve(&Choice::Language("rust".to_string()), None)
            .expect("the choice must resolve")
            .expect("Rust must exist")
    }

    #[test]
    fn highlight_keeps_the_source_text_exact() {
        let text = "fn main() {\n\tlet s = \"a < b & c > d\";\n}\n";
        for line_numbers in [false, true] {
            let html = highlight_pre(text, rust(), line_numbers).expect("the text must highlight");
            assert!(
                html.contains("style=\"color:"),
                "the output must carry token colors"
            );
            assert_eq!(
                text_content(&html),
                text,
                "textContent must equal the paste text, line_numbers={line_numbers}"
            );
        }
    }

    #[test]
    fn resolve_uses_the_filename_extension_for_auto() {
        let syntax = resolve(&Choice::Auto, Some("main.rs"))
            .expect("auto must resolve")
            .expect("a .rs file must find a syntax");
        assert!(syntax.file_extensions.iter().any(|ext| ext == "rs"));
        assert!(
            resolve(&Choice::Auto, Some("no-extension"))
                .expect("auto must resolve")
                .is_none(),
            "a name without an extension must stay plain"
        );
    }

    #[test]
    fn resolve_rejects_an_unknown_language() {
        let error = resolve(
            &Choice::Language("definitely-not-a-language".to_string()),
            None,
        )
        .expect_err("an unknown language must fail");
        assert!(error.contains("invalid syntax"));
        assert!(error.contains("auto"));
    }

    #[test]
    fn highlight_skips_text_over_the_caps() {
        let large = "a".repeat(MAX_HIGHLIGHT_BYTES + 1);
        assert!(
            highlight_pre(&large, rust(), false).is_none(),
            "the byte cap must skip highlighting"
        );
        let many = "x\n".repeat(MAX_HIGHLIGHT_LINES + 1);
        assert!(
            highlight_pre(&many, rust(), false).is_none(),
            "the line cap must skip highlighting"
        );
    }

    #[test]
    fn theme_uses_the_page_palette() {
        let theme = theme();
        assert_eq!(theme.name.as_deref(), Some("cyberpunkNeon"));
        assert!(
            theme
                .scopes
                .iter()
                .any(|item| item.style.foreground == Some(rgb(0xea00d9))),
            "the theme must use the page accent color"
        );
    }
}
