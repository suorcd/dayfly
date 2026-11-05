//! Simple human web frontend, served by the same binary in HTTP mode.
//!
//! Server-rendered HTML with a small vanilla-JS drag-and-drop
//! enhancement; no JavaScript framework and no frontend build step.
//! Two pages, plus a view page that falls out naturally:
//!
//! * `GET /` — combined new-paste form: drop/choose a file or paste
//!   text, with a TTL selector.
//! * `GET /pastes` — current pastes for a date, from the bucket scan
//!   (no database), with a delete action per row.
//! * `GET /view/<id>` — one paste: text in `<pre>`, image inline,
//!   everything else as a download link.
//!
//! Theme: adapted base look from the user's own site stylesheet,
//! https://suorcd.us/style.css (verified live as the site's single
//! stylesheet), palette `cyberpunkNeon` — the live look. Only the
//! relevant base rules are adapted here; the whole stylesheet is NOT
//! copied, and the file's other two palettes (`green-dark`,
//! `neon-dark01`) are deliberately NOT used. Blog-specific rules
//! (post lists, hero scanline animation, code tabs, tag pills) are
//! omitted unless they directly serve a pastebin page.
//!
//! Fonts: the site's stack is JetBrainsMono Nerd Font / JetBrains
//! Mono / Hack / Fira Code for body and Source Code Pro / Hack for
//! code. Those fonts are NOT bundled here (licensing for this use
//! was not established); the stack falls back to system monospace on
//! machines without them. Flagged in README.
//!
//! Auth: these pages are under the same model as the API. When
//! `DAYFLY_HTTP_BEARER_TOKEN` is set, the API check applies to
//! these routes too, which plain browser forms cannot satisfy (a
//! form cannot send an `Authorization` header). For browser use,
//! run on a private network / localhost without the token set, and
//! use the token for API clients. See README. No public sharing,
//! accounts, or comments are added.

use std::cmp::Ordering;
use std::collections::HashMap;

use axum::extract::{DefaultBodyLimit, Multipart, Path, Query, State};
use axum::http::header::{CACHE_CONTROL, CONTENT_LENGTH, CONTENT_TYPE, HOST, ORIGIN, REFERER};
use axum::http::uri::{Authority, Uri};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::middleware::{map_response, Next};
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use axum::{Form, Router};
use chrono::{DateTime, Utc};
use serde::Deserialize;

use crate::config::Config;
use crate::http::{
    check_auth, check_list_access, check_paste_control, public_base, read_metadata_field,
    status_for, AppState, ListAccessError, MultipartFieldError, MULTIPART_METADATA_MAX_BYTES,
};
use crate::store::{
    default_list_window, parse_list_window_request, ListWindowRequest, PasteSummary, Store,
    StoreError, StoredPaste, Ttl,
};

/// Adapted base look from suorcd.us/style.css, palette cyberpunkNeon
/// (background #091833, text #e3e8fc, accent #ea00d9, code background
/// #133e7c, border #0abdc6). Dark-only, no light-mode switch in v1.
const CSS: &str = r#"
body{font-family:"JetBrainsMono Nerd Font","JetBrains Mono",Hack,"Fira Code",ui-monospace,Menlo,Consolas,monospace;line-height:1.6;color:#e3e8fc;background:#091833;max-width:1200px;margin:0 auto;padding:20px}
header{padding-bottom:1rem;background:linear-gradient(to bottom,transparent 96%,transparent 100%)}
header::after{content:"";display:block;height:2px;margin-top:1rem;background:linear-gradient(to right,#ea00d9,transparent)}
nav{display:flex;gap:1rem;flex-wrap:wrap;align-items:center}
nav .prompt{color:#0abdc6;opacity:.6}
nav a{padding:.25rem .5rem;border:1px dashed transparent;border-radius:4px}
nav a:hover{border-color:#0abdc6;box-shadow:0 0 10px rgba(10,189,198,.35)}
nav a.active{color:#0abdc6;text-decoration:underline;text-underline-offset:.3rem;border-color:#0abdc6;box-shadow:0 0 10px rgba(10,189,198,.35)}
h1{font-size:1.8rem;text-transform:lowercase;color:#ea00d9;text-shadow:0 0 12px rgba(234,0,217,.55)}
h2{font-size:1.3rem;text-transform:lowercase;color:#ea00d9;border-bottom:1px dotted #0abdc6;padding-bottom:.25rem}
hr{border:none;border-top:1px dashed #0abdc6;margin:1.5rem 0;opacity:.6}
a{color:#ea00d9;text-decoration:none}
a:hover{text-decoration:underline;text-shadow:0 0 8px rgba(234,0,217,.8)}
table{border-collapse:collapse;width:auto;table-layout:fixed} th,td{text-align:left;padding:.35rem .4rem;border:1px dashed #0abdc6;vertical-align:top}
.col-link{width:31ch}
.col-filename{width:28ch}
.col-content-type{width:9rem}
.col-size{width:11ch}
.col-created{width:22ch}
.col-ttl{width:5ch}
.col-action{width:12ch}
.content-type-cell{display:block;max-width:calc(8rem - 1ch);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.size-cell{text-align:right;white-space:nowrap}
.size-unit{display:inline-block;min-width:3ch;text-align:left;margin-left:.4ch}
th:nth-child(4){text-align:center}
.ttl-header{cursor:help;text-decoration:underline dotted;text-underline-offset:.25rem}
.hint{color:#0abdc6;cursor:help;text-decoration:underline dotted;text-underline-offset:.25rem;font-size:.95rem}
.view-bar{display:grid;grid-template-columns:1fr auto;align-items:center;gap:.5rem;margin-bottom:1rem}
.view-actions{display:flex;flex-wrap:wrap;gap:.5rem}
.metadata-toggle{position:absolute;opacity:0;width:1px;height:1px}
.metadata-label{grid-column:2;text-align:right;cursor:pointer;color:#0abdc6}
.metadata-label::before{content:"▶ "}
.metadata-toggle:checked ~ .metadata-label::before{content:"▼ "}
.metadata-toggle:focus-visible ~ .metadata-label{outline:1px dashed #0abdc6;outline-offset:.2rem}
.metadata-panel{grid-column:1/-1;display:none;margin-top:.5rem;padding:1rem;border:1px dashed #0abdc6;border-radius:8px;background:rgba(19,62,124,.35)}
.metadata-panel form{background:none;border:0;padding:0;margin:0;border-radius:0;backdrop-filter:none}
.not-found{text-align:center;margin:2rem 0}
.not-found-code{font-size:5rem;line-height:1.1;margin-bottom:.5rem;color:#0abdc6;text-shadow:0 0 15px rgba(10,189,198,.55)}
.not-found h2{border-bottom:none;margin-top:0}
.not-found-log{max-width:650px;margin:1.5rem auto 2rem;text-align:left;font-size:.95rem}
.nf-user{color:#ea00d9}
.nf-error{color:#ffb86c}
.nf-muted{opacity:.7}
.nf-warn{color:#e5c07b}
.nf-cursor{animation:nf-blink 1s step-end infinite}
@keyframes nf-blink{0%,100%{opacity:1}50%{opacity:0}}
.metadata-toggle:checked ~ .metadata-panel{display:block}
.content-box{position:relative}
.content-box pre{margin:0;padding-right:6rem}
.copy-content{position:absolute;top:.5rem;right:.5rem}
pre.numbered{counter-reset:line;white-space:pre;overflow-x:auto}
pre.numbered .line{counter-increment:line}
pre.numbered .ln{user-select:none}
pre.numbered .ln::before{content:counter(line);display:inline-block;min-width:3ch;margin-right:1ch;text-align:right;opacity:.5}
a.button{display:inline-block;font-size:1rem;padding:.35rem .8rem;cursor:pointer;background:rgba(19,62,124,.5);color:#e3e8fc;border:1px dashed #0abdc6;border-radius:4px;text-decoration:none}
a.button:hover{text-decoration:none;box-shadow:0 0 10px rgba(10,189,198,.4)}
.table-wrap{overflow-x:auto}
.list-panel{width:max-content;max-width:100%}
.list-panel form{margin-bottom:1rem}
.filename-cell{white-space:nowrap}
th:first-child,td:first-child{white-space:nowrap}
td:first-child{overflow:hidden;text-overflow:ellipsis}
th:nth-child(4),td:nth-child(4){white-space:nowrap}
th:nth-child(5),td:nth-child(5){white-space:nowrap}
th:nth-child(6),td:nth-child(6){text-align:center;white-space:nowrap}
th:nth-child(7),td:nth-child(7){text-align:center}
th{color:#ea00d9}
.card,form{background:rgba(19,62,124,.35);border:1px dashed #0abdc6;border-radius:8px;padding:1rem;backdrop-filter:blur(4px)}
details{background:rgba(19,62,124,.35);border:1px dashed #0abdc6;border-radius:8px;padding:.5rem 1rem;margin-bottom:1rem}
summary{cursor:pointer;color:#0abdc6}
td form{display:inline;background:none;border:0;padding:0;border-radius:0;backdrop-filter:none}
details form{background:none;border:0;padding:0;border-radius:0;backdrop-filter:none;margin:0}
.dropzone{display:block;border:2px dashed #0abdc6;border-radius:8px;padding:1.5rem;margin-bottom:.5rem;text-align:center;cursor:pointer;background:rgba(19,62,124,.2)}
.dropzone:hover,.dropzone.dragover,.dropzone:focus-within{border-color:#ea00d9;box-shadow:0 0 12px rgba(234,0,217,.35)}
.dropzone input[type=file]{position:absolute;width:1px;height:1px;padding:0;margin:-1px;overflow:hidden;clip:rect(0,0,0,0);white-space:nowrap;border:0}
.dropzone-title{display:block;font-weight:bold;color:#ea00d9}
.dropzone .muted{display:block}
.dropzone .error{color:#ff6b6b;border-color:#ff6b6b}
.dropzone.compact{display:inline-flex;flex-direction:column;align-items:flex-start;gap:.35rem;width:auto;max-width:100%;margin:0;padding:.45rem .8rem;border-width:1px;text-align:left;font-size:.95rem}
.dropzone-line{display:inline-flex;flex-wrap:wrap;align-items:baseline;gap:.4rem}
.badge{display:inline-block;max-width:100%;font-size:.8rem;line-height:1.4;padding:.05rem .5rem;border:1px solid #0abdc6;border-radius:999px;color:#0abdc6;background:rgba(10,189,198,.12);overflow-wrap:anywhere}
.form-actions{display:flex;flex-wrap:wrap;gap:.75rem;align-items:stretch;margin-top:1rem}
details.cli-hint{margin-top:1.5rem}
form > p:first-child{margin-top:0}
form > p:last-child{margin-bottom:0}
.form-row{display:flex;flex-wrap:wrap;align-items:center;gap:.5rem 1.25rem}
.form-row p{margin:0}
.form-row input{min-width:16rem;max-width:100%}
.form-actions .dropzone.compact{justify-content:center}
.form-actions button[type=button]{align-self:center}
.form-actions button[type=submit]{margin-left:auto}
textarea{width:100%;min-height:12rem;box-sizing:border-box;font-family:"Source Code Pro",Hack,ui-monospace,Menlo,Consolas,monospace;background:#133e7c;color:#e3e8fc;border:1px dashed #0abdc6;border-radius:4px;padding:.5rem}
input,select{font-size:1rem;padding:.3rem .5rem;background:#133e7c;color:#e3e8fc;border:1px dashed #0abdc6;border-radius:4px}
textarea:disabled,input:disabled{opacity:.45;background:#0d2447;color:#9aa6c4;border-color:#4a5568;cursor:not-allowed}
button{font-size:1rem;padding:.35rem .8rem;cursor:pointer;background:rgba(19,62,124,.5);color:#e3e8fc;border:1px dashed #0abdc6;border-radius:4px}
button:hover{box-shadow:0 0 10px rgba(10,189,198,.4)}
pre,code{font-family:"Source Code Pro",Hack,ui-monospace,Menlo,Consolas,monospace;background:#133e7c;border:1px dashed #0abdc6;border-radius:4px}
pre{white-space:pre-wrap;word-wrap:break-word;padding:1rem;overflow-x:auto}
pre.curl-hint{white-space:pre;word-wrap:normal;background:#091833}
img{max-width:100%;height:auto;border:1px dashed #0abdc6;border-radius:8px}
footer{text-align:center;font-size:.85rem;margin-top:2rem;padding-top:1rem;border-top:1px dashed #0abdc6;color:#e3e8fc;opacity:.8}
.muted{opacity:.7} .expired{color:#ff6b6b}
@media (max-width:768px){body{padding:12px} header{flex-direction:column} nav{flex-direction:column;align-items:flex-start;gap:.5rem} nav a{font-size:.9rem}}
"#;

/// Progressive enhancement for the combined new-paste form. Without JS the
/// file input and the textarea both work; with JS, dropping a file fills the
/// input and disables the text fields so the file is what gets submitted.
const DROPZONE_SCRIPT: &str = r#"<script>
(() => {
  const zone = document.getElementById('dropzone');
  const input = document.getElementById('file');
  const status = document.getElementById('file-status');
  const clear = document.getElementById('clear-file');
  const text = document.getElementById('text');
  const filename = document.getElementById('filename');
  const form = document.querySelector('form[action="/ui/create"]');
  if (!form || !zone || !input || !status || !clear || !text || !filename) return;

  const maxBytes = Number(form.dataset.maxFileBytes) || 0;
  const humanSize = (bytes) => {
    const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
    let value = bytes;
    let unit = 0;
    while (value >= 1024 && unit < units.length - 1) {
      value /= 1024;
      unit += 1;
    }
    return (unit === 0 ? value : value.toFixed(1)) + ' ' + units[unit];
  };

  const show = () => {
    const file = input.files && input.files[0];
    if (file && maxBytes > 0 && file.size > maxBytes) {
      status.textContent = 'File is too large: ' + humanSize(file.size) + ' (limit ' + humanSize(maxBytes) + ')';
      status.classList.add('error');
      input.value = '';
      clear.hidden = true;
      text.disabled = false;
      filename.disabled = false;
      return;
    }
    status.classList.remove('error');
    if (file) {
      status.textContent = 'Selected: ' + file.name;
      clear.hidden = false;
      text.disabled = true;
      filename.disabled = true;
    } else {
      status.textContent = 'No file selected';
      clear.hidden = true;
      text.disabled = false;
      filename.disabled = false;
    }
  };

  input.addEventListener('change', show);
  clear.addEventListener('click', () => {
    input.value = '';
    show();
  });
  form.addEventListener('submit', (event) => {
    const file = input.files && input.files[0];
    if (file && maxBytes > 0 && file.size > maxBytes) {
      event.preventDefault();
      status.textContent = 'File is too large: ' + humanSize(file.size) + ' (limit ' + humanSize(maxBytes) + ')';
      status.classList.add('error');
      text.disabled = false;
      filename.disabled = false;
    }
  });

  let dragDepth = 0;
  zone.addEventListener('dragenter', (event) => {
    event.preventDefault();
    dragDepth += 1;
    zone.classList.add('dragover');
  });
  zone.addEventListener('dragover', (event) => {
    event.preventDefault();
  });
  zone.addEventListener('dragleave', (event) => {
    event.preventDefault();
    dragDepth = Math.max(0, dragDepth - 1);
    if (dragDepth === 0) zone.classList.remove('dragover');
  });
  zone.addEventListener('drop', (event) => {
    event.preventDefault();
    dragDepth = 0;
    zone.classList.remove('dragover');
    const files = event.dataTransfer && event.dataTransfer.files;
    if (!files || files.length === 0) return;
    try {
      const transfer = new DataTransfer();
      transfer.items.add(files[0]);
      input.files = transfer.files;
      show();
    } catch (error) {
      status.textContent = 'Drop is not supported here; use the file picker.';
    }
  });

  show();
})();
</script>"#;

/// Nav items in display order: `(href, label)`.
const NAV_ITEMS: [(&str, &str); 3] = [
    ("/", "new paste"),
    ("/pastes", "current pastes"),
    ("/.well-known/dayfly.json", "api description"),
];

/// Build one page. `active` names the nav href for the current page, and
/// `show_heading` controls the `<h1>`. Pages that repeat a nav label keep
/// the heading off.
/// Copy the current view URL or the paste body to the clipboard.
///
/// The copy buttons start hidden and the script unhides them, so a
/// browser without JavaScript never shows an inert control.
const COPY_LINK_SCRIPT: &str = r#"<script>
(() => {
  const copyText = async (text) => {
    try {
      await navigator.clipboard.writeText(text);
      return true;
    } catch {
      const area = document.createElement('textarea');
      area.value = text;
      area.style.position = 'fixed';
      area.style.opacity = '0';
      document.body.appendChild(area);
      area.select();
      let ok = false;
      try { ok = document.execCommand('copy'); } catch { ok = false; }
      area.remove();
      return ok;
    }
  };

  const flash = (button, label) => {
    const old = button.textContent;
    button.textContent = label;
    setTimeout(() => { button.textContent = old; }, 1500);
  };

  const link = document.querySelector('.copy-link');
  if (link) {
    link.hidden = false;
    link.addEventListener('click', async () => {
      const url = new URL(link.dataset.copy, location.origin).href;
      flash(link, (await copyText(url)) ? 'Copied' : 'Copy failed');
    });
  }

  document.querySelectorAll('.copy-content').forEach((button) => {
    button.hidden = false;
    button.addEventListener('click', async () => {
      const pre = button.parentElement.querySelector('pre');
      if (!pre) return;
      flash(button, (await copyText(pre.textContent)) ? 'Copied' : 'Copy failed');
    });
  });
})();
</script>"#;

fn page(
    footer: &Footer,
    title: &str,
    active: Option<&str>,
    show_heading: bool,
    body: &str,
) -> Html<String> {
    let title = html_escape(title);
    let nav: String = NAV_ITEMS
        .iter()
        .map(|(href, label)| nav_link(href, label, active))
        .collect();
    let heading = if show_heading {
        format!("<h1>{title}</h1>\n")
    } else {
        String::new()
    };
    let footer = footer.render();
    Html(format!(
        "<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n<title>{title} — dayfly</title>\n<style>{CSS}</style>\n</head>\n<body>\n<header><nav><span class=\"prompt\">~/dayfly $</span>{nav}</nav></header>\n{heading}{body}\n<footer>{footer}</footer>\n</body>\n</html>\n"
    ))
}

/// Footer content derived from the active configuration.
///
/// The default is `dayfly — internal use only`. A configured site name
/// replaces that text. An optional contact and acceptable-use link follow
/// the site name.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Footer {
    site_name: Option<String>,
    contact: Option<String>,
    aup_url: Option<String>,
}

impl Footer {
    pub(crate) fn from_config(config: &Config) -> Self {
        Self {
            site_name: config.site_name.clone(),
            contact: config.contact.clone(),
            aup_url: config.aup_url.clone(),
        }
    }

    /// Render the footer as HTML.
    fn render(&self) -> String {
        let mut parts = vec![match &self.site_name {
            Some(name) => html_escape(name),
            None => "dayfly — internal use only".to_string(),
        }];
        if let Some(contact) = &self.contact {
            parts.push(contact_html(contact));
        }
        if let Some(url) = &self.aup_url {
            let url = html_escape(url);
            parts.push(format!("<a href=\"{url}\">acceptable use</a>"));
        }
        parts.push("&copy; 2026 suorcd".to_string());
        parts.join(" · ")
    }
}

/// Render a contact as a link when it has a known form.
fn contact_html(contact: &str) -> String {
    let escaped = html_escape(contact);
    if contact.starts_with("http://") || contact.starts_with("https://") {
        format!("<a href=\"{escaped}\">{escaped}</a>")
    } else if contact.contains('@') && !contact.chars().any(char::is_whitespace) {
        format!("<a href=\"mailto:{escaped}\">{escaped}</a>")
    } else {
        escaped
    }
}

fn nav_link(href: &str, label: &str, active: Option<&str>) -> String {
    if active == Some(href) {
        format!("<a href=\"{href}\" class=\"active\" aria-current=\"page\">{label}</a>")
    } else {
        format!("<a href=\"{href}\">{label}</a>")
    }
}

/// A themed 404 page for a path that matches no route.
pub(crate) fn not_found_page(footer: &Footer, path: &str) -> Html<String> {
    let path = html_escape(path);
    let body = format!(
        "<div class=\"not-found\">\
         <h1 class=\"not-found-code\">404</h1>\
         <h2>Sector not found</h2>\
         <pre class=\"not-found-log\"><span class=\"nf-user\">~/dayfly</span> $ cd {path}\n\
<span class=\"nf-error\">bash: cd: {path}: No such file or directory</span>\n\
\n\
<span class=\"nf-user\">~/dayfly</span> $ cat page.html\n\
<span class=\"nf-error\">cat: page.html: No such file or directory</span>\n\
\n\
<span class=\"nf-muted\">Initiating deep bucket scan...</span>\n\
<span class=\"nf-muted\">Scanning objects.......... [FAILED]</span>\n\
<span class=\"nf-warn\">Error 404: The requested resource has been wiped, relocated, or never existed.</span>\n\
\n\
<span class=\"nf-user\">~/dayfly</span> $ <span class=\"nf-cursor\">_</span></pre>\
         <a class=\"button\" href=\"/\">cd /</a>\
         </div>"
    );
    page(footer, "Not found", None, false, &body)
}

/// A styled 405 page for a path that does not accept the method.
pub(crate) fn method_not_allowed_page(footer: &Footer, path: &str) -> Html<String> {
    page(
        footer,
        "Method not allowed",
        None,
        true,
        &format!(
            "<p>This method is not allowed for <code>{}</code>.</p>\
             <p><a href=\"/\">Back</a></p>",
            html_escape(path)
        ),
    )
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

/// Truncate a filename in the middle and keep the extension.
///
/// A name of 25 characters or fewer stays unchanged. A longer name
/// keeps the first 15 characters, an ellipsis, and the last 10
/// characters.
fn middle_truncate_filename(name: &str) -> String {
    const MAX_CHARS: usize = 25;
    const HEAD_CHARS: usize = 15;
    const TAIL_CHARS: usize = 10;
    let chars: Vec<char> = name.chars().collect();
    if chars.len() <= MAX_CHARS {
        return name.to_string();
    }
    let head: String = chars[..HEAD_CHARS].iter().collect();
    let tail: String = chars[chars.len() - TAIL_CHARS..].iter().collect();
    format!("{head}…{tail}")
}

/// Render the filename cell: full name in the tooltip, short name on screen.
fn filename_cell(filename: Option<&str>) -> String {
    match filename {
        Some(name) => format!(
            "<td class=\"filename-cell\" title=\"{}\">{}</td>",
            html_escape(name),
            html_escape(&middle_truncate_filename(name))
        ),
        None => "<td class=\"filename-cell\">—</td>".to_string(),
    }
}

/// Shell examples for the new-paste page.
///
/// The base URL comes from the request, so the examples use the scheme
/// and host that the reader used. The bearer value stays an environment
/// placeholder: the configured token never appears in the page.
fn curl_hint(base: &str, bearer: bool) -> String {
    let auth = if bearer {
        "-H \"Authorization: Bearer $DAYFLY_HTTP_BEARER_TOKEN\" "
    } else {
        ""
    };
    let commands = format!(
        "BASE={base}\n\
         curl -sS {auth}--data-binary @note.txt \"$BASE/upload\"\n\
         cat error.log | curl -sS {auth}--data-binary @- \"$BASE/upload\"\n\
         curl -sS {auth}--data-binary @report.pdf -H \"X-Filename: report.pdf\" \"$BASE/upload\"\n\
         curl -sS {auth}-H \"Accept: application/json\" -H \"X-TTL: 1h\" --data-binary @note.txt \"$BASE/upload\""
    );
    format!(
        "<details class=\"cli-hint\"><summary>cli</summary>\n\
         <pre class=\"curl-hint\">{}</pre>\n\
         </details>\n",
        html_escape(&commands)
    )
}

/// Split a byte count into a rounded whole value and its unit.
///
/// Byte counts below 1 KiB stay exact. Larger counts round to the
/// nearest whole unit, and a rounded 1024 moves to the next unit.
fn human_size_parts(size: u64) -> (u64, &'static str) {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = size as f64;
    let mut unit = 0;
    while v >= 1024.0 && unit < UNITS.len() - 1 {
        v /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        return (size, UNITS[0]);
    }
    let mut value = (v + 0.5).floor() as u64;
    if value == 1024 && unit < UNITS.len() - 1 {
        value = 1;
        unit += 1;
    }
    (value, UNITS[unit])
}

fn human_size(size: u64) -> String {
    let (value, unit) = human_size_parts(size);
    format!("{value} {unit}")
}

fn fmt_dt(dt: Option<DateTime<Utc>>) -> String {
    dt.map(|d| d.format("%Y-%m-%d %H:%M UTC").to_string())
        .unwrap_or_else(|| "—".to_string())
}

/// Whole days remaining until expiry, zero-padded for the table.
///
/// Consistent with the API's read-time expiry check: an object past its
/// `expires_at` is expired, even if it is still physically in the bucket.
fn days_remaining(expires_at: Option<DateTime<Utc>>) -> String {
    let Some(exp) = expires_at else {
        return "—".to_string();
    };
    let now = Utc::now();
    if exp <= now {
        return "expired".to_string();
    }
    let secs = (exp - now).num_seconds();
    // Ceiling division, in stable Rust (div_ceil is still unstable).
    let days = (secs + 86_400 - 1) / 86_400;
    format!("{days:02}")
}

/// Days remaining with the unit, for the view page.
fn days_remaining_label(expires_at: Option<DateTime<Utc>>) -> String {
    let value = days_remaining(expires_at);
    match value.as_str() {
        "expired" | "—" => value,
        "01" => format!("{value} day"),
        _ => format!("{value} days"),
    }
}

fn ttl_options(selected: u32) -> String {
    let mut days = vec![1u32, 3, 7, 14, 30, 90];
    if !days.contains(&selected) {
        // The configured default can be any positive day count. Render it
        // even when it is not one of the common choices, so the browser
        // cannot silently fall back to the first option.
        days.push(selected);
        days.sort_unstable();
    }
    let mut out = String::new();
    for days in days {
        let sel = if days == selected { " selected" } else { "" };
        out.push_str(&format!(
            "<option value=\"{days}\"{sel}>{days} day{}</option>",
            if days == 1 { "" } else { "s" }
        ));
    }
    out
}

/// Return true when the request's `Origin`, or its fallback `Referer`,
/// has the same authority as the `Host` header.
///
/// The browser write routes use this check. Only absolute
/// `http://` and `https://` values are accepted. The authority
/// comparison ignores ASCII case, and a missing port uses the scheme's
/// default port.
fn same_origin_allowed(headers: &HeaderMap) -> bool {
    let Some(host) = headers
        .get(HOST)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<Authority>().ok())
    else {
        return false;
    };

    let candidate = match headers.get(ORIGIN) {
        Some(value) => match value.to_str() {
            Ok(value) => value,
            Err(_) => return false,
        },
        None => match headers.get(REFERER).and_then(|value| value.to_str().ok()) {
            Some(value) => value,
            None => return false,
        },
    };

    let Ok(uri) = candidate.parse::<Uri>() else {
        return false;
    };
    let Some(scheme) = uri.scheme_str() else {
        return false;
    };
    let is_https = scheme.eq_ignore_ascii_case("https");
    if !is_https && !scheme.eq_ignore_ascii_case("http") {
        return false;
    }
    let Some(authority) = uri.authority() else {
        return false;
    };
    if authority.as_str().contains('@') {
        return false;
    }
    let default_port = if is_https { 443 } else { 80 };
    authority.host().eq_ignore_ascii_case(host.host())
        && authority.port_u16().unwrap_or(default_port) == host.port_u16().unwrap_or(default_port)
}

/// Allow API writes from non-browser clients, but reject a browser request
/// whose `Origin` or `Referer` does not match `Host`.
///
/// A request with neither header has no browser origin information, so curl
/// and other non-browser clients keep working. A browser form normally sends
/// at least one of the two headers.
pub(crate) fn api_write_allowed(headers: &HeaderMap) -> bool {
    if headers.get(ORIGIN).is_none() && headers.get(REFERER).is_none() {
        return true;
    }
    same_origin_allowed(headers)
}

fn forbidden_page() -> Response {
    (
        StatusCode::FORBIDDEN,
        Html("<p>Forbidden: the request origin does not match the Host header.</p>".to_string()),
    )
        .into_response()
}

async fn index_handler(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if let Err(resp) = check_auth(&state, &headers) {
        return resp;
    }
    let store = state.store();
    let footer = Footer::from_config(&store.config);
    let default_ttl = store.config.default_ttl_days;
    let max_file_bytes = store.config.max_file_paste_bytes();
    let max_file_human = human_size(max_file_bytes);
    let mut body = format!(
        r#"<form method="post" action="/ui/create" enctype="multipart/form-data" data-max-file-bytes="{max_file_bytes}">
<p><label>Text<br><textarea name="text" id="text"></textarea></label></p>
<div class="form-row">
<p><label>Keep for <select name="ttl_days">{}</select></label> <span class="hint" title="The selector uses whole days. API clients can send a sub-day TTL with X-TTL. Expiry is checked on read; the bucket sweeper is day-rounded. The bucket itself is private; only this binary talks to Garage.">i</span></p>
<p><label>Filename (optional, text only) <input type="text" name="filename" id="filename"></label></p>
</div>
<p class="muted">Max file size: {max_file_human}. A selected file takes precedence and disables the text fields.</p>
<div class="form-actions">
<label class="dropzone compact" id="dropzone">
<input type="file" name="file" id="file" aria-label="Choose a file">
<span class="dropzone-line"><span class="dropzone-title">Drop a file</span><span class="muted">or click to choose one</span></span>
<span class="badge" id="file-status" aria-live="polite" aria-atomic="true">No file selected</span>
</label>
<button type="button" id="clear-file" hidden>Remove file</button>
<button type="submit">Create paste</button>
</div>
</form>"#,
        ttl_options(default_ttl),
    );
    body.push_str(DROPZONE_SCRIPT);
    if let Some(base) = public_base(&headers) {
        body.push_str(&curl_hint(&base, store.config.http_bearer_token.is_some()));
    }
    page(&footer, "New paste", Some("/"), false, &body).into_response()
}

#[derive(Debug, Deserialize)]
struct TextForm {
    text: String,
    filename: Option<String>,
    ttl_days: Option<u32>,
}

async fn text_submit_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Form(form): Form<TextForm>,
) -> Response {
    if let Err(resp) = check_auth(&state, &headers) {
        return resp;
    }
    if !same_origin_allowed(&headers) {
        return forbidden_page();
    }
    let filename = form.filename.filter(|f| !f.trim().is_empty());
    let store = state.store();
    let footer = Footer::from_config(&store.config);
    let ttl = match form.ttl_days {
        Some(days) => match Ttl::checked_days(days) {
            Ok(ttl) => Some(ttl),
            Err(e) => return bad_request_page(&footer, &e.to_string()),
        },
        None => None,
    };
    match store
        .store_text(form.text.as_bytes(), filename.as_deref(), None, ttl)
        .await
    {
        Ok(stored) => {
            Redirect::to(&stored_view_target(&stored, store.config.show_words)).into_response()
        }
        Err(e) => error_page(&footer, e),
    }
}

fn error_page(footer: &Footer, e: StoreError) -> Response {
    let status = status_for(&e);
    let msg = match &e {
        StoreError::TooLarge { limit } => {
            format!("payload too large: limit is {}", human_size(*limit))
        }
        _ => e.to_string(),
    };
    (
        status,
        page(
            footer,
            "Error",
            None,
            true,
            &format!("<p>{}</p><p><a href=\"/\">Back</a></p>", html_escape(&msg)),
        ),
    )
        .into_response()
}

/// A clear 401 page instead of an empty table that looks like "no pastes".
fn unauthorized_page(footer: &Footer, message: &str) -> Response {
    (
        StatusCode::UNAUTHORIZED,
        page(
            footer,
            "Unauthorized",
            None,
            true,
            &format!(
                "<p>{}</p><p><a href=\"/\">Back</a></p>",
                html_escape(message)
            ),
        ),
    )
        .into_response()
}

/// A malformed multipart body is a client error, not a missing field.
fn bad_request_page(footer: &Footer, message: &str) -> Response {
    (
        StatusCode::BAD_REQUEST,
        page(
            footer,
            "Error",
            None,
            true,
            &format!(
                "<p>{}</p><p><a href=\"/\">Back</a></p>",
                html_escape(message)
            ),
        ),
    )
        .into_response()
}

/// Pick the word alias when the human UI is configured to show words,
/// otherwise the canonical ID. Legacy objects without an alias fall back to
/// the ID either way.
fn stored_view_target(stored: &StoredPaste, show_words: bool) -> String {
    let name = if show_words {
        stored.words.as_deref().unwrap_or(&stored.id)
    } else {
        &stored.id
    };
    format!("/view/{name}")
}

/// Reject an obviously oversized multipart request from its Content-Length
/// before the handler reads the body. The multipart envelope and text field
/// add overhead, so this is a coarse upper bound; the streaming cap remains
/// authoritative.
fn multipart_length_allows(store: &Store, headers: &HeaderMap) -> bool {
    let Some(length) = headers
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
    else {
        return true;
    };
    let max_request = store
        .config
        .max_file_paste_bytes()
        .saturating_add(store.config.max_text_paste_bytes())
        .saturating_add(1024 * 1024);
    length <= max_request
}

/// Stream one multipart file part into the store. The caller decides the
/// error page; this keeps the upload and combined-create handlers on one
/// streaming path.
async fn store_file_field(
    store: &Store,
    field: axum::extract::multipart::Field<'_>,
    ttl: Option<Ttl>,
) -> Result<StoredPaste, StoreError> {
    let field_content_type = field.content_type().map(|s| s.to_string());
    let filename = field.file_name().map(|s| s.to_string());
    let max_bytes = store.config.max_bytes_for_upload(filename.as_deref());
    let stream = futures::stream::unfold(field, |mut f| async move {
        match f.chunk().await {
            Ok(Some(chunk)) => Some((Ok(chunk), f)),
            Ok(None) => None,
            Err(e) => Some((Err(StoreError::Io(e.to_string())), f)),
        }
    });
    store
        .store_stream(
            Box::pin(stream),
            filename.as_deref(),
            field_content_type.as_deref(),
            ttl,
            None,
            max_bytes,
        )
        .await
}

/// Read a multipart text field into memory, bounded by the same text cap as
/// the API. This is the combined form's textarea, not a small metadata field.
async fn read_text_field(
    mut field: axum::extract::multipart::Field<'_>,
    limit: usize,
) -> Result<Vec<u8>, MultipartFieldError> {
    let mut bytes = Vec::new();
    while let Some(chunk) = field
        .chunk()
        .await
        .map_err(|e| MultipartFieldError::Malformed(e.to_string()))?
    {
        if bytes.len().saturating_add(chunk.len()) > limit {
            return Err(MultipartFieldError::TooLarge);
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

async fn upload_submit_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    mut multipart: Multipart,
) -> Response {
    if let Err(resp) = check_auth(&state, &headers) {
        return resp;
    }
    if !same_origin_allowed(&headers) {
        return forbidden_page();
    }
    let store = state.store();
    let footer = Footer::from_config(&store.config);
    if !multipart_length_allows(&store, &headers) {
        return error_page(
            &footer,
            StoreError::TooLarge {
                limit: store.config.max_file_paste_bytes(),
            },
        );
    }
    let mut ttl: Option<Ttl> = None;
    // Stream the file field inside the loop, not moved out of it: an
    // axum Field borrows its Multipart, so holding it across a
    // next_field call does not compile (see http.rs for the same note).
    loop {
        let field = match multipart.next_field().await {
            Ok(Some(field)) => field,
            Ok(None) => break,
            Err(e) => return bad_request_page(&footer, &format!("invalid multipart body: {e}")),
        };
        let name = field.name().unwrap_or("").to_string();
        if name == "file" {
            return match store_file_field(&store, field, ttl).await {
                Ok(stored) => Redirect::to(&stored_view_target(&stored, store.config.show_words))
                    .into_response(),
                Err(e) => error_page(&footer, e),
            };
        }
        if name == "ttl_days" {
            match read_metadata_field(field).await {
                Ok(text) => match Ttl::parse_days(&text) {
                    Ok(parsed) => ttl = Some(parsed),
                    Err(e) => return bad_request_page(&footer, &e.to_string()),
                },
                Err(MultipartFieldError::TooLarge) => {
                    return error_page(
                        &footer,
                        StoreError::TooLarge {
                            limit: MULTIPART_METADATA_MAX_BYTES as u64,
                        },
                    );
                }
                Err(MultipartFieldError::Malformed(message)) => {
                    return bad_request_page(
                        &footer,
                        &format!("invalid multipart body: {message}"),
                    );
                }
            }
        }
    }
    error_page(
        &footer,
        StoreError::InvalidId("upload requires a 'file' field".into()),
    )
}

/// Combined browser create route: one multipart form carries the TTL, an
/// optional file, and an optional text body. A selected file wins; otherwise
/// the text is stored with the same cap as the API.
async fn create_submit_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    mut multipart: Multipart,
) -> Response {
    if let Err(resp) = check_auth(&state, &headers) {
        return resp;
    }
    if !same_origin_allowed(&headers) {
        return forbidden_page();
    }
    let store = state.store();
    let footer = Footer::from_config(&store.config);
    if !multipart_length_allows(&store, &headers) {
        return error_page(
            &footer,
            StoreError::TooLarge {
                limit: store.config.max_file_paste_bytes(),
            },
        );
    }

    let mut ttl: Option<Ttl> = None;
    let mut text: Option<Vec<u8>> = None;
    let mut text_filename: Option<String> = None;
    let text_limit = usize::try_from(store.config.max_text_paste_bytes()).unwrap_or(usize::MAX);

    loop {
        let field = match multipart.next_field().await {
            Ok(Some(field)) => field,
            Ok(None) => break,
            Err(e) => return bad_request_page(&footer, &format!("invalid multipart body: {e}")),
        };
        let name = field.name().unwrap_or("").to_string();
        match name.as_str() {
            "ttl_days" => match read_metadata_field(field).await {
                Ok(value) => match Ttl::parse_days(&value) {
                    Ok(parsed) => ttl = Some(parsed),
                    Err(e) => return bad_request_page(&footer, &e.to_string()),
                },
                Err(MultipartFieldError::TooLarge) => {
                    return error_page(
                        &footer,
                        StoreError::TooLarge {
                            limit: MULTIPART_METADATA_MAX_BYTES as u64,
                        },
                    );
                }
                Err(MultipartFieldError::Malformed(message)) => {
                    return bad_request_page(
                        &footer,
                        &format!("invalid multipart body: {message}"),
                    );
                }
            },
            "filename" => match read_metadata_field(field).await {
                Ok(value) => {
                    text_filename = if value.trim().is_empty() {
                        None
                    } else {
                        Some(value)
                    };
                }
                Err(MultipartFieldError::TooLarge) => {
                    return error_page(
                        &footer,
                        StoreError::TooLarge {
                            limit: MULTIPART_METADATA_MAX_BYTES as u64,
                        },
                    );
                }
                Err(MultipartFieldError::Malformed(message)) => {
                    return bad_request_page(
                        &footer,
                        &format!("invalid multipart body: {message}"),
                    );
                }
            },
            "text" => match read_text_field(field, text_limit).await {
                Ok(bytes) => text = Some(bytes),
                Err(MultipartFieldError::TooLarge) => {
                    return error_page(
                        &footer,
                        StoreError::TooLarge {
                            limit: store.config.max_text_paste_bytes(),
                        },
                    );
                }
                Err(MultipartFieldError::Malformed(message)) => {
                    return bad_request_page(
                        &footer,
                        &format!("invalid multipart body: {message}"),
                    );
                }
            },
            "file" => {
                // An empty file input may arrive as a part with no filename.
                // Treat that as "no file" so a text paste still works.
                if !field.file_name().is_some_and(|name| !name.is_empty()) {
                    continue;
                }
                return match store_file_field(&store, field, ttl).await {
                    Ok(stored) => {
                        Redirect::to(&stored_view_target(&stored, store.config.show_words))
                            .into_response()
                    }
                    Err(e) => error_page(&footer, e),
                };
            }
            _ => {}
        }
    }

    let Some(bytes) = text else {
        return bad_request_page(&footer, "provide text or a file");
    };
    if bytes.is_empty() {
        return bad_request_page(&footer, "provide text or a file");
    }
    match store
        .store_text(&bytes, text_filename.as_deref(), None, ttl)
        .await
    {
        Ok(stored) => {
            Redirect::to(&stored_view_target(&stored, store.config.show_words)).into_response()
        }
        Err(e) => error_page(&footer, e),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PasteSort {
    Size,
    Created,
    Expires,
}

impl PasteSort {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "size" => Some(Self::Size),
            "created" => Some(Self::Created),
            "expires" => Some(Self::Expires),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Size => "size",
            Self::Created => "created",
            Self::Expires => "expires",
        }
    }

    fn default_order(self) -> SortOrder {
        match self {
            Self::Expires => SortOrder::Asc,
            Self::Size | Self::Created => SortOrder::Desc,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SortOrder {
    Asc,
    Desc,
}

impl SortOrder {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "asc" => Some(Self::Asc),
            "desc" => Some(Self::Desc),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Asc => "asc",
            Self::Desc => "desc",
        }
    }

    fn toggle(self) -> Self {
        match self {
            Self::Asc => Self::Desc,
            Self::Desc => Self::Asc,
        }
    }
}

fn parse_sort_order(query: &HashMap<String, String>) -> Result<(PasteSort, SortOrder), String> {
    let sort = match query.get("sort").map(String::as_str) {
        None => PasteSort::Created,
        Some(value) => PasteSort::parse(value)
            .ok_or_else(|| "invalid sort: expected size, created, or expires".to_string())?,
    };
    let order = match query.get("order").map(String::as_str) {
        None => sort.default_order(),
        Some(value) => SortOrder::parse(value)
            .ok_or_else(|| "invalid order: expected asc or desc".to_string())?,
    };
    Ok((sort, order))
}

/// Compare two timestamps with a direction. A missing value sorts last in
/// both directions.
fn compare_optional<T: Ord>(a: Option<T>, b: Option<T>, order: SortOrder) -> Ordering {
    match (a, b) {
        (Some(a), Some(b)) => match order {
            SortOrder::Asc => a.cmp(&b),
            SortOrder::Desc => b.cmp(&a),
        },
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

fn sort_pastes(items: &mut [PasteSummary], sort: PasteSort, order: SortOrder) {
    items.sort_by(|a, b| {
        let primary = match sort {
            PasteSort::Size => match order {
                SortOrder::Asc => a.size.cmp(&b.size),
                SortOrder::Desc => b.size.cmp(&a.size),
            },
            PasteSort::Created => compare_optional(a.created_at, b.created_at, order),
            PasteSort::Expires => compare_optional(a.expires_at, b.expires_at, order),
        };
        primary.then_with(|| a.id.cmp(&b.id))
    });
}

/// Build one sortable table header. The active column shows its direction.
/// A click toggles the active column and keeps the date filters.
fn sort_header(
    label: &str,
    key: PasteSort,
    sort: PasteSort,
    order: SortOrder,
    filters: &str,
) -> String {
    let active = sort == key;
    let next_order = if active {
        order.toggle()
    } else {
        key.default_order()
    };
    let arrow = if active {
        match order {
            SortOrder::Asc => " ▲",
            SortOrder::Desc => " ▼",
        }
    } else {
        ""
    };
    let mut href = format!(
        "/pastes?sort={}&order={}",
        key.as_str(),
        next_order.as_str()
    );
    if !filters.is_empty() {
        href.push('&');
        href.push_str(filters);
    }
    format!(
        "<a href=\"{}\">{}{}</a>",
        html_escape(&href),
        html_escape(label),
        arrow
    )
}

async fn list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let footer = Footer::from_config(&state.store().config);
    if let Err(error) = check_list_access(&state, &headers) {
        return match error {
            // A configured token already gated this page, and the shared
            // JSON auth error is what every other UI route returns.
            ListAccessError::Unauthorized(response) => response,
            // This case would otherwise have rendered an open, empty list.
            ListAccessError::MissingToken => unauthorized_page(
                &footer,
                "Listing is gated by DAYFLY_LIST_REQUIRES_TOKEN, but no bearer token \
                 is configured. Set DAYFLY_HTTP_BEARER_TOKEN, or turn the setting off.",
            ),
        };
    }
    let (sort, order) = match parse_sort_order(&query) {
        Ok(value) => value,
        Err(message) => return bad_request_page(&footer, &message),
    };
    let date = query.get("date").cloned().unwrap_or_default();
    let start = query.get("start").cloned().unwrap_or_default();
    let end = query.get("end").cloned().unwrap_or_default();
    let request = match parse_list_window_request(
        query.get("date").map(String::as_str),
        query.get("start").map(String::as_str),
        query.get("end").map(String::as_str),
    ) {
        Ok(request) => request,
        Err(e) => return error_page(&footer, e),
    };
    // list_window already filters expired objects (HeadObject per key),
    // so this page is consistent with the API's read-time expiry check.
    let store = state.store();
    let (start_value, end_value) = match &request {
        ListWindowRequest::Default => {
            let (start_date, end_date) =
                default_list_window(Utc::now(), store.config.list_default_days);
            (
                start_date.format("%Y-%m-%d").to_string(),
                end_date.format("%Y-%m-%d").to_string(),
            )
        }
        _ => (start.clone(), end.clone()),
    };
    let items = match &request {
        ListWindowRequest::Date(date) => store.list_recent(Some(date), 1000).await,
        ListWindowRequest::Range { start, end } => {
            store.list_window(Some(start), Some(end), 1000).await
        }
        ListWindowRequest::Default => store.list_window(None, None, 1000).await,
    };
    let mut items = match items {
        Ok(items) => items,
        Err(e) => return error_page(&footer, e),
    };
    sort_pastes(&mut items, sort, order);

    let mut filters = Vec::new();
    if !date.is_empty() {
        filters.push(format!("date={date}"));
    }
    if !start.is_empty() {
        filters.push(format!("start={start}"));
    }
    if !end.is_empty() {
        filters.push(format!("end={end}"));
    }
    let filters = filters.join("&");
    let size_header = sort_header("Size", PasteSort::Size, sort, order, &filters);
    let created_header = sort_header("Created", PasteSort::Created, sort, order, &filters);

    let mut rows = String::new();
    let show_words = store.config.show_words;
    let show_internal_paths = store.config.show_internal_paths;
    let link_header = if show_internal_paths {
        "Link / ID"
    } else {
        "Link"
    };
    let token_note = if store.config.paste_tokens {
        "<p class=\"muted\">Paste tokens are on. The row Delete buttons need a bearer token. \
         Open a paste and enter its token to delete it.</p>"
    } else {
        ""
    };
    for item in &items {
        let id_esc = html_escape(&item.id);
        let link_cell = match (show_words, item.words.as_deref()) {
            (true, Some(words)) => {
                let words_esc = html_escape(words);
                if show_internal_paths {
                    format!(
                        "<a href=\"/view/{words_esc}\" title=\"{words_esc}\">{words_esc}</a><br><span class=\"muted\">{id_esc}</span>"
                    )
                } else {
                    format!("<a href=\"/view/{words_esc}\" title=\"{words_esc}\">{words_esc}</a>")
                }
            }
            _ => format!("<a href=\"/view/{id_esc}\" title=\"{id_esc}\">{id_esc}</a>"),
        };
        let content_type = html_escape(item.content_type.as_deref().unwrap_or("—"));
        let filename = filename_cell(item.filename.as_deref());
        let (size_value, size_unit) = human_size_parts(item.size);
        rows.push_str(&format!(
            "<tr><td>{link_cell}</td>\
             {filename}<td><span class=\"content-type-cell\" title=\"{content_type}\">{content_type}</span></td>\
             <td class=\"size-cell\"><span class=\"size-number\">{size_value}</span><span class=\"size-unit\">{size_unit}</span></td>\
             <td>{}</td><td>{}</td>\
             <td><form method=\"post\" action=\"/ui/delete\"><input type=\"hidden\" name=\"id\" value=\"{id_esc}\"><button type=\"submit\">Delete</button></form></td></tr>\n",
            fmt_dt(item.created_at),
            days_remaining(item.expires_at),
        ));
    }
    if rows.is_empty() {
        rows.push_str(
            "<tr><td colspan=\"7\" class=\"muted\">No current pastes in this window.</td></tr>\n",
        );
    }
    page(
        &footer,
        "Current pastes",
        Some("/pastes"),
        false,
        &format!(
            r#"<div class="list-panel"><form method="get" action="/pastes"><p><label>Start <input type="date" name="start" value="{}"></label> <label>End <input type="date" name="end" value="{}"></label> <button type="submit">Show range</button> <span class="hint" title="Dates are UTC. With no dates, the page shows the last {default_days} days, ending today in UTC. Set DAYFLY_LIST_DEFAULT_DAYS to change that. Built from a direct bucket scan (ListObjectsV2 for each date prefix, plus HeadObject per paste). No database. Listing is O(n) in objects scanned and gets slower as the bucket grows — that slowdown is the trigger for adding a database later.">i</span></p></form>
{token_note}<div class="table-wrap"><table><colgroup><col class="col-link"><col class="col-filename"><col class="col-content-type"><col class="col-size"><col class="col-created"><col class="col-ttl"><col class="col-action"></colgroup><thead><tr><th>{link_header}</th><th>Filename</th><th>Content-Type</th><th>{size_header}</th><th>{created_header}</th><th class="ttl-header" title="days">TTL</th><th>Action</th></tr></thead><tbody>{}</tbody></table></div></div>"#,
            html_escape(&start_value),
            html_escape(&end_value),
            rows,
            default_days = store.config.list_default_days,
        ),
    )
    .into_response()
}

#[derive(Debug, Deserialize)]
struct DeleteForm {
    id: String,
    paste_token: Option<String>,
}

async fn delete_submit_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Form(form): Form<DeleteForm>,
) -> Response {
    if let Err(resp) =
        check_paste_control(&state, &headers, &form.id, form.paste_token.as_deref()).await
    {
        return resp;
    }
    if !same_origin_allowed(&headers) {
        return forbidden_page();
    }
    let store = state.store();
    let footer = Footer::from_config(&store.config);
    match store.delete(&form.id).await {
        Ok(()) => Redirect::to("/pastes").into_response(),
        Err(e) => error_page(&footer, e),
    }
}

/// Whether the view page can show this type as escaped text.
///
/// Markup source (HTML, SVG, XML) is text. The view escapes it, so the
/// browser never renders it as markup.
fn text_preview_type(main: &str) -> bool {
    main.starts_with("text/")
        || matches!(
            main,
            "application/json" | "application/xml" | "application/javascript" | "image/svg+xml"
        )
}

/// Maximum lines that the view page numbers.
const MAX_NUMBERED_LINES: usize = 5_000;

/// Render the text preview, optionally with presentation-only line numbers.
///
/// The generated numbers never enter the DOM text: they come from a CSS
/// counter on an empty span. `textContent` therefore equals `text`.
fn text_pre_view(text: &str, line_numbers: bool) -> String {
    if !line_numbers {
        return format!("<pre>{}</pre>", html_escape(text));
    }
    let lines = text.bytes().filter(|byte| *byte == b'\n').count() + usize::from(!text.is_empty());
    if lines > MAX_NUMBERED_LINES {
        return format!("<pre>{}</pre>", html_escape(text));
    }
    let mut out = String::from("<pre class=\"numbered\">");
    for line in text.split_inclusive('\n') {
        out.push_str("<span class=\"line\"><span class=\"ln\" aria-hidden=\"true\"></span>");
        out.push_str(&html_escape(line));
        out.push_str("</span>");
    }
    out.push_str("</pre>");
    out
}

async fn view_inner(state: &AppState, headers: &HeaderMap, id: &str) -> Response {
    if let Err(resp) = check_auth(state, headers) {
        return resp;
    }
    let store = state.store();
    let footer = Footer::from_config(&store.config);
    let meta = match store.head_metadata(id).await {
        Ok(m) => m,
        Err(e) => return error_page(&footer, e),
    };
    if meta.expired {
        return error_page(&footer, StoreError::Expired(meta.id));
    }
    let id_esc = html_escape(&meta.id);
    let main = meta.content_type.split(';').next().unwrap_or("").trim();
    let display_name = if store.config.show_words {
        meta.words.as_deref().unwrap_or(meta.id.as_str())
    } else {
        meta.id.as_str()
    };
    let target_esc = html_escape(display_name);
    let download_name = html_escape(meta.filename.as_deref().unwrap_or(display_name));
    let token_field = if store.config.paste_tokens {
        "<p><label>Paste token <input type=\"text\" name=\"paste_token\"></label></p>"
    } else {
        ""
    };
    let mut body = format!(
        "<div class=\"view-bar\">\
         <div class=\"view-actions\">\
         <a class=\"button\" href=\"/v1/pastes/{target_esc}\" download=\"{download_name}\">Download</a>\
         <a class=\"button\" href=\"/raw/{target_esc}\">Raw</a>\
         <button type=\"button\" class=\"copy-link\" data-copy=\"/view/{target_esc}\" hidden>Copy link</button>\
         </div>\
         <input type=\"checkbox\" id=\"metadata-toggle\" class=\"metadata-toggle\">\
         <label for=\"metadata-toggle\" class=\"metadata-label\">Metadata &amp; delete</label>\
         <div class=\"metadata-panel\">\
         <p>Filename: {}</p><p>Content-Type: {}</p><p>Size: {}</p><p>Created: {}</p><p>Expires: {} ({})</p>\
         <form method=\"post\" action=\"/ui/delete\"><input type=\"hidden\" name=\"id\" value=\"{id_esc}\">{token_field}<button type=\"submit\">Delete this paste</button></form>\
         </div>\
         </div>",
        html_escape(meta.filename.as_deref().unwrap_or("—")),
        html_escape(&meta.content_type),
        human_size(meta.size),
        fmt_dt(meta.created_at),
        fmt_dt(meta.expires_at),
        days_remaining_label(meta.expires_at),
    );
    if text_preview_type(main) {
        // Text view, capped at 1 MiB so the page stays usable; the API
        // remains the route for full / sliced reads of large pastes.
        let slice = store.fetch_text(&meta.id, 0, Some(1_000_000)).await;
        match slice {
            Ok(s) => {
                let pre = text_pre_view(&s.text, store.config.view_line_numbers);
                body.push_str(&format!(
                    "<hr>\
                     <div class=\"content-box\"><button type=\"button\" class=\"copy-content\" \
                     aria-label=\"Copy paste content\" hidden>Copy</button>\
                     {pre}</div>"
                ));
                if s.truncated {
                    body.push_str("<p class=\"muted\">Text truncated at 1 MiB for this view. The raw route returns the full paste.</p>");
                }
            }
            Err(e) => return error_page(&footer, e),
        }
    } else if main.starts_with("image/") && main != "image/svg+xml" {
        // Safe images inline. SVG and HTML are never rendered inline
        // here (see README safety note); they get a download link.
        body.push_str(&format!(
            "<p><img src=\"/v1/pastes/{id_esc}\" alt=\"{}\"></p>",
            html_escape(meta.filename.as_deref().unwrap_or("image"))
        ));
    }
    body.push_str(COPY_LINK_SCRIPT);
    page(&footer, display_name, None, true, &body).into_response()
}

async fn favicon_handler(State(state): State<AppState>) -> Response {
    let store = state.store();
    let Some(favicon) = &store.config.favicon else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static(favicon.content_type));
    headers.insert(
        CACHE_CONTROL,
        HeaderValue::from_static("public, max-age=86400"),
    );
    (headers, favicon.bytes.to_vec()).into_response()
}

async fn view_dated(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((date, random)): Path<(String, String)>,
) -> Response {
    view_inner(&state, &headers, &format!("{date}/{random}")).await
}

async fn view_bare(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(random): Path<String>,
) -> Response {
    view_inner(&state, &headers, &random).await
}

/// Deny framing and block MIME sniffing for every UI response.
///
/// The human pages carry an inline style and script, so a broad
/// `default-src` policy would break them. Keep the policy to framing.
async fn deny_framing(mut response: Response) -> Response {
    response.headers_mut().insert(
        HeaderName::from_static("content-security-policy"),
        HeaderValue::from_static("frame-ancestors 'none'"),
    );
    response.headers_mut().insert(
        HeaderName::from_static("x-frame-options"),
        HeaderValue::from_static("DENY"),
    );
    response.headers_mut().insert(
        HeaderName::from_static("x-content-type-options"),
        HeaderValue::from_static("nosniff"),
    );
    response
}

/// UI routes for the shared reloadable [`AppState`].
///
/// `main` merges these with the API routes and attaches the same state.
pub fn ui_router(state: AppState) -> Router<AppState> {
    // See http.rs: the store caps streams while it reads them. The 2 MiB
    // axum default would reject real file uploads early. The combined form
    // buffers only small metadata fields, so it disables the coarse limit
    // and relies on the field and stream caps.
    let form_routes = Router::new()
        .route("/ui/upload", post(upload_submit_handler))
        .route("/ui/create", post(create_submit_handler))
        .layer(DefaultBodyLimit::disable());
    // The URL-encoded text form is buffered. Set its body limit from the
    // current text cap for each request, so a reload applies without a
    // router rebuild. The decoded text still goes through the exact cap.
    let text_routes = Router::new()
        .route("/ui/text", post(text_submit_handler))
        .layer(axum::middleware::from_fn_with_state(state, text_body_limit));
    Router::new()
        .merge(form_routes)
        .merge(text_routes)
        .route("/", get(index_handler))
        .route("/favicon.ico", get(favicon_handler))
        .route("/ui/delete", post(delete_submit_handler))
        .route("/pastes", get(list_handler))
        .route("/view/{date}/{random}", get(view_dated))
        .route("/view/{random}", get(view_bare))
        .layer(map_response(deny_framing))
}

/// Enforce the current text cap on the URL-encoded form body.
///
/// The body is buffered before the form is parsed. The limit follows the
/// current configuration, so a reload affects the next request.
async fn text_body_limit(
    State(state): State<AppState>,
    mut request: axum::extract::Request,
    next: Next,
) -> Response {
    let store = state.store();
    let limit = text_form_body_limit(store.config.max_text_paste_bytes());
    DefaultBodyLimit::max(limit).apply(&mut request);
    next.run(request).await
}

/// Raw-body limit for the URL-encoded text form.
///
/// The worst case is three bytes on the wire for every decoded byte.
/// The extra margin covers the form field names, the filename, and the
/// TTL. The decoded text is still capped exactly by
/// `max_text_paste_bytes`.
fn text_form_body_limit(max_text_paste_bytes: u64) -> usize {
    let encoded = max_text_paste_bytes.saturating_mul(3);
    usize::try_from(encoded.saturating_add(8 * 1024)).unwrap_or(usize::MAX)
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::header::{CONTENT_TYPE, HOST, ORIGIN, REFERER};
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    use super::*;
    use crate::config::Config;
    use crate::store::Store;

    const TEST_TOKEN: &str = "correct-horse-battery-staple";

    fn test_config() -> Config {
        Config {
            endpoint: "http://127.0.0.1:1".to_string(),
            bucket: "test-bucket".to_string(),
            region: "us-east-1".to_string(),
            access_key_id: "test-access-key".to_string(),
            secret_access_key: "test-secret-key".to_string(),
            max_text_bytes: 10 * 1024 * 1024,
            max_file_bytes: 1024 * 1024 * 1024,
            max_paste_bytes: 1024 * 1024 * 1024,
            alias_words: 3,
            default_ttl_days: 7,
            list_default_days: 2,
            retention_by_size: false,
            retention_min_days: 30,
            retention_max_days: 365,
            http_bind: "127.0.0.1:0".to_string(),
            trusted_proxies: Vec::new(),
            show_words: false,
            show_internal_paths: false,
            inline_pdf: true,
            view_line_numbers: false,
            allow_unauthenticated: false,
            list_requires_token: false,
            paste_tokens: false,
            rate_limit_per_minute: 0,
            http_bearer_token: None,
            mcp_local_root: None,
            favicon: None,
            robots_txt: None,
            security_txt: None,
            site_name: None,
            contact: None,
            aup_url: None,
        }
    }

    async fn test_state(token: Option<&str>) -> AppState {
        let mut config = test_config();
        config.http_bearer_token = token.map(str::to_string);
        AppState::new(
            Store::from_config(config)
                .await
                .expect("test S3 config must build without network access"),
        )
    }

    async fn ui_list_body(state: AppState, auth: Option<&str>) -> (StatusCode, String) {
        let app = ui_router(state.clone()).with_state(state);
        let mut builder = Request::builder().method("GET").uri("/pastes");
        if let Some(auth) = auth {
            builder = builder.header(axum::http::header::AUTHORIZATION, auth);
        }
        let request = builder.body(Body::empty()).expect("request must build");
        let response = app
            .oneshot(request)
            .await
            .expect("router call must complete");
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("response body must be readable");
        (status, String::from_utf8_lossy(&body).into_owned())
    }

    /// Call the UI list route. A passing token reaches the unreachable test
    /// S3 endpoint, and the UI maps that S3 error to a 5xx status. A 401
    /// means the auth check rejected the request.
    async fn ui_list_status(state: AppState, auth: Option<&str>) -> StatusCode {
        ui_list_body(state, auth).await.0
    }

    #[tokio::test]
    async fn ui_wrong_bearer_token_returns_401() {
        let state = test_state(Some(TEST_TOKEN)).await;
        assert_eq!(
            ui_list_status(state, Some("Bearer wrong-horse-battery-staple")).await,
            StatusCode::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn ui_right_bearer_token_passes_auth_and_reaches_the_store() {
        let state = test_state(Some(TEST_TOKEN)).await;
        let auth = format!("Bearer {TEST_TOKEN}");
        assert_eq!(
            ui_list_status(state, Some(&auth)).await,
            StatusCode::BAD_GATEWAY,
            "the right token must pass auth and reach the S3 sink"
        );
    }

    #[tokio::test]
    async fn ui_without_configured_token_passes_auth_and_reaches_the_store() {
        let state = test_state(None).await;
        assert_eq!(
            ui_list_status(state, None).await,
            StatusCode::BAD_GATEWAY,
            "an unset token must leave the UI routes open"
        );
    }

    async fn state_with_gated_listing(token: Option<&str>) -> AppState {
        let mut config = test_config();
        config.list_requires_token = true;
        config.http_bearer_token = token.map(str::to_string);
        AppState::new(
            Store::from_config(config)
                .await
                .expect("test S3 config must build without network access"),
        )
    }

    #[tokio::test]
    async fn ui_gated_listing_without_a_configured_token_renders_401() {
        let state = state_with_gated_listing(None).await;
        let (status, body) = ui_list_body(state, None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert!(
            body.contains("DAYFLY_LIST_REQUIRES_TOKEN"),
            "the page must name the setting: {body}"
        );
        assert!(
            body.contains("DAYFLY_HTTP_BEARER_TOKEN"),
            "the page must name the missing token: {body}"
        );
        assert!(
            !body.contains("No current pastes"),
            "a refusal must not look like an empty list: {body}"
        );
    }

    #[tokio::test]
    async fn ui_gated_listing_requires_the_token() {
        let state = state_with_gated_listing(Some(TEST_TOKEN)).await;
        assert_eq!(ui_list_status(state, None).await, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn ui_gated_listing_accepts_the_token() {
        let state = state_with_gated_listing(Some(TEST_TOKEN)).await;
        let auth = format!("Bearer {TEST_TOKEN}");
        assert_eq!(
            ui_list_status(state, Some(&auth)).await,
            StatusCode::BAD_GATEWAY,
            "the right token must pass the list gate and reach the S3 sink"
        );
    }

    #[tokio::test]
    async fn ui_html_responses_deny_framing() {
        let state = test_state(None).await;
        let app = ui_router(state.clone()).with_state(state);

        for uri in ["/", "/pastes", "/view/example"] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("GET")
                        .uri(uri)
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .expect("router call must complete");
            assert_eq!(
                response
                    .headers()
                    .get("content-security-policy")
                    .and_then(|value| value.to_str().ok()),
                Some("frame-ancestors 'none'"),
                "{uri}"
            );
            assert_eq!(
                response
                    .headers()
                    .get("x-frame-options")
                    .and_then(|value| value.to_str().ok()),
                Some("DENY"),
                "{uri}"
            );
            assert_eq!(
                response
                    .headers()
                    .get("x-content-type-options")
                    .and_then(|value| value.to_str().ok()),
                Some("nosniff"),
                "{uri}"
            );
        }
    }

    fn summary(
        id: &str,
        size: u64,
        created_at: Option<DateTime<Utc>>,
        expires_at: Option<DateTime<Utc>>,
    ) -> PasteSummary {
        PasteSummary {
            id: id.to_string(),
            key: format!("pastes/{id}"),
            words: None,
            size,
            last_modified: created_at,
            expires_at,
            created_at,
            content_type: Some("text/plain".to_string()),
            filename: None,
        }
    }

    fn dt(value: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(value)
            .expect("test timestamp must parse")
            .with_timezone(&Utc)
    }

    #[test]
    fn sort_pastes_covers_size_created_and_expires() {
        let early = dt("2026-10-01T00:00:00Z");
        let late = dt("2026-10-02T00:00:00Z");
        let soon = dt("2026-10-10T00:00:00Z");
        let later = dt("2026-10-20T00:00:00Z");
        let mut items = vec![
            summary("2026-10-28/a", 20, Some(late), Some(later)),
            summary("2026-10-28/b", 10, Some(early), None),
            summary("2026-10-28/c", 30, None, Some(soon)),
        ];

        sort_pastes(&mut items, PasteSort::Size, SortOrder::Desc);
        assert_eq!(
            items
                .iter()
                .map(|item| item.id.as_str())
                .collect::<Vec<_>>(),
            ["2026-10-28/c", "2026-10-28/a", "2026-10-28/b"]
        );
        sort_pastes(&mut items, PasteSort::Size, SortOrder::Asc);
        assert_eq!(
            items
                .iter()
                .map(|item| item.id.as_str())
                .collect::<Vec<_>>(),
            ["2026-10-28/b", "2026-10-28/a", "2026-10-28/c"]
        );

        sort_pastes(&mut items, PasteSort::Created, SortOrder::Desc);
        assert_eq!(
            items
                .iter()
                .map(|item| item.id.as_str())
                .collect::<Vec<_>>(),
            ["2026-10-28/a", "2026-10-28/b", "2026-10-28/c"]
        );
        sort_pastes(&mut items, PasteSort::Created, SortOrder::Asc);
        assert_eq!(
            items
                .iter()
                .map(|item| item.id.as_str())
                .collect::<Vec<_>>(),
            ["2026-10-28/b", "2026-10-28/a", "2026-10-28/c"]
        );

        sort_pastes(&mut items, PasteSort::Expires, SortOrder::Asc);
        assert_eq!(
            items
                .iter()
                .map(|item| item.id.as_str())
                .collect::<Vec<_>>(),
            ["2026-10-28/c", "2026-10-28/a", "2026-10-28/b"]
        );
        sort_pastes(&mut items, PasteSort::Expires, SortOrder::Desc);
        assert_eq!(
            items
                .iter()
                .map(|item| item.id.as_str())
                .collect::<Vec<_>>(),
            ["2026-10-28/a", "2026-10-28/c", "2026-10-28/b"]
        );
    }

    #[test]
    fn sort_defaults_to_created_descending_and_rejects_bad_values() {
        let empty = HashMap::new();
        assert_eq!(
            parse_sort_order(&empty).expect("defaults must parse"),
            (PasteSort::Created, SortOrder::Desc)
        );

        let mut size = HashMap::new();
        size.insert("sort".to_string(), "size".to_string());
        assert_eq!(
            parse_sort_order(&size).expect("size must parse"),
            (PasteSort::Size, SortOrder::Desc)
        );

        let mut expires = HashMap::new();
        expires.insert("sort".to_string(), "expires".to_string());
        assert_eq!(
            parse_sort_order(&expires).expect("expires must parse"),
            (PasteSort::Expires, SortOrder::Asc)
        );

        let mut bad_sort = HashMap::new();
        bad_sort.insert("sort".to_string(), "bogus".to_string());
        assert!(parse_sort_order(&bad_sort).is_err());

        let mut bad_order = HashMap::new();
        bad_order.insert("order".to_string(), "sideways".to_string());
        assert!(parse_sort_order(&bad_order).is_err());
    }

    #[test]
    fn sort_headers_toggle_and_keep_filters() {
        let filters = "date=2026-10-28&start=2026-10-01&end=2026-10-31";
        let created = sort_header(
            "Created",
            PasteSort::Created,
            PasteSort::Created,
            SortOrder::Desc,
            filters,
        );
        assert!(
            created.contains(
                "href=\"/pastes?sort=created&amp;order=asc&amp;date=2026-10-28&amp;start=2026-10-01&amp;end=2026-10-31\""
            ),
            "got: {created}"
        );
        assert!(created.contains("Created ▼"), "got: {created}");

        let size = sort_header(
            "Size",
            PasteSort::Size,
            PasteSort::Created,
            SortOrder::Desc,
            filters,
        );
        assert!(
            size.contains(
                "href=\"/pastes?sort=size&amp;order=desc&amp;date=2026-10-28&amp;start=2026-10-01&amp;end=2026-10-31\""
            ),
            "got: {size}"
        );
        assert!(
            !size.contains('▼'),
            "inactive header must have no arrow: {size}"
        );
    }

    #[tokio::test]
    async fn list_rejects_invalid_sort_and_order() {
        let state = test_state(None).await;
        let app = ui_router(state.clone()).with_state(state);
        for uri in ["/pastes?sort=bogus", "/pastes?order=sideways"] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("GET")
                        .uri(uri)
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .expect("router call must complete");
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
        }
    }

    #[test]
    fn error_page_uses_the_shared_status_mapping() {
        assert_eq!(
            error_page(&Footer::default(), StoreError::Io("disk".to_string())).status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            error_page(&Footer::default(), StoreError::S3("s3".to_string())).status(),
            StatusCode::BAD_GATEWAY
        );
        assert_eq!(
            error_page(&Footer::default(), StoreError::QuotaExceeded).status(),
            StatusCode::INSUFFICIENT_STORAGE
        );
    }

    #[test]
    fn page_escapes_the_title() {
        let rendered = page(
            &Footer::default(),
            "<script>alert(1)</script>",
            None,
            true,
            "",
        )
        .0;
        assert!(
            rendered.contains("<title>&lt;script&gt;alert(1)&lt;/script&gt; — dayfly</title>"),
            "the title tag must be escaped: {rendered}"
        );
        assert!(
            rendered.contains("<h1>&lt;script&gt;alert(1)&lt;/script&gt;</h1>"),
            "the heading must be escaped: {rendered}"
        );
        assert!(!rendered.contains("<script>alert(1)</script>"));
    }

    #[test]
    fn footer_defaults_to_internal_use() {
        let rendered = page(&Footer::default(), "Home", None, true, "").0;
        assert!(
            rendered.contains("<footer>dayfly — internal use only · &copy; 2026 suorcd</footer>"),
            "the default footer must keep the internal-use text: {rendered}"
        );
    }

    #[test]
    fn footer_site_name_replaces_internal_use() {
        let footer = Footer {
            site_name: Some("dayfly".to_string()),
            ..Footer::default()
        };
        assert_eq!(footer.render(), "dayfly · &copy; 2026 suorcd");
    }

    #[test]
    fn footer_links_contact_and_acceptable_use() {
        let footer = Footer {
            site_name: Some("pastes.example".to_string()),
            contact: Some("abuse@example.com".to_string()),
            aup_url: Some("https://example.com/aup".to_string()),
        };
        assert_eq!(
            footer.render(),
            "pastes.example · <a href=\"mailto:abuse@example.com\">abuse@example.com</a> · \
             <a href=\"https://example.com/aup\">acceptable use</a> · &copy; 2026 suorcd"
        );
    }

    #[test]
    fn footer_escapes_configured_values() {
        let footer = Footer {
            site_name: Some("<script>alert(1)</script>".to_string()),
            contact: Some("\"><script>alert(2)</script>".to_string()),
            aup_url: Some("https://example.com/\"x".to_string()),
        };
        let rendered = footer.render();
        assert!(!rendered.contains("<script>"));
        assert!(rendered.contains("&lt;script&gt;"));
    }

    #[test]
    fn html_escape_covers_script() {
        assert_eq!(
            html_escape("<script>alert(\"x\")</script>"),
            "&lt;script&gt;alert(&quot;x&quot;)&lt;/script&gt;"
        );
    }

    #[test]
    fn middle_truncate_filename_keeps_the_extension() {
        assert_eq!(middle_truncate_filename("short.txt"), "short.txt");
        assert_eq!(
            middle_truncate_filename("a2345678901234567890123456"),
            "a23456789012345…7890123456"
        );
        assert_eq!(
            middle_truncate_filename("very-long-report-name-that-keeps-going.xlsx"),
            "very-long-repor…going.xlsx"
        );
    }

    #[test]
    fn human_size_units() {
        assert_eq!(human_size(0), "0 B");
        assert_eq!(human_size(512), "512 B");
        assert_eq!(human_size(2048), "2 KiB");
        assert_eq!(human_size(5 * 1024 * 1024), "5 MiB");
        assert_eq!(human_size(1024 * 1024 - 1), "1 MiB");
        assert_eq!(human_size_parts(1536), (2, "KiB"));
    }

    #[test]
    fn days_remaining_expired_and_future() {
        let past = Utc::now() - chrono::Duration::days(1);
        assert_eq!(days_remaining(Some(past)), "expired");
        let today = Utc::now();
        let future = today + chrono::Duration::days(3);
        assert_eq!(days_remaining(Some(future)), "03");
        assert_eq!(days_remaining_label(Some(future)), "03 days");
        let tomorrow = today + chrono::Duration::hours(1);
        assert_eq!(days_remaining(Some(tomorrow)), "01");
        assert_eq!(days_remaining_label(Some(tomorrow)), "01 day");
        assert_eq!(days_remaining(None), "—");
    }

    fn same_origin(host: &str, origin: Option<&str>, referer: Option<&str>) -> bool {
        let mut headers = HeaderMap::new();
        headers.insert(HOST, host.parse().expect("host header"));
        if let Some(origin) = origin {
            headers.insert(ORIGIN, origin.parse().expect("origin header"));
        }
        if let Some(referer) = referer {
            headers.insert(REFERER, referer.parse().expect("referer header"));
        }
        same_origin_allowed(&headers)
    }

    #[test]
    fn same_origin_origin_is_allowed() {
        assert!(same_origin(
            "dayfly.example",
            Some("https://dayfly.example"),
            None
        ));
    }

    #[test]
    fn cross_site_origin_is_denied() {
        assert!(!same_origin(
            "dayfly.example",
            Some("https://evil.example"),
            None
        ));
    }

    #[test]
    fn same_origin_referer_with_path_is_allowed() {
        assert!(same_origin(
            "dayfly.example:8080",
            None,
            Some("http://dayfly.example:8080/pastes")
        ));
    }

    #[test]
    fn cross_site_referer_is_denied() {
        assert!(!same_origin(
            "dayfly.example",
            None,
            Some("https://evil.example/pastes")
        ));
    }

    #[test]
    fn missing_origin_and_referer_is_denied() {
        assert!(!same_origin("dayfly.example", None, None));
    }

    #[test]
    fn null_origin_is_denied() {
        assert!(!same_origin("dayfly.example", Some("null"), None));
    }

    #[test]
    fn malformed_origin_is_denied() {
        assert!(!same_origin("dayfly.example", Some("https://"), None));
        assert!(!same_origin("dayfly.example", Some("not a url"), None));
    }

    #[test]
    fn malformed_origin_does_not_fall_back_to_referer() {
        assert!(!same_origin(
            "dayfly.example",
            Some("null"),
            Some("https://dayfly.example/pastes")
        ));
    }

    #[test]
    fn host_comparison_ignores_ascii_case() {
        assert!(same_origin(
            "DAYFLY.EXAMPLE",
            Some("https://dayfly.EXAMPLE"),
            None
        ));
    }

    #[test]
    fn default_port_matches_omitted_port() {
        assert!(same_origin(
            "dayfly.example",
            Some("https://dayfly.example:443"),
            None
        ));
    }

    #[tokio::test]
    async fn cross_site_delete_post_is_forbidden() {
        let state = AppState::new(
            Store::from_config(test_config())
                .await
                .expect("test S3 config must build without network access"),
        );
        let app = ui_router(state.clone()).with_state(state);
        let request = Request::builder()
            .method("POST")
            .uri("/ui/delete")
            .header(HOST, "dayfly.example")
            .header(ORIGIN, "https://evil.example")
            .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from("id=2026-10-28/abc123"))
            .expect("request must build");
        let response = app
            .oneshot(request)
            .await
            .expect("router call must complete");
        assert_eq!(
            response.status(),
            StatusCode::FORBIDDEN,
            "a cross-site delete post must be rejected"
        );
    }
}
