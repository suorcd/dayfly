//! Content-Type detection for uploads.
//!
//! Precedence, in order:
//!
//! 1. A specific client-supplied Content-Type — except curl's
//!    `--data-binary` default, `application/x-www-form-urlencoded`, and the
//!    generic `application/octet-stream`, which are treated as unspecified.
//! 2. Magic-byte sniffing of the first bytes (the `infer` crate) for
//!    images, PDF, ZIP, video/audio containers, and other signatures.
//!    A ZIP signature alone cannot name an Office, OpenDocument, or
//!    EPUB document, so a known zip-based extension refines the type.
//!    If sniffing finds a well-known signature and the client said
//!    something different, the detected type wins and the conflict is
//!    recorded in metadata.
//! 3. Filename extension via `mime_guess`, when no signature was found
//!    (and the extension maps to something more specific than octet-stream).
//! 4. Plain-text detection: valid UTF-8 with no NUL bytes in the whole
//!    payload.
//! 5. Fallback: `application/octet-stream`.
//!
//! Buffered uploads detect on the whole payload. Streaming uploads
//! peek at the first chunk (up to 8 KiB, see [`detect_and_wrap`]) and
//! forward those same bytes into the upload stream. A streamed sample
//! that is truncated cannot prove that the whole body is text. Such a
//! sample falls back to `application/octet-stream`, unless a known
//! length shows that the whole body fits in the sample.

use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::Stream;
use futures::StreamExt;

use crate::store::StoreError;

/// How many bytes are peeked for sniffing. The peek buffer may hold one
/// whole incoming chunk if that chunk is larger than this (chunk sizes
/// from HTTP bodies are bounded), but a streamed upload sees no more
/// than this sample before it starts the S3 upload.
pub const PEEK_BYTES: usize = 8 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Detection {
    pub content_type: String,
    /// One of: "client" | "sniffed" | "extension" | "text" | "fallback".
    pub source: &'static str,
    /// `Some("explicit -> sniffed")` when a specific client type was
    /// overridden by a well-known sniffed signature.
    pub conflict: Option<String>,
}

/// Values that mean "the client did not really choose a type".
fn is_generic_type(normalised: &str) -> bool {
    matches!(
        normalised,
        "application/x-www-form-urlencoded" | "application/octet-stream" | ""
    )
}

/// Normalise a client Content-Type: lowercase, strip parameters, trim.
fn normalise_explicit(explicit: Option<&str>) -> Option<String> {
    let raw = explicit?.trim();
    if raw.is_empty() {
        return None;
    }
    let main = raw.split(';').next()?.trim().to_ascii_lowercase();
    if main.is_empty() {
        None
    } else {
        Some(main)
    }
}

fn extension_of(filename: Option<&str>) -> Option<String> {
    let name = filename?;
    Some(Path::new(name).extension()?.to_str()?.to_ascii_lowercase())
}

fn extension_type(filename: Option<&str>) -> Option<String> {
    let ext = extension_of(filename)?;
    let mime = mime_guess::from_ext(&ext).first()?;
    let s = mime.to_string();
    if s == "application/octet-stream" {
        None
    } else {
        Some(s)
    }
}

/// Zip-based document extensions. `infer` sees only the ZIP signature,
/// and the filename names the specific type inside the container.
/// `mime_guess` returns the exact type for these; the
/// `zip_document_extensions_map_to_specific_types` test checks that.
const MIME_GUESS_ZIP_DOCUMENTS: &[&str] =
    &["docx", "epub", "odg", "odp", "ods", "odt", "pptx", "xlsx"];

/// Zip-based macro-enabled document extensions. `mime_guess` lowercases
/// the `macroEnabled` segment, so it does not return the exact type.
const MACRO_ZIP_DOCUMENTS: &[(&str, &str)] = &[
    ("docm", "application/vnd.ms-word.document.macroEnabled.12"),
    (
        "pptm",
        "application/vnd.ms-powerpoint.presentation.macroEnabled.12",
    ),
    ("xlsm", "application/vnd.ms-excel.sheet.macroEnabled.12"),
];

/// The specific type of a zip-based document, if the filename names one.
fn zip_document_type(filename: Option<&str>) -> Option<String> {
    let ext = extension_of(filename)?;
    if let Some((_, mime)) = MACRO_ZIP_DOCUMENTS
        .iter()
        .find(|(name, _)| *name == ext.as_str())
    {
        return Some((*mime).to_string());
    }
    if MIME_GUESS_ZIP_DOCUMENTS.contains(&ext.as_str()) {
        return extension_type(filename);
    }
    None
}

fn is_text(prefix: &[u8]) -> bool {
    !prefix.contains(&0) && std::str::from_utf8(prefix).is_ok()
}

/// Pure detection function: given an explicit type, a filename, and a
/// payload known in full, decide the final Content-Type.
pub fn detect_content_type(
    explicit: Option<&str>,
    filename: Option<&str>,
    payload: &[u8],
) -> Detection {
    detect_content_type_sample(explicit, filename, payload, true)
}

/// Pure detection function for a sample that may be truncated. When
/// `sample_complete` is false, plain-text detection cannot prove that
/// the whole body is valid UTF-8 with no NUL bytes, so the fallback
/// wins. Sniffing, an explicit client type, and a filename extension
/// still apply to a truncated sample.
pub fn detect_content_type_sample(
    explicit: Option<&str>,
    filename: Option<&str>,
    sample: &[u8],
    sample_complete: bool,
) -> Detection {
    let explicit_norm = normalise_explicit(explicit);
    let explicit_specific = explicit_norm.clone().filter(|e| !is_generic_type(e));

    // 2. Magic-byte sniffing. `infer` only returns specific signatures
    // and only reads bytes near the start of the sample.
    if let Some(kind) = infer::get(sample) {
        let mut sniffed = kind.mime_type().to_string();
        let mut source: &'static str = "sniffed";
        // The ZIP signature alone cannot name an Office, OpenDocument,
        // or EPUB document. A known zip-based extension supplies the
        // exact type.
        if sniffed == "application/zip" {
            if let Some(container) = zip_document_type(filename) {
                sniffed = container;
                source = "extension";
            }
        }
        if let Some(exp) = &explicit_specific {
            if *exp == sniffed {
                return Detection {
                    content_type: sniffed,
                    source,
                    conflict: None,
                };
            }
            return Detection {
                content_type: sniffed.clone(),
                source,
                conflict: Some(format!("{exp} -> {sniffed}")),
            };
        }
        return Detection {
            content_type: sniffed,
            source,
            conflict: None,
        };
    }

    // 1. Specific client type (when sniffing did not find a signature).
    if let Some(exp) = explicit_specific {
        return Detection {
            content_type: exp,
            source: "client",
            conflict: None,
        };
    }

    // 3. Filename extension.
    if let Some(ext_type) = extension_type(filename) {
        return Detection {
            content_type: ext_type,
            source: "extension",
            conflict: None,
        };
    }

    // 4. Plain text, but only when the sample is the whole body.
    if sample_complete && is_text(sample) {
        return Detection {
            content_type: "text/plain; charset=utf-8".to_string(),
            source: "text",
            conflict: None,
        };
    }

    // 5. Fallback.
    Detection {
        content_type: "application/octet-stream".to_string(),
        source: "fallback",
        conflict: None,
    }
}

/// Content-Disposition for serving an object back.
///
/// Safe inline types are served inline so a web frontend can render an
/// image or show text. Everything else, and specifically `text/html` and
/// `image/svg+xml`, is served as an attachment. This is a deliberate v1
/// choice, not a sandbox: see README. Serving sniffed HTML/SVG inline on
/// a trusted origin would let an uploaded file run script in that
/// origin's context.
pub fn content_disposition(content_type: &str, filename: Option<&str>) -> String {
    let main = content_type.split(';').next().unwrap_or("").trim();
    let safe_inline = matches!(
        main,
        "text/plain"
            | "text/csv"
            | "text/markdown"
            | "image/png"
            | "image/jpeg"
            | "image/gif"
            | "image/webp"
            | "application/pdf"
    );
    let disposition = if safe_inline { "inline" } else { "attachment" };
    match filename {
        Some(f) if !f.is_empty() => {
            let safe: String = f
                .chars()
                .map(|c| {
                    if c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_') {
                        c
                    } else {
                        '_'
                    }
                })
                .collect();
            format!("{disposition}; filename=\"{safe}\"")
        }
        _ => disposition.to_string(),
    }
}

/// A stream that first yields a buffered peek, then the inner stream.
/// Used by [`detect_and_wrap`] so peeked bytes are neither lost nor
/// duplicated.
struct PeekedStream<S> {
    peek: Option<Bytes>,
    inner: S,
}

impl<S> Stream for PeekedStream<S>
where
    S: Stream<Item = Result<Bytes, StoreError>> + Unpin,
{
    type Item = Result<Bytes, StoreError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(peek) = self.peek.take() {
            return Poll::Ready(Some(Ok(peek)));
        }
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

/// Peek at the start of a stream, detect its Content-Type, and return a
/// stream that yields exactly the same bytes (peek first, then the rest).
///
/// The peek accumulates chunks until it holds at least [`PEEK_BYTES`]
/// bytes or the stream ends. If the very first chunk is larger than
/// [`PEEK_BYTES`], the whole chunk is buffered and forwarded, but
/// detection sees no more than the first [`PEEK_BYTES`] bytes. A
/// `known_length` at or below [`PEEK_BYTES`] proves that the sample is
/// the whole body, so text detection can run on it.
pub async fn detect_and_wrap<'a, S>(
    stream: S,
    explicit: Option<&str>,
    filename: Option<&str>,
    known_length: Option<u64>,
) -> (
    Detection,
    Pin<Box<dyn Stream<Item = Result<Bytes, StoreError>> + Send + 'a>>,
)
where
    S: Stream<Item = Result<Bytes, StoreError>> + Send + Unpin + 'a,
{
    // Fuse the input once. A multipart field stream built with
    // `stream::unfold` panics when it is polled after it returns `None`,
    // and both detection and the upload driver may poll a short body to
    // EOF before the upload path runs.
    let mut stream = stream.fuse();
    let mut buffer: Vec<u8> = Vec::new();
    let mut first_error: Option<StoreError> = None;
    while buffer.len() < PEEK_BYTES {
        match stream.next().await {
            Some(Ok(chunk)) => {
                if chunk.is_empty() {
                    continue;
                }
                buffer.extend_from_slice(&chunk);
            }
            Some(Err(e)) => {
                first_error = Some(e);
                break;
            }
            None => break,
        }
    }
    if let Some(e) = first_error {
        // Detection cannot run; forward the error as the first item so
        // the upload fails (and is aborted upstream) rather than
        // silently succeeding with a wrong type.
        let err_stream = futures::stream::iter(vec![Err(e)]).chain(stream);
        return (
            Detection {
                content_type: "application/octet-stream".to_string(),
                source: "fallback",
                conflict: None,
            },
            Box::pin(err_stream),
        );
    }
    // The sample is complete when the stream ended below the cap, or
    // when a known length shows that the whole body fits in the sample.
    let sample_complete =
        known_length.map_or(buffer.len() < PEEK_BYTES, |len| len <= PEEK_BYTES as u64);
    let detection = detect_content_type_sample(explicit, filename, &buffer, sample_complete);
    let peek = if buffer.is_empty() {
        None
    } else {
        Some(Bytes::from(buffer))
    };
    let wrapped = PeekedStream {
        peek,
        inner: stream,
    };
    (detection, Box::pin(wrapped))
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;

    fn png() -> Vec<u8> {
        let mut v = vec![0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a];
        v.extend_from_slice(b"fake-png-body");
        v
    }
    fn jpeg() -> Vec<u8> {
        vec![
            0xff, 0xd8, 0xff, 0xe0, 0x00, 0x10, b'J', b'F', b'I', b'F', 0x00,
        ]
    }
    fn gif() -> Vec<u8> {
        b"GIF89a".to_vec()
    }
    fn webp() -> Vec<u8> {
        let mut v = b"RIFF".to_vec();
        v.extend_from_slice(&[0x10, 0x00, 0x00, 0x00]);
        v.extend_from_slice(b"WEBP");
        v
    }
    fn pdf() -> Vec<u8> {
        b"%PDF-1.7 fake".to_vec()
    }
    fn zip() -> Vec<u8> {
        vec![b'P', b'K', 0x03, 0x04, 0x14, 0x00]
    }

    #[test]
    fn png_detected() {
        let d = detect_content_type(None, None, &png());
        assert_eq!(d.content_type, "image/png");
        assert_eq!(d.source, "sniffed");
    }

    #[test]
    fn jpeg_detected() {
        let d = detect_content_type(None, None, &jpeg());
        assert_eq!(d.content_type, "image/jpeg");
        assert_eq!(d.source, "sniffed");
    }

    #[test]
    fn gif_detected() {
        let d = detect_content_type(None, None, &gif());
        assert_eq!(d.content_type, "image/gif");
    }

    #[test]
    fn webp_detected() {
        let d = detect_content_type(None, None, &webp());
        assert_eq!(d.content_type, "image/webp");
    }

    #[test]
    fn pdf_detected() {
        let d = detect_content_type(None, None, &pdf());
        assert_eq!(d.content_type, "application/pdf");
    }

    #[test]
    fn zip_detected() {
        let d = detect_content_type(None, None, &zip());
        assert_eq!(d.content_type, "application/zip");
    }

    const OOXML_DOCX: &str =
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document";
    const OOXML_XLSX: &str = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
    const OOXML_PPTX: &str =
        "application/vnd.openxmlformats-officedocument.presentationml.presentation";
    const ODF_TEXT: &str = "application/vnd.oasis.opendocument.text";

    #[test]
    fn docx_detected_as_wordprocessingml() {
        let d = detect_content_type(None, Some("report.docx"), &zip());
        assert_eq!(d.content_type, OOXML_DOCX);
        assert_eq!(d.source, "extension");
    }

    #[test]
    fn odt_detected_as_opendocument_text() {
        let d = detect_content_type(None, Some("notes.odt"), &zip());
        assert_eq!(d.content_type, ODF_TEXT);
        assert_eq!(d.source, "extension");
    }

    #[test]
    fn zip_extension_stays_application_zip() {
        let d = detect_content_type(None, Some("archive.zip"), &zip());
        assert_eq!(d.content_type, "application/zip");
        assert_eq!(d.source, "sniffed");
    }

    #[test]
    fn docx_explicit_conflicting_type_records_conflict() {
        let d = detect_content_type(Some("text/plain"), Some("report.docx"), &zip());
        assert_eq!(d.content_type, OOXML_DOCX);
        assert_eq!(d.source, "extension");
        assert_eq!(d.conflict, Some(format!("text/plain -> {OOXML_DOCX}")));
    }

    #[test]
    fn docx_explicit_matching_type_has_no_conflict() {
        let d = detect_content_type(Some(OOXML_DOCX), Some("report.docx"), &zip());
        assert_eq!(d.content_type, OOXML_DOCX);
        assert_eq!(d.source, "extension");
        assert!(d.conflict.is_none());
    }

    #[test]
    fn docx_filename_without_zip_bytes_uses_extension_path() {
        let d = detect_content_type(None, Some("report.docx"), &[0x00, 0xff, 0x01, 0x02]);
        assert_eq!(d.content_type, OOXML_DOCX);
        assert_eq!(d.source, "extension");
        assert!(d.conflict.is_none());
    }

    #[test]
    fn zip_document_extensions_map_to_specific_types() {
        let cases = [
            ("docm", "application/vnd.ms-word.document.macroEnabled.12"),
            ("docx", OOXML_DOCX),
            ("epub", "application/epub+zip"),
            ("odg", "application/vnd.oasis.opendocument.graphics"),
            ("odp", "application/vnd.oasis.opendocument.presentation"),
            ("ods", "application/vnd.oasis.opendocument.spreadsheet"),
            ("odt", ODF_TEXT),
            (
                "pptm",
                "application/vnd.ms-powerpoint.presentation.macroEnabled.12",
            ),
            ("pptx", OOXML_PPTX),
            ("xlsm", "application/vnd.ms-excel.sheet.macroEnabled.12"),
            ("xlsx", OOXML_XLSX),
        ];
        for (ext, expected) in cases {
            let name = format!("document.{ext}");
            let d = detect_content_type(None, Some(&name), &zip());
            assert_eq!(d.content_type, expected, "extension {ext}");
            assert_eq!(d.source, "extension", "extension {ext}");
        }
    }

    #[test]
    fn utf8_text_detected() {
        let d = detect_content_type(None, None, b"hello, world\nsecond line\n");
        assert_eq!(d.content_type, "text/plain; charset=utf-8");
        assert_eq!(d.source, "text");
    }

    #[test]
    fn ascii_payload_with_nul_after_peek_is_not_text() {
        let mut payload = vec![b'a'; PEEK_BYTES];
        payload.push(0);
        let d = detect_content_type(None, None, &payload);
        assert_eq!(d.content_type, "application/octet-stream");
        assert_eq!(d.source, "fallback");
    }

    #[test]
    fn utf8_text_over_peek_is_text_when_payload_is_whole() {
        let payload = vec![b'a'; PEEK_BYTES + 1];
        let d = detect_content_type(None, None, &payload);
        assert_eq!(d.content_type, "text/plain; charset=utf-8");
        assert_eq!(d.source, "text");
    }

    #[test]
    fn arbitrary_binary_falls_back() {
        let d = detect_content_type(None, None, &[0x00, 0xff, 0x01, 0x02, 0xfe, 0x00]);
        assert_eq!(d.content_type, "application/octet-stream");
        assert_eq!(d.source, "fallback");
    }

    #[test]
    fn curl_form_urlencoded_default_is_ignored_in_favour_of_sniffing() {
        let d = detect_content_type(Some("application/x-www-form-urlencoded"), None, &png());
        assert_eq!(d.content_type, "image/png");
        assert_eq!(d.source, "sniffed");
        assert!(d.conflict.is_none(), "a default is not a conflict");
    }

    #[test]
    fn explicit_type_overrides_generic_text() {
        let d = detect_content_type(Some("text/csv"), None, b"a,b\n1,2\n");
        assert_eq!(d.content_type, "text/csv");
        assert_eq!(d.source, "client");
        let d2 = detect_content_type(Some("application/json"), None, b"{\"a\": 1}");
        assert_eq!(d2.content_type, "application/json");
        assert_eq!(d2.source, "client");
    }

    #[test]
    fn explicit_type_overrides_extension() {
        let d = detect_content_type(Some("text/csv"), Some("data.txt"), b"a,b\n");
        assert_eq!(d.content_type, "text/csv");
        assert_eq!(d.source, "client");
    }

    #[test]
    fn extension_used_when_no_explicit_type() {
        let d = detect_content_type(None, Some("data.csv"), b"a,b\n1,2\n");
        assert_eq!(d.content_type, "text/csv");
        assert_eq!(d.source, "extension");
    }

    #[test]
    fn sniffed_signature_wins_over_wrong_explicit_type_and_records_conflict() {
        let d = detect_content_type(Some("text/plain"), None, &png());
        assert_eq!(d.content_type, "image/png");
        assert_eq!(d.source, "sniffed");
        assert_eq!(d.conflict.as_deref(), Some("text/plain -> image/png"));
    }

    #[test]
    fn content_disposition_html_and_svg_are_attachments() {
        assert!(content_disposition("text/html", Some("x.html")).starts_with("attachment"));
        assert!(content_disposition("image/svg+xml", Some("x.svg")).starts_with("attachment"));
        assert!(content_disposition("image/png", Some("x.png")).starts_with("inline"));
        assert!(content_disposition("text/plain", None).starts_with("inline"));
        assert!(content_disposition("application/octet-stream", None).starts_with("attachment"));
    }

    #[tokio::test]
    async fn peeked_bytes_are_neither_lost_nor_duplicated() {
        // Input delivered as many small chunks, including a first chunk
        // smaller than the peek window, followed by a large chunk.
        let mut input: Vec<u8> = png();
        input.extend_from_slice(&vec![b'z'; 20_000]);
        let chunks: Vec<Result<Bytes, StoreError>> = vec![
            Ok(Bytes::from(input[..100].to_vec())),
            Ok(Bytes::from(input[100..5000].to_vec())),
            Ok(Bytes::from(input[5000..].to_vec())),
        ];
        let (detection, wrapped) = detect_and_wrap(stream::iter(chunks), None, None, None).await;
        assert_eq!(detection.content_type, "image/png");
        let mut out = Vec::new();
        let mut s = wrapped;
        while let Some(item) = s.next().await {
            out.extend_from_slice(&item.unwrap());
        }
        assert_eq!(out, input, "peeked bytes must be forwarded exactly once");
    }

    #[tokio::test]
    async fn truncated_streamed_text_falls_back_to_octet_stream() {
        let input = vec![b'a'; PEEK_BYTES + 1];
        let chunks = vec![Ok(Bytes::from(input))];
        let (detection, _wrapped) = detect_and_wrap(stream::iter(chunks), None, None, None).await;
        assert_eq!(detection.content_type, "application/octet-stream");
        assert_eq!(detection.source, "fallback");
    }

    #[tokio::test]
    async fn short_streamed_text_is_text() {
        let chunks = vec![Ok(Bytes::from_static(b"hello, world\nsecond line\n"))];
        let (detection, _wrapped) = detect_and_wrap(stream::iter(chunks), None, None, None).await;
        assert_eq!(detection.content_type, "text/plain; charset=utf-8");
        assert_eq!(detection.source, "text");
    }

    #[tokio::test]
    async fn known_length_within_peek_allows_text() {
        let input = vec![b'a'; PEEK_BYTES];
        let chunks = vec![Ok(Bytes::from(input))];
        let (detection, _wrapped) =
            detect_and_wrap(stream::iter(chunks), None, None, Some(PEEK_BYTES as u64)).await;
        assert_eq!(detection.content_type, "text/plain; charset=utf-8");
        assert_eq!(detection.source, "text");
    }

    #[tokio::test]
    async fn explicit_text_plain_wins_with_truncated_sample() {
        let input = vec![b'a'; PEEK_BYTES + 1];
        let chunks = vec![Ok(Bytes::from(input))];
        let (detection, _wrapped) =
            detect_and_wrap(stream::iter(chunks), Some("text/plain"), None, None).await;
        assert_eq!(detection.content_type, "text/plain");
        assert_eq!(detection.source, "client");
    }

    #[tokio::test]
    async fn extension_wins_with_truncated_sample() {
        let input = vec![b'a'; PEEK_BYTES + 1];
        let chunks = vec![Ok(Bytes::from(input))];
        let (detection, _wrapped) =
            detect_and_wrap(stream::iter(chunks), None, Some("notes.csv"), None).await;
        assert_eq!(detection.content_type, "text/csv");
        assert_eq!(detection.source, "extension");
    }

    #[tokio::test]
    async fn truncated_single_large_chunk_falls_back_and_forwards_bytes() {
        let input = vec![b'a'; 20_000];
        let chunks = vec![Ok(Bytes::from(input.clone()))];
        let (detection, wrapped) = detect_and_wrap(stream::iter(chunks), None, None, None).await;
        assert_eq!(detection.content_type, "application/octet-stream");
        assert_eq!(detection.source, "fallback");
        let mut out = Vec::new();
        let mut s = wrapped;
        while let Some(item) = s.next().await {
            out.extend_from_slice(&item.unwrap());
        }
        assert_eq!(out, input);
    }
}
