//! NSV (Newline-Separated Values) format implementation for Rust
//!
//! Fast parallel implementation using rayon and memchr.
//! See https://nsv-format.org for the specification.
//!
//! ## Parallel Parsing Strategy
//!
//! For files larger than 64KB, we use a chunked parallel approach:
//! 1. Pick N evenly-spaced byte positions (one per CPU core)
//! 2. For each, scan forward to the nearest `\n\n` row boundary — O(avg_row_len)
//! 3. Each worker independently parses its chunk (boundary scan + cell split + unescape)
//!
//! This works because literal `0x0A` bytes in NSV are always structural (never escaped),
//! so row alignment recovery from any byte position is a trivial forward scan.
//! The sequential phase is O(N), not O(input_len) — all real work is parallel.
//!
//! For smaller files, we use a sequential fast path to avoid thread overhead.

pub mod util;

#[cfg(feature = "rayon")]
use memchr::memmem;
#[cfg(feature = "rayon")]
use rayon::prelude::*;

use std::borrow::Cow;
use std::io::{self, Read, Write};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Threshold for using parallel parsing (64KB)
#[cfg(feature = "rayon")]
const PARALLEL_THRESHOLD: usize = 64 * 1024;

/// Decode an NSV string into a seqseq.
pub fn decode(s: &str) -> Vec<Vec<String>> {
    decode_bytes(s.as_bytes())
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|cell| {
                    // SAFETY: input was &str (valid UTF-8). NSV splitting and unescaping
                    // only operate on ASCII bytes (0x0A, 0x5C, 0x6E), which cannot split
                    // a multi-byte UTF-8 sequence. Each resulting cell is therefore valid UTF-8.
                    unsafe { String::from_utf8_unchecked(cell.into_owned()) }
                })
                .collect()
        })
        .collect()
}

/// Decode raw bytes into a seqseq of byte slices.
/// No encoding assumption — works with any ASCII-compatible encoding.
///
/// Cells are returned as `Cow<[u8]>` — borrowed when no unescaping was needed
/// (zero-copy), owned when the cell contained escape sequences.
pub fn decode_bytes<'a>(input: &'a [u8]) -> Vec<Vec<Cow<'a, [u8]>>> {
    if input.is_empty() {
        return Vec::new();
    }

    #[cfg(feature = "rayon")]
    if input.len() >= PARALLEL_THRESHOLD {
        return decode_bytes_parallel(input);
    }

    decode_bytes_sequential(input)
}

/// Sequential implementation for small inputs (byte-level).
fn decode_bytes_sequential<'a>(input: &'a [u8]) -> Vec<Vec<Cow<'a, [u8]>>> {
    let mut data = Vec::new();
    let mut row: Vec<Cow<'a, [u8]>> = Vec::new();
    let mut start = 0;

    for (pos, &b) in input.iter().enumerate() {
        if b == b'\n' {
            if pos > start {
                row.push(unescape_bytes(&input[start..pos]));
            } else {
                data.push(row);
                row = Vec::new();
            }
            start = pos + 1;
        }
    }

    if start < input.len() {
        row.push(unescape_bytes(&input[start..]));
    }

    if !row.is_empty() {
        data.push(row);
    }

    data
}

#[cfg(feature = "rayon")]
/// Chunked parallel implementation for large inputs (byte-level).
///
/// Splits the input into N equal-sized byte chunks (one per core), aligns each
/// split point to the nearest `\n\n` row boundary, and parses each chunk
/// independently. The sequential phase is O(N), not O(input_len).
fn decode_bytes_parallel<'a>(input: &'a [u8]) -> Vec<Vec<Cow<'a, [u8]>>> {
    let num_threads = rayon::current_num_threads();
    let chunk_size = input.len() / num_threads;

    if chunk_size == 0 {
        return decode_bytes_sequential(input);
    }

    // Find N-1 split points at \n\n boundaries near evenly-spaced positions.
    // Cost: O(N * avg_row_len) — negligible compared to input size.
    let finder = memmem::Finder::new(b"\n\n");
    let mut splits = Vec::with_capacity(num_threads + 1);
    splits.push(0usize);

    for i in 1..num_threads {
        let nominal = i * chunk_size;
        if let Some(offset) = finder.find(&input[nominal..]) {
            let split = nominal + offset + 2; // byte after \n\n
            if split < input.len() {
                splits.push(split);
            }
        }
    }
    splits.push(input.len());
    splits.dedup();

    if splits.len() <= 2 {
        return decode_bytes_sequential(input);
    }

    // Parse each chunk in parallel. Each chunk starts at a row boundary
    // (or byte 0), so the sequential parser produces correct results per chunk.
    let chunks: Vec<&[u8]> = splits.windows(2).map(|w| &input[w[0]..w[1]]).collect();

    let chunk_results: Vec<Vec<Vec<Cow<'a, [u8]>>>> = chunks
        .par_iter()
        .map(|chunk| decode_bytes_sequential(chunk))
        .collect();

    let total_rows: usize = chunk_results.iter().map(|r| r.len()).sum();
    let mut result = Vec::with_capacity(total_rows);
    for chunk_rows in chunk_results {
        result.extend(chunk_rows);
    }
    result
}

/// Unescape a single NSV cell.
///
/// Returns `Cow::Borrowed` when no unescaping is needed.
pub fn unescape(s: &str) -> Cow<'_, str> {
    match unescape_bytes(s.as_bytes()) {
        // SAFETY: input was &str (valid UTF-8). Borrowed means no transformation,
        // so the result is the same valid UTF-8 slice.
        Cow::Borrowed(b) => Cow::Borrowed(unsafe { std::str::from_utf8_unchecked(b) }),
        // SAFETY: unescape only removes/replaces ASCII bytes — preserves UTF-8 validity.
        Cow::Owned(v) => Cow::Owned(unsafe { String::from_utf8_unchecked(v) }),
    }
}

/// Unescape a single raw cell (byte-level).
///
/// Interprets `\` as the empty cell token (returns empty vec).
/// `\\` → `\`, `\n` → LF. Unrecognized sequences pass through.
/// Dangling backslash at end is stripped.
///
/// Returns `Cow::Borrowed` when no unescaping is needed (no backslash present).
pub fn unescape_bytes(s: &[u8]) -> Cow<'_, [u8]> {
    if s == b"\\" {
        return Cow::Owned(Vec::new());
    }

    if !s.contains(&b'\\') {
        return Cow::Borrowed(s);
    }

    let mut out = Vec::with_capacity(s.len());
    let mut escaped = false;

    for &b in s {
        if escaped {
            match b {
                b'n' => out.push(b'\n'),
                b'\\' => out.push(b'\\'),
                _ => {
                    out.push(b'\\');
                    out.push(b);
                }
            }
            escaped = false;
        } else if b == b'\\' {
            escaped = true;
        } else {
            out.push(b);
        }
    }

    Cow::Owned(out)
}

/// Escape a single NSV cell.
///
/// Returns `Cow::Borrowed` when no escaping is needed.
pub fn escape(s: &str) -> Cow<'_, str> {
    match escape_bytes(s.as_bytes()) {
        // SAFETY: input was &str (valid UTF-8). Borrowed means no transformation,
        // so the result is the same valid UTF-8 slice.
        Cow::Borrowed(b) => Cow::Borrowed(unsafe { std::str::from_utf8_unchecked(b) }),
        // SAFETY: escape only inserts ASCII bytes (\, n) — preserves UTF-8 validity.
        Cow::Owned(v) => Cow::Owned(unsafe { String::from_utf8_unchecked(v) }),
    }
}

/// Escape a single raw cell (byte-level).
///
/// Empty input → `\` (empty cell token).
/// `\` → `\\`, LF → `\n`.
///
/// Returns `Cow::Borrowed` when no escaping is needed (non-empty, no `\` or LF).
pub fn escape_bytes(s: &[u8]) -> Cow<'_, [u8]> {
    if s.is_empty() {
        return Cow::Owned(b"\\".to_vec());
    }

    if s.contains(&b'\n') || s.contains(&b'\\') {
        let mut out = Vec::with_capacity(s.len() + s.len() / 4);
        for &b in s {
            match b {
                b'\\' => {
                    out.push(b'\\');
                    out.push(b'\\');
                }
                b'\n' => {
                    out.push(b'\\');
                    out.push(b'n');
                }
                _ => out.push(b),
            }
        }
        Cow::Owned(out)
    } else {
        Cow::Borrowed(s)
    }
}

// ── Projected (column-selective) parsing ─────────────────────────────
//
// Single-pass scan that tracks the column index, skips non-projected
// columns entirely (no allocation, no unescape), and directly produces
// the final `Vec<Vec<Vec<u8>>>`.

/// Build a column-map: `col_map[original_col] = projected_index`.
/// Entries for non-projected columns are `usize::MAX`.
fn build_col_map(columns: &[usize]) -> (Vec<usize>, usize) {
    let max_col = columns.iter().copied().max().unwrap_or(0);
    let mut col_map = vec![usize::MAX; max_col + 1];
    for (proj_idx, &orig_col) in columns.iter().enumerate() {
        col_map[orig_col] = proj_idx;
    }
    (col_map, max_col)
}

/// Decode only the specified columns from raw bytes.
///
/// Single-pass: scans for cell/row boundaries and directly unescapes
/// only the cells in projected columns.  No intermediate structural index.
/// Each inner vec has exactly `columns.len()` entries (same order as `columns`).
///
/// Cells are returned as `Cow<[u8]>` — borrowed when no unescaping was needed.
pub fn decode_bytes_projected<'a>(input: &'a [u8], columns: &[usize]) -> Vec<Vec<Cow<'a, [u8]>>> {
    if input.is_empty() || columns.is_empty() {
        return Vec::new();
    }

    #[cfg(feature = "rayon")]
    if input.len() >= PARALLEL_THRESHOLD {
        return decode_projected_parallel(input, columns);
    }

    decode_projected_sequential(input, columns)
}

/// Sequential single-pass projected decode.
fn decode_projected_sequential<'a>(input: &'a [u8], columns: &[usize]) -> Vec<Vec<Cow<'a, [u8]>>> {
    let (col_map, max_col) = build_col_map(columns);
    let stride = columns.len();
    let mut data: Vec<Vec<Cow<'a, [u8]>>> = Vec::new();
    let mut row: Vec<Cow<'a, [u8]>> = vec![Cow::Borrowed(b""); stride];
    let mut col_idx: usize = 0;
    let mut start = 0;
    let mut row_has_cells = false;

    for (pos, &b) in input.iter().enumerate() {
        if b == b'\n' {
            if pos > start {
                if col_idx <= max_col {
                    if let Some(&proj_idx) = col_map.get(col_idx) {
                        if proj_idx != usize::MAX {
                            row[proj_idx] = unescape_bytes(&input[start..pos]);
                        }
                    }
                }
                col_idx += 1;
                row_has_cells = true;
            } else {
                if row_has_cells || !data.is_empty() || col_idx == 0 {
                    data.push(row);
                    row = vec![Cow::Borrowed(b""); stride];
                }
                col_idx = 0;
                row_has_cells = false;
            }
            start = pos + 1;
        }
    }

    if start < input.len() {
        if col_idx <= max_col {
            if let Some(&proj_idx) = col_map.get(col_idx) {
                if proj_idx != usize::MAX {
                    row[proj_idx] = unescape_bytes(&input[start..]);
                }
            }
        }
        row_has_cells = true;
    }

    if row_has_cells {
        data.push(row);
    }

    data
}

#[cfg(feature = "rayon")]
/// Parallel single-pass projected decode.
fn decode_projected_parallel<'a>(input: &'a [u8], columns: &[usize]) -> Vec<Vec<Cow<'a, [u8]>>> {
    let num_threads = rayon::current_num_threads();
    let chunk_size = input.len() / num_threads;

    if chunk_size == 0 {
        return decode_projected_sequential(input, columns);
    }

    let finder = memmem::Finder::new(b"\n\n");
    let mut splits = Vec::with_capacity(num_threads + 1);
    splits.push(0usize);

    for i in 1..num_threads {
        let nominal = i * chunk_size;
        if let Some(offset) = finder.find(&input[nominal..]) {
            let split = nominal + offset + 2;
            if split < input.len() {
                splits.push(split);
            }
        }
    }
    splits.push(input.len());
    splits.dedup();

    if splits.len() <= 2 {
        return decode_projected_sequential(input, columns);
    }

    let chunks: Vec<&[u8]> = splits.windows(2).map(|w| &input[w[0]..w[1]]).collect();

    let chunk_results: Vec<Vec<Vec<Cow<'a, [u8]>>>> = chunks
        .par_iter()
        .map(|chunk| decode_projected_sequential(chunk, columns))
        .collect();

    let total_rows: usize = chunk_results.iter().map(|r| r.len()).sum();
    let mut result = Vec::with_capacity(total_rows);
    for chunk_rows in chunk_results {
        result.extend(chunk_rows);
    }
    result
}

/// Encode a seqseq into an NSV string.
pub fn encode(data: &[Vec<String>]) -> String {
    let mut result = Vec::new();

    for row in data {
        for cell in row {
            result.extend_from_slice(&escape_bytes(cell.as_bytes()));
            result.push(b'\n');
        }
        result.push(b'\n');
    }

    // Safety: encoding only inserts ASCII bytes (\, n, LF) — preserves UTF-8.
    String::from_utf8(result).unwrap()
}

/// Encode a seqseq of byte vectors into raw NSV bytes.
pub fn encode_bytes(data: &[Vec<Vec<u8>>]) -> Vec<u8> {
    let mut result = Vec::new();

    for row in data {
        for cell in row {
            result.extend_from_slice(&escape_bytes(cell));
            result.push(b'\n');
        }
        result.push(b'\n');
    }

    result
}

/// A single warning found during validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Warning {
    pub kind: WarningKind,
    pub pos: usize,
    pub line: usize,
    pub col: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WarningKind {
    /// `\` followed by a byte other than `n` or `\`
    UnknownEscape(u8),
    /// `\` immediately before LF or at EOF
    DanglingBackslash,
    /// Non-empty input not ending with LF
    NoTerminalLf,
}

/// Report edge cases in raw NSV input without altering parsing behavior.
///
/// Warns on unknown escape sequences, dangling backslashes, and missing terminal LF.
/// Positions are byte offsets; line and col are 1-indexed.
pub fn check(input: &[u8]) -> Vec<Warning> {
    if input.is_empty() {
        return Vec::new();
    }

    let mut warnings = Vec::new();
    let len = input.len();
    let mut line: usize = 1;
    let mut line_start: usize = 0;
    let mut escaped = false;

    for (i, &b) in input.iter().enumerate() {
        if escaped {
            match b {
                b'n' | b'\\' => {}
                b'\n' => warnings.push(Warning {
                    kind: WarningKind::DanglingBackslash,
                    pos: i - 1,
                    line,
                    col: i - line_start,
                }),
                _ => warnings.push(Warning {
                    kind: WarningKind::UnknownEscape(b),
                    pos: i - 1,
                    line,
                    col: i - line_start,
                }),
            }
            escaped = false;
        } else if b == b'\\' {
            escaped = true;
        }
        if b == b'\n' {
            line += 1;
            line_start = i + 1;
        }
    }

    if escaped {
        warnings.push(Warning {
            kind: WarningKind::DanglingBackslash,
            pos: len - 1,
            line,
            col: len - line_start,
        });
    }

    if line_start != len {
        warnings.push(Warning {
            kind: WarningKind::NoTerminalLf,
            pos: len,
            line,
            col: len - line_start + 1,
        });
    }

    warnings
}

// ── Streaming ────────────────────────────────────────────────────────

/// Streaming NSV reader. Yields one complete row of byte vectors at a time.
///
/// On EOF, returns `Ok(None)` without discarding buffered state — calling
/// `next_row()` again after more data arrives resumes where it left off.
pub struct Reader<R> {
    inner: io::BufReader<R>,
    line_buf: Vec<u8>,
    row: Vec<Vec<u8>>,
}

impl<R: io::Read> Reader<R> {
    pub fn new(reader: R) -> Self {
        Self::from_buf_reader(io::BufReader::new(reader))
    }

    pub fn from_buf_reader(reader: io::BufReader<R>) -> Self {
        Reader { inner: reader, line_buf: Vec::new(), row: Vec::new() }
    }

    pub fn next_row(&mut self) -> io::Result<Option<Vec<Vec<u8>>>> {
        let mut byte = [0u8; 1];
        loop {
            match self.inner.read(&mut byte) {
                Ok(0) => return Ok(None),
                Err(e) => return Err(e),
                Ok(_) if byte[0] != b'\n' => self.line_buf.push(byte[0]),
                Ok(_) if self.line_buf.is_empty() => return Ok(Some(std::mem::take(&mut self.row))),
                Ok(_) => {
                    self.row.push(unescape_bytes(&self.line_buf).into_owned());
                    self.line_buf.clear();
                }
            }
        }
    }

    /// Completed cells of the row currently being assembled.
    pub fn partial_row(&self) -> &[Vec<u8>] {
        &self.row
    }

    /// Bytes accumulated for the cell currently being read (not yet unescaped).
    pub fn partial_cell(&self) -> &[u8] {
        &self.line_buf
    }

    /// Recover the inner `BufReader`.
    pub fn into_inner(self) -> io::BufReader<R> {
        self.inner
    }
}

impl<R: io::Read> Iterator for Reader<R> {
    type Item = io::Result<Vec<Vec<u8>>>;
    fn next(&mut self) -> Option<Self::Item> {
        self.next_row().transpose()
    }
}

/// Streaming NSV writer. Wraps any `W: Write` and writes one row at a time.
///
/// No internal buffering — wrap the inner writer in `BufWriter` if needed.
pub struct Writer<W> {
    inner: W,
}

impl<W: Write> Writer<W> {
    pub fn new(writer: W) -> Self {
        Writer { inner: writer }
    }

    /// Write a single complete row. Each cell is escaped and `\n`-terminated;
    /// an extra `\n` terminates the row.
    ///
    /// Accepts any cell type that implements `AsRef<[u8]>`: `&[u8]`, `Vec<u8>`,
    /// `&str`, `String`, etc.
    pub fn write_row<C: AsRef<[u8]>>(&mut self, row: &[C]) -> io::Result<()> {
        for cell in row {
            self.inner.write_all(&escape_bytes(cell.as_ref()))?;
            self.inner.write_all(b"\n")?;
        }
        self.inner.write_all(b"\n")
    }

    /// Recover the inner writer.
    pub fn into_inner(self) -> W {
        self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Convert Cow cells to owned for comparison with pre-built Vec<Vec<Vec<u8>>> data.
    fn owned(rows: Vec<Vec<Cow<[u8]>>>) -> Vec<Vec<Vec<u8>>> {
        rows.into_iter()
            .map(|row| row.into_iter().map(|c| c.into_owned()).collect())
            .collect()
    }

    #[test]
    fn test_simple_table() {
        let nsv = "col1\ncol2\n\na\nb\n\nc\nd\n";
        let result = decode(nsv);
        assert_eq!(
            result,
            vec![
                vec!["col1".to_string(), "col2".to_string()],
                vec!["a".to_string(), "b".to_string()],
                vec!["c".to_string(), "d".to_string()],
            ]
        );
    }

    #[test]
    fn test_empty_fields() {
        let nsv = "a\n\\\nb\n\n\\\nc\n\\\n";
        let result = decode(nsv);
        assert_eq!(
            result,
            vec![
                vec!["a".to_string(), "".to_string(), "b".to_string()],
                vec!["".to_string(), "c".to_string(), "".to_string()],
            ]
        );
    }

    #[test]
    fn test_escape_sequences() {
        let nsv = "Line 1\\nLine 2\nBackslash: \\\\\nNot a newline: \\\\n\n";
        let result = decode(nsv);
        assert_eq!(
            result,
            vec![vec![
                "Line 1\nLine 2".to_string(),
                "Backslash: \\".to_string(),
                "Not a newline: \\n".to_string()
            ],]
        );
    }

    #[test]
    fn test_empty_rows() {
        let nsv = "first\n\n\n\nsecond\n";
        let result = decode(nsv);
        assert_eq!(
            result,
            vec![
                vec!["first".to_string()],
                vec![],
                vec![],
                vec!["second".to_string()],
            ]
        );
    }

    #[test]
    fn test_multiple_empty_rows() {
        let nsv = "a\n\n\n\n\nb\n";
        let result = decode(nsv);
        assert_eq!(
            result,
            vec![
                vec!["a".to_string()],
                vec![],
                vec![],
                vec![],
                vec!["b".to_string()],
            ]
        );
    }

    #[test]
    fn test_roundtrip() {
        let original = vec![
            vec!["col1".to_string(), "col2".to_string()],
            vec!["a".to_string(), "b".to_string()],
            vec!["".to_string(), "value\\with\\backslash".to_string()],
            vec!["multi\nline".to_string(), "normal".to_string()],
        ];

        let encoded = encode(&original);
        let decoded = decode(&encoded);
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_unrecognized_escape() {
        let nsv = "\\x41\\t\\r\n";
        let result = decode(nsv);
        assert_eq!(result, vec![vec!["\\x41\\t\\r".to_string()],]);
    }

    #[test]
    fn test_dangling_backslash() {
        let nsv = "text\\\n";
        let result = decode(nsv);
        assert_eq!(result, vec![vec!["text".to_string()],]);
    }

    #[test]
    fn test_empty_input() {
        let result = decode("");
        assert_eq!(result, Vec::<Vec<String>>::new());
    }

    #[test]
    fn test_no_trailing_newline() {
        let nsv = "a\nb";
        let result = decode(nsv);
        assert_eq!(result, vec![vec!["a".to_string(), "b".to_string()],]);
    }

    #[test]
    fn test_only_empty_rows() {
        let nsv = "\n\n\n\n";
        let result = decode(nsv);
        assert_eq!(
            result,
            vec![
                Vec::<String>::new(),
                Vec::<String>::new(),
                Vec::<String>::new(),
                Vec::<String>::new(),
            ]
        );
    }

    #[test]
    fn test_starts_with_empty_row() {
        let nsv = "\n\nfirst\n";
        let result = decode(nsv);
        assert_eq!(
            result,
            vec![
                Vec::<String>::new(),
                Vec::<String>::new(),
                vec!["first".to_string()],
            ]
        );
    }

    #[cfg(feature = "rayon")]
    #[test]
    fn test_large_file() {
        // Generate ~10MB of data to verify parallel path is exercised
        // (needs to exceed PARALLEL_THRESHOLD of 64KB)
        let large_data: Vec<Vec<String>> = (0..100_000)
            .map(|i| vec![format!("row{}", i), format!("data{}", i)])
            .collect();

        let encoded = encode(&large_data);

        // Verify it's large enough to trigger parallel parsing
        assert!(encoded.len() > PARALLEL_THRESHOLD);

        let decoded = decode(&encoded);
        assert_eq!(large_data, decoded);
    }

    #[cfg(feature = "rayon")]
    #[test]
    fn test_parallel_with_empty_rows() {
        // Test parallel path with empty rows mixed in
        let mut data = Vec::new();

        // Create enough data to exceed 64KB threshold
        for i in 0..10_000 {
            data.push(vec![format!("value{}", i)]);

            // Add empty row every 100 rows
            if i % 100 == 0 {
                data.push(vec![]);
            }
        }

        let encoded = encode(&data);
        assert!(encoded.len() > PARALLEL_THRESHOLD);

        let decoded = decode(&encoded);
        assert_eq!(data, decoded);
    }

    #[cfg(feature = "rayon")]
    #[test]
    fn test_parallel_with_escape_sequences() {
        // Test parallel path with cells containing escape sequences
        let mut data = Vec::new();

        for i in 0..10_000 {
            data.push(vec![
                format!("Line 1\nLine 2 {}", i),
                format!("Backslash: \\ {}", i),
                "".to_string(),
            ]);
        }

        let encoded = encode(&data);
        assert!(encoded.len() > PARALLEL_THRESHOLD);

        let decoded = decode(&encoded);
        assert_eq!(data, decoded);
    }

    // ── Byte-level tests ──

    #[test]
    fn test_bytes_roundtrip() {
        let original: Vec<Vec<Vec<u8>>> = vec![
            vec![b"col1".to_vec(), b"col2".to_vec()],
            vec![b"a".to_vec(), b"b".to_vec()],
            vec![b"".to_vec(), b"value\\with\\backslash".as_slice().to_vec()],
            vec![b"multi\nline".to_vec(), b"normal".to_vec()],
        ];

        let encoded = encode_bytes(&original);
        let decoded = owned(decode_bytes(&encoded));
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_bytes_non_utf8() {
        // Latin-1 bytes with values > 0x7F — not valid UTF-8
        let cell1: Vec<u8> = vec![0xC0, 0xE9, 0xF1]; // àéñ in Latin-1
        let cell2: Vec<u8> = vec![0xFF, 0xFE, 0x80]; // arbitrary high bytes
        let original: Vec<Vec<Vec<u8>>> = vec![vec![cell1.clone(), cell2.clone()]];

        let encoded = encode_bytes(&original);
        let decoded = owned(decode_bytes(&encoded));
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_bytes_empty_cells_and_rows() {
        let original: Vec<Vec<Vec<u8>>> = vec![
            vec![b"a".to_vec(), b"".to_vec(), b"b".to_vec()],
            vec![],
            vec![b"".to_vec()],
        ];

        let encoded = encode_bytes(&original);
        let decoded = owned(decode_bytes(&encoded));
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_bytes_with_special_ascii_bytes() {
        // Verify that only 0x0A (LF), 0x5C (\), 0x6E (n) are structurally significant
        let cell: Vec<u8> = (0u8..=255)
            .filter(|&b| b != b'\n' && b != b'\\')
            .collect();
        let original = vec![vec![cell.clone()]];

        let encoded = encode_bytes(&original);
        let decoded = owned(decode_bytes(&encoded));
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_decode_bytes_matches_decode_str() {
        // Confirm decode(s) and decode_bytes(s.as_bytes()) produce equivalent structures
        let inputs = vec![
            "col1\ncol2\n\na\nb\n\nc\nd\n",
            "a\n\\\nb\n\n\\\nc\n\\\n",
            "Line 1\\nLine 2\nBackslash: \\\\\n",
            "first\n\n\n\nsecond\n",
            "",
            "\n\n\n\n",
        ];

        for input in inputs {
            let str_result = decode(input);
            let byte_result = decode_bytes(input.as_bytes());

            // Convert byte result to strings for comparison
            let byte_as_str: Vec<Vec<String>> = byte_result
                .into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(|cell| String::from_utf8(cell.into_owned()).unwrap())
                        .collect()
                })
                .collect();

            assert_eq!(str_result, byte_as_str, "mismatch for input: {:?}", input);
        }
    }

    #[cfg(feature = "rayon")]
    #[test]
    fn test_bytes_large_parallel() {
        // Generate enough data to trigger parallel path
        let large_data: Vec<Vec<Vec<u8>>> = (0..100_000)
            .map(|i| {
                vec![
                    format!("row{}", i).into_bytes(),
                    format!("data{}", i).into_bytes(),
                ]
            })
            .collect();

        let encoded = encode_bytes(&large_data);
        assert!(encoded.len() > PARALLEL_THRESHOLD);

        let decoded = owned(decode_bytes(&encoded));
        assert_eq!(large_data, decoded);
    }

    // ── check() tests ──

    #[test]
    fn test_check_empty_input() {
        assert_eq!(check(b""), vec![]);
    }

    #[test]
    fn test_check_just_lf() {
        assert_eq!(check(b"\n"), vec![]);
    }

    #[test]
    fn test_check_no_issues() {
        assert_eq!(check(b"col1\ncol2\n\na\nb\n\n"), vec![]);
        assert_eq!(check(b"hello\\\\world\n\\n\n\n"), vec![]);
    }

    #[test]
    fn test_check_single_unknown_escape() {
        let warnings = check(b"hello\\tworld\n");
        assert_eq!(
            warnings,
            vec![Warning {
                kind: WarningKind::UnknownEscape(b't'),
                pos: 5,
                line: 1,
                col: 6,
            }]
        );
    }

    #[test]
    fn test_check_multiple_unknown_escapes_different_lines() {
        let warnings = check(b"\\thello\n\\rworld\n\n");
        assert_eq!(
            warnings,
            vec![
                Warning {
                    kind: WarningKind::UnknownEscape(b't'),
                    pos: 0,
                    line: 1,
                    col: 1,
                },
                Warning {
                    kind: WarningKind::UnknownEscape(b'r'),
                    pos: 8,
                    line: 2,
                    col: 1,
                },
            ]
        );
    }

    #[test]
    fn test_check_dangling_backslash_mid_file() {
        let warnings = check(b"text\\\nmore\n\n");
        assert_eq!(
            warnings,
            vec![Warning {
                kind: WarningKind::DanglingBackslash,
                pos: 4,
                line: 1,
                col: 5,
            }]
        );
    }

    #[test]
    fn test_check_dangling_backslash_at_eof() {
        let warnings = check(b"text\\");
        assert_eq!(
            warnings,
            vec![
                Warning {
                    kind: WarningKind::DanglingBackslash,
                    pos: 4,
                    line: 1,
                    col: 5,
                },
                Warning {
                    kind: WarningKind::NoTerminalLf,
                    pos: 5,
                    line: 1,
                    col: 6,
                },
            ]
        );
    }

    #[test]
    fn test_check_no_terminal_lf() {
        let warnings = check(b"hello");
        assert_eq!(
            warnings,
            vec![Warning {
                kind: WarningKind::NoTerminalLf,
                pos: 5,
                line: 1,
                col: 6,
            }]
        );
    }

    #[test]
    fn test_check_combination() {
        // \(0) t(1) h(2) e(3) l(4) l(5) o(6) \(7) LF(8) w(9) o(10) r(11) l(12) d(13)
        let warnings = check(b"\\thello\\\nworld");
        assert_eq!(
            warnings,
            vec![
                Warning {
                    kind: WarningKind::UnknownEscape(b't'),
                    pos: 0,
                    line: 1,
                    col: 1,
                },
                Warning {
                    kind: WarningKind::DanglingBackslash,
                    pos: 7,
                    line: 1,
                    col: 8,
                },
                Warning {
                    kind: WarningKind::NoTerminalLf,
                    pos: 14,
                    line: 2,
                    col: 6,
                },
            ]
        );
    }

    #[test]
    fn test_check_non_utf8() {
        // Non-UTF-8 bytes with a bad escape: 0xFF 0xFE \t 0x80 LF
        let input: &[u8] = &[0xFF, 0xFE, b'\\', b't', 0x80, b'\n'];
        let warnings = check(input);
        assert_eq!(
            warnings,
            vec![Warning {
                kind: WarningKind::UnknownEscape(b't'),
                pos: 2,
                line: 1,
                col: 3,
            }]
        );
    }

    // ── Projected decode tests ──

    #[test]
    fn test_project_subset() {
        let nsv = b"c0\nc1\nc2\nc3\n\na\nb\nc\nd\n\ne\nf\ng\nh\n\n";
        let projected = owned(decode_bytes_projected(nsv, &[0, 2]));
        assert_eq!(projected.len(), 3);
        assert_eq!(projected[0], vec![b"c0".to_vec(), b"c2".to_vec()]);
        assert_eq!(projected[1], vec![b"a".to_vec(), b"c".to_vec()]);
        assert_eq!(projected[2], vec![b"e".to_vec(), b"g".to_vec()]);
    }

    #[test]
    fn test_project_single_column() {
        let nsv = b"name\nage\nsalary\n\nAlice\n30\n50000\n\nBob\n25\n75000\n\n";
        let projected = owned(decode_bytes_projected(nsv, &[1]));
        assert_eq!(projected.len(), 3);
        assert_eq!(projected[0], vec![b"age".to_vec()]);
        assert_eq!(projected[1], vec![b"30".to_vec()]);
        assert_eq!(projected[2], vec![b"25".to_vec()]);
    }

    #[test]
    fn test_project_reorder() {
        let nsv = b"a\nb\nc\n\n1\n2\n3\n\n";
        let projected = owned(decode_bytes_projected(nsv, &[2, 0]));
        assert_eq!(projected[0], vec![b"c".to_vec(), b"a".to_vec()]);
        assert_eq!(projected[1], vec![b"3".to_vec(), b"1".to_vec()]);
    }

    #[test]
    fn test_project_out_of_range() {
        let nsv = b"a\nb\n\n";
        let projected = owned(decode_bytes_projected(nsv, &[0, 5]));
        assert_eq!(projected[0], vec![b"a".to_vec(), b"".to_vec()]);
    }

    #[test]
    fn test_projected_matches_full() {
        let nsv = b"c0\nc1\nc2\n\na\nb\nc\n\n";
        let full = owned(decode_bytes(nsv));
        let projected = owned(decode_bytes_projected(nsv, &[0, 1, 2]));
        assert_eq!(projected, full);
    }

    #[cfg(feature = "rayon")]
    #[test]
    fn test_project_with_escapes_parallel() {
        let mut data = Vec::new();
        for i in 0..10_000 {
            data.push(vec![
                format!("Line 1\nLine 2 {}", i),
                format!("Backslash: \\ {}", i),
                format!("plain{}", i),
            ]);
        }
        let encoded = encode(&data);
        let encoded_bytes = encoded.as_bytes();
        assert!(encoded_bytes.len() > PARALLEL_THRESHOLD);

        let projected = decode_bytes_projected(encoded_bytes, &[2]);
        assert_eq!(projected.len(), data.len());
        for (ri, row) in data.iter().enumerate() {
            assert_eq!(
                String::from_utf8(projected[ri][0].to_vec()).unwrap(),
                row[2]
            );
        }

        let full = owned(decode_bytes(encoded_bytes));
        let projected_all = owned(decode_bytes_projected(encoded_bytes, &[0, 1, 2]));
        assert_eq!(projected_all, full);
    }

    // ── Streaming tests ──

    use std::io::Cursor;

    #[test]
    fn test_bytes_reader_matches_batch() {
        for input in [
            &b"a\nb\n\nc\nd\n\n"[..],
            b"a\n\\\nb\n\n\\\nc\n\\\n\n",          // empty cells
            b"Line 1\\nLine 2\n\\\\\n\\\\n\n\n",    // escapes
            b"first\n\n\n\nsecond\n\n",              // consecutive empty rows
            b"\\\n\\\n\\\n\n",                       // only empty cells
            b"\n\n\n\n",                             // only empty rows
            b"",
        ] {
            let streaming: Vec<_> = Reader::new(Cursor::new(input))
                .map(|r| r.unwrap())
                .collect();
            assert_eq!(streaming, owned(decode_bytes(input)), "input: {:?}", input);
        }
    }

    #[test]
    fn test_bytes_reader_incomplete_row_not_emitted() {
        let mut r = Reader::new(Cursor::new(&b"a\nb\n\nc\nd"[..]));
        assert_eq!(r.next_row().unwrap(), Some(vec![b"a".to_vec(), b"b".to_vec()]));
        assert_eq!(r.next_row().unwrap(), None); // "c\nd" buffered, not emitted
    }

    // ── Resumable ──

    use std::cell::RefCell;

    struct GrowableStream(RefCell<(Vec<u8>, usize)>);
    impl GrowableStream {
        fn new() -> Self { GrowableStream(RefCell::new((Vec::new(), 0))) }
        fn append(&self, b: &[u8]) { self.0.borrow_mut().0.extend_from_slice(b); }
    }
    impl io::Read for &GrowableStream {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            let mut s = self.0.borrow_mut();
            let n = buf.len().min(s.0.len() - s.1);
            buf[..n].copy_from_slice(&s.0[s.1..s.1 + n]);
            s.1 += n;
            Ok(n)
        }
    }

    #[test]
    fn test_bytes_reader_resumable() {
        let s = GrowableStream::new();
        let mut r = Reader::new(&s);

        // Partial row, then complete it
        s.append(b"a\nb\n\nc\n");
        assert_eq!(r.next_row().unwrap(), Some(vec![b"a".to_vec(), b"b".to_vec()]));
        assert_eq!(r.next_row().unwrap(), None);
        s.append(b"d\n\n");
        assert_eq!(r.next_row().unwrap(), Some(vec![b"c".to_vec(), b"d".to_vec()]));

        // Mid-line split
        s.append(b"hel");
        assert_eq!(r.next_row().unwrap(), None);
        s.append(b"lo\n\n");
        assert_eq!(r.next_row().unwrap(), Some(vec![b"hello".to_vec()]));
    }

    // ── Reader ──

    #[test]
    fn test_bytes_reader() {
        for input in [
            &b"col1\ncol2\n\na\nb\n\nc\nd\n\n"[..],
            b"\n\n\n\n", b"", b"text\\\n\n",
        ] {
            let streaming: Vec<_> = Reader::new(Cursor::new(input))
                .map(|r| r.unwrap())
                .collect();
            assert_eq!(streaming, owned(decode_bytes(input)));
        }
        // Non-UTF-8 round-trip
        let orig = vec![vec![vec![0xC0, 0xE9], vec![0xFF, 0xFE]]];
        let enc = encode_bytes(&orig);
        let dec: Vec<_> = Reader::new(Cursor::new(&enc[..]))
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(dec, orig);
    }

    // ── Writer ──

    #[test]
    fn test_writer() {
        let mut buf = Vec::new();
        let mut w = Writer::new(&mut buf);
        w.write_row(&["hello", "world"]).unwrap();
        assert_eq!(buf, b"hello\nworld\n\n");

        buf.clear();
        Writer::new(&mut buf).write_row(&["line1\nline2", "back\\slash"]).unwrap();
        assert_eq!(buf, b"line1\\nline2\nback\\\\slash\n\n");

        buf.clear();
        Writer::new(&mut buf).write_row(&["", "", ""]).unwrap();
        assert_eq!(buf, b"\\\n\\\n\\\n\n");

        buf.clear();
        let empty: &[&str] = &[];
        Writer::new(&mut buf).write_row(empty).unwrap();
        assert_eq!(buf, b"\n");
    }

    #[test]
    fn test_writer_matches_batch_encode() {
        let data = vec![
            vec!["a".to_string(), "b".to_string()],
            vec!["".to_string()],
            vec!["line\none".to_string(), "back\\slash".to_string()],
        ];
        let mut buf = Vec::new();
        {
            let mut w = Writer::new(&mut buf);
            for row in &data { w.write_row(row).unwrap(); }
        }
        assert_eq!(buf, encode(&data).as_bytes());
    }

    // ── Round-trip ──

    #[test]
    fn test_roundtrip_streaming() {
        let original: Vec<Vec<Vec<u8>>> = vec![
            vec![b"a".to_vec(), b"b".to_vec()],
            vec![b"".to_vec(), b"val\\ue".to_vec()],
            vec![b"multi\nline".to_vec(), b"normal".to_vec()],
            vec![],
        ];
        let mut buf = Vec::new();
        {
            let mut w = Writer::new(&mut buf);
            for row in &original {
                let refs: Vec<&[u8]> = row.iter().map(|c| c.as_slice()).collect();
                w.write_row(&refs).unwrap();
            }
        }
        let decoded: Vec<_> = Reader::new(Cursor::new(&buf[..]))
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(decoded, original);
    }
}
