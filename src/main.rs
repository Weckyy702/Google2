use crossbeam_channel::Receiver;
use crossbeam_utils::thread;
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use std::{
    collections::{HashMap, VecDeque},
    error::Error,
    fs::{self, File},
    io::{BufReader, BufWriter, Read, Write},
    iter::Iterator,
    path::{Path, PathBuf},
    time::Instant,
};

const MAN_PATHS: [&str; 3] = ["/usr/local/man", "/usr/local/share/man", "/usr/share/man"];

const MAGIC_BYTES: [u8; 5] = [0x49, 0x6e, 0x64, 0x65, 0x78];

const NUM_THREADS: usize = 4;

type ParsingFunction = fn(BufReader<File>) -> Option<Vec<char>>;
type ParsersPerFileType = HashMap<String, ParsingFunction>;

#[derive(Default, Debug, PartialEq, Eq)]
struct Document {
    word_count: usize,
    document_frequencies: HashMap<String, usize>,
}

impl Document {
    fn add_word(&mut self, word: String) {
        if let Some(count) = self.document_frequencies.get_mut(&word) {
            *count += 1;
        } else {
            self.document_frequencies.insert(word, 1);
        }
        self.word_count += 1
    }
}

struct Lexer<'a> {
    content: &'a [char],
}

impl<'a> Lexer<'a> {
    fn new(content: &'a [char]) -> Self {
        Self { content }
    }

    fn is_word_part(c: char) -> bool {
        !Self::is_ignored(c)
    }

    fn is_ignored(c: char) -> bool {
        const IGNORED_CHARS: [char; 19] = [
            ',', '(', ')', '[', ']', '{', '}', '/', '+', '<', '>', '$', '^', ':', '!', '=', '"',
            '#', '"',
        ];

        if c.is_whitespace() {
            return true;
        }

        IGNORED_CHARS.contains(&c)
    }
}

impl Lexer<'_> {
    fn next_token(&mut self) -> Option<String> {
        if self.content.is_empty() {
            return None;
        }

        self.trim();

        if Self::is_word_part(self.content[0]) {
            return self.collect_while(Self::is_word_part);
        }

        if self.content[0].is_numeric() {
            return self.collect_while(char::is_numeric);
        }

        self.collect(1)
    }

    fn trim(&mut self) {
        if self.content.is_empty() {
            return;
        }

        self.collect_while(Self::is_ignored);
    }

    fn collect_while<P>(&mut self, mut predicate: P) -> Option<String>
    where
        P: FnMut(char) -> bool,
    {
        let mut n = 0;
        while n < self.content.len() && predicate(self.content[n]) {
            n += 1;
        }
        self.collect(n)
    }

    fn collect(&mut self, n: usize) -> Option<String> {
        if n >= self.content.len() {
            return None;
        }

        let result = self
            .content
            .iter()
            .take(n)
            .collect::<String>()
            .to_lowercase();
        self.content = &self.content[n..];

        Some(result)
    }
}

impl Iterator for Lexer<'_> {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_token()
    }
}

type DocumentIndex = HashMap<PathBuf, Document>;

fn log_err<E: Error>(e: E) {
    eprintln!("Error: {e}");
}

fn log_fatal<E: Error>(e: E) -> ! {
    eprintln!("FATAL: {e}");
    std::process::exit(42069)
}

// File readers. These read an optional string from a (possibly compressed) file

fn read_gzip(file: BufReader<File>) -> Option<Vec<char>> {
    let mut reader = BufReader::new(GzDecoder::new(file));

    let mut buf = String::new();
    reader.read_to_string(&mut buf).map_err(log_err).ok()?;

    Some(buf.chars().collect())
}

// Document parsers. These output actual documents

fn parse_document(content: Vec<char>) -> Option<Document> {
    const IGNORED_WORDS: [&str; 28] = [
        "\"", ".TP", ".TH", ".RI", ".hy", ".", ".sp", ";", ".nf", ".RE", ".TE", ".IR", ".BR", ".f",
        "'\\\"", ".BI", ".nh", ".PP", ".\\\"", ".SH", ".B", ".RS", ".ad", ".TS", "*\"", ".nf",
        "\\", ".I",
    ];

    let mut doc = Document::default();

    for word in Lexer::new(&content) {
        let word = word.strip_suffix('.').unwrap_or(&word[..]);
        if !IGNORED_WORDS.contains(&word) {
            doc.add_word(word.into());
        }
    }

    Some(doc)
}

fn parse_files_threaded(
    receiver: Receiver<Option<PathBuf>>,
    parsers: &ParsersPerFileType,
) -> Option<DocumentIndex> {
    let mut documents = DocumentIndex::new();

    while let Ok(Some(file_path)) = receiver.recv() {
        let Some(extension) = file_path.as_path().extension() else {
                eprintln!("ERROR: File {file_path:?} has no extension!");
                continue;
            };

        let Some(extension) = extension.to_str().map(|str| str.to_string()) else {
                eprintln!("ERROR: Extension {extension:?} is not valid unicode!");
                continue;
            };

        let Some(reader) = parsers.get(&extension) else {
                eprintln!("ERROR: File extension {extension:?} has no associated parser!");
                continue;
            };

        let file = BufReader::new(File::open(&file_path).map_err(log_err).ok()?);

        let Some(content) = reader(file) else {
                continue;
            };

        if let Some(document) = parse_document(content) {
            documents.insert(file_path, document);
        }
    }

    Some(documents)
}

fn scan_directories(start_paths: &[&str]) -> Option<DocumentIndex> {
    let (sender, reciever) = crossbeam_channel::bounded(NUM_THREADS * 2);

    let parsers: [(String, ParsingFunction); 1] = [("gz".into(), read_gzip)];
    let parsers = ParsersPerFileType::from_iter(parsers);

    let mut files = start_paths
        .iter()
        .map(|s| PathBuf::from(s))
        .collect::<VecDeque<_>>();

    let index = thread::scope(|s| {
        let handles = (0..NUM_THREADS)
            .map(|_| s.spawn(|_| parse_files_threaded(reciever.clone(), &parsers)))
            .collect::<Vec<_>>();

        while let Some(path) = files.pop_front() {
            for entry in fs::read_dir(path).map_err(log_err).ok()? {
                let entry = entry.map_err(log_fatal).map_err(log_err).ok()?;
                if entry.file_type().map_err(log_fatal).ok()?.is_dir() {
                    files.push_back(entry.path());
                    continue;
                }
                sender.send(Some(entry.path())).map_err(log_fatal).unwrap();
            }
        }

        for _ in 0..NUM_THREADS {
            sender.send(None).map_err(log_fatal).unwrap();
        }

        Some(
            handles
                .into_iter()
                .flat_map(|h| h.join())
                .flatten()
                .flatten()
                .collect::<DocumentIndex>(),
        )
    })
    .expect("Can spawn threads")?;

    println!("Scanned {} files", index.len());
    Some(index)
}

fn write_size<W: Write>(writer: &mut W, value: usize) -> Result<(), ()> {
    writer.write_all(&value.to_be_bytes()).map_err(log_err)?;
    Ok(())
}

fn write_str<W: Write>(writer: &mut W, path: &str) -> Result<(), ()> {
    let bytes = path.as_bytes();

    //Need to write the byte size so we can correctly get it out of the file later
    write_size(writer, bytes.len())?;
    writer.write_all(&bytes).map_err(log_err)?;

    Ok(())
}

fn write_document<W: Write>(writer: &mut W, document: &Document) -> Result<(), ()> {
    write_size(writer, document.word_count)?;
    write_size(writer, document.document_frequencies.len())?;

    for (word, count) in &document.document_frequencies {
        write_str(writer, word)?;
        write_size(writer, *count)?;
    }

    Ok(())
}

fn write_document_and_path<W: Write>(
    writer: &mut W,
    path: &str,
    document: &Document,
) -> Result<(), ()> {
    write_str(writer, path)?;
    write_document(writer, document)
}

fn write_index_to<W: Write>(writer: &mut W, index: &DocumentIndex) -> Result<(), ()> {
    // Magic byte
    writer.write_all(&MAGIC_BYTES).map_err(log_err)?;

    // Length of index
    write_size(writer, index.len())?;

    for (path, document) in index {
        write_document_and_path(writer, path.to_str().ok_or(())?, document)?
    }

    Ok(())
}

fn write_compressed_index(index: &DocumentIndex, output_path: &Path) -> Result<(), ()> {
    let mut writer = GzEncoder::new(
        BufWriter::new(File::create(output_path).map_err(log_err)?),
        Compression::default(),
    );

    write_index_to(&mut writer, index)
}

fn write_uncompressed_index(index: &DocumentIndex, output_path: &Path) -> Result<(), ()> {
    let mut writer = BufWriter::new(File::create(output_path).map_err(log_err)?);

    write_index_to(&mut writer, index)
}

fn read_bytes<R: Read, const N: usize>(reader: &mut R) -> Result<[u8; N], ()> {
    let mut buf = [0u8; N];
    reader.read_exact(&mut buf).map_err(log_err)?;

    Ok(buf)
}

fn read_size<R: Read>(reader: &mut R) -> Result<usize, ()> {
    let bytes = read_bytes(reader)?;

    Ok(usize::from_be_bytes(bytes))
}

fn read_str<R: Read>(reader: &mut R) -> Result<String, ()> {
    let size = read_size(reader)?;
    let mut bytes = vec![0; size];

    reader.read_exact(&mut bytes).map_err(log_err)?;
    String::from_utf8(bytes).map_err(log_err)
}

fn read_document<R: Read>(reader: &mut R) -> Result<Document, ()> {
    let word_count = read_size(reader)?;
    let map_size = read_size(reader)?;

    let mut words = HashMap::with_capacity(map_size);

    for _ in 0..map_size {
        let word = read_str(reader)?;
        let count = read_size(reader)?;

        words.insert(word, count);
    }

    Ok(Document {
        word_count,
        document_frequencies: words,
    })
}

fn read_document_and_path<R: Read>(reader: &mut R) -> Result<(PathBuf, Document), ()> {
    let path: PathBuf = read_str(reader)?.into();
    let document = read_document(reader)?;

    Ok((path, document))
}

fn load_index_from<R: Read>(reader: &mut R) -> Result<DocumentIndex, ()> {
    // Magic byte
    let magic_bytes = read_bytes(reader)?;
    if magic_bytes != MAGIC_BYTES {
        eprintln!("ERROR: incorrect magic bytes: {magic_bytes:?}");
        return Err(());
    }

    // Length of index
    let index_size = read_size(reader)?;
    let mut index = DocumentIndex::with_capacity(index_size);

    for _ in 0..index_size {
        let (path, document) = read_document_and_path(reader)?;
        index.insert(path, document);
    }

    Ok(index)
}

fn load_compressed_index(path: &Path) -> Result<DocumentIndex, ()> {
    let mut reader = GzDecoder::new(BufReader::new(File::open(path).map_err(log_err)?));

    load_index_from(&mut reader)
}

fn load_uncompressed_index(path: &Path) -> Result<DocumentIndex, ()> {
    let mut reader = BufReader::new(File::open(path).map_err(log_err)?);

    load_index_from(&mut reader)
}

fn tf(document: &Document, word: &str) -> f64 {
    let n = document.word_count as f64;

    *document.document_frequencies.get(word).unwrap_or(&0) as f64 / n
}

fn idf(index: &DocumentIndex, word: &str) -> f64 {
    let n = index.len() as f64;
    let m = index
        .iter()
        .filter(|(_, document)| document.document_frequencies.contains_key(word))
        .count()
        .max(1) as f64;

    (n / m).ln()
}

fn get_cached_idf<'a>(
    index: &DocumentIndex,
    idf_cache: &mut HashMap<&'a str, f64>,
    word: &'a str,
) -> f64 {
    if let Some(&idf) = idf_cache.get(word) {
        return idf;
    }
    let idf = idf(index, word);
    idf_cache.insert(word, idf);
    return idf;
}

fn tf_idf<'a>(
    document: &Document,
    index: &DocumentIndex,
    idf_cache: &mut HashMap<&'a str, f64>,
    words: &[&'a str],
) -> f64 {
    let mut rank = 0f64;
    for word in words {
        let idf = get_cached_idf(index, idf_cache, word);
        rank += tf(document, word) * idf;
    }
    rank
}

fn find_results<'a>(index: &'a DocumentIndex, words: &[&str]) -> Vec<(&'a PathBuf, f64)> {
    let mut idf_cache = HashMap::new();
    let mut results = index
        .iter()
        .map(|(path, document)| (path, tf_idf(document, index, &mut idf_cache, words)))
        .collect::<Vec<_>>();
    results.sort_unstable_by(|(_, rank1), (_, rank2)| rank2.total_cmp(rank1));
    results
}

//Silly macro to time the execution of an expression
macro_rules! time {
    ($exp:expr, $label:literal) => {{
        let start = Instant::now();
        let result = $exp;
        println!("{} took {:?}", $label, start.elapsed());
        result
    }};
}

fn main() {
    let index_path = Path::new("./out.dat");
    if !index_path.exists() {
        eprintln!(
            "WARN: Index file {} is not accessible! Rebuilding index...",
            index_path.display()
        );
        let index = time!(scan_directories(&MAN_PATHS).unwrap(), "Scanning");

        time!(
            write_uncompressed_index(&index, index_path).unwrap(),
            "Writing"
        );
    }

    let index = time!(
        load_uncompressed_index(index_path).unwrap(),
        "Loading index"
    );

    let documents = time!(
        find_results(&index, &["tcp", "socket", "stream"]),
        "Finding results"
    );
    }
}
