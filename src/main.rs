use anyhow::{bail, Context, Result};
use clap::Parser;
use regex::Regex;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fs::{self, File};
use std::io;
use std::path::{Component, Path, PathBuf};
use zip::read::ZipArchive;

#[derive(Parser, Debug)]
#[command(author, version, about = "Extract each .cbz in a directory into its own folder")]
struct Args {
    /// Directory containing .cbz files. Overrides input_dir from the config file.
    input_dir: Option<PathBuf>,

    /// Show planned folder names without extracting files
    #[arg(long)]
    dry_run: bool,
}

#[derive(Debug, Deserialize)]
struct Config {
    /// Regex applied to the CBZ file stem. Named captures can be used in folder_template.
    #[serde(default = "default_pattern")]
    pattern: String,

    /// Output folder template, e.g. "{series} - Volume {volume_padded}"
    #[serde(default = "default_folder_template")]
    folder_template: String,

    /// Simple naming mode. Replaces "xx" with the zero-padded volume number.
    /// Example: folder_name_def = "Tokyo_Ghoul_RE_xx"
    folder_name_def: Option<String>,

    /// Replace characters unsafe for file paths with '_'
    #[serde(default = "default_true")]
    sanitize_folder_name: bool,

    /// Directory containing the CBZ files.
    input_dir: Option<PathBuf>,

    /// How to handle chapter folders inside the CBZ.
    #[serde(default)]
    flatten_mode: FlattenMode,
}

#[derive(Debug, Deserialize, Clone, Copy, Default)]
#[serde(rename_all = "snake_case")]
enum FlattenMode {
    /// Keep the archive's original internal folder structure.
    #[default]
    Preserve,
    /// Remove chapter folders and put files directly in the volume folder.
    /// Keeps the original file names when possible; duplicate names get a numeric suffix.
    FlattenChapterFolders,
    /// Remove chapter folders and rename all pages sequentially like 000.png, 001.png, ...
    FlattenChapters,
}

impl fmt::Display for FlattenMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            FlattenMode::Preserve => "preserve",
            FlattenMode::FlattenChapterFolders => "flatten_chapter_folders",
            FlattenMode::FlattenChapters => "flatten_chapters",
        };
        write!(f, "{value}")
    }
}

#[derive(Debug)]
struct ArchiveSummary {
    page_count: usize,
    chapter_folder_count: usize,
}

fn default_pattern() -> String {
    r"^(?P<series>.+?)\s+v(?P<volume>\d+).*$".to_string()
}

fn default_folder_template() -> String {
    "{series} v{volume_padded}".to_string()
}

fn default_true() -> bool {
    true
}

fn main() -> Result<()> {
    let args = Args::parse();
    let config_path = PathBuf::from("manga-extract.toml");
    let config = load_config(&config_path)?;
    let regex = Regex::new(&config.pattern).context("invalid regex in config")?;
    let input_dir = resolve_input_dir(&args, &config, &config_path)?;

    let mut cbz_files = collect_cbz_files(&input_dir)?;
    cbz_files.sort();

    if cbz_files.is_empty() {
        println!("No .cbz files found in {}", input_dir.display());
        return Ok(());
    }

    for cbz_path in cbz_files {
        let folder_name = build_folder_name(&cbz_path, &regex, &config)?;
        let output_dir = input_dir.join(&folder_name);

        if args.dry_run {
            let summary = summarize_archive(&cbz_path)?;
            println!("{} -> {}", cbz_path.display(), output_dir.display());
            println!("  flatten_mode: {}", config.flatten_mode);
            println!("  page_count: {}", summary.page_count);
            println!("  chapter_folders: {}", summary.chapter_folder_count);
            if let Some(range) = planned_output_range(config.flatten_mode, summary.page_count) {
                println!("  writing {}", range);
            }
            continue;
        }

        let summary = summarize_archive(&cbz_path)?;
        println!("Extracting {} -> {}", cbz_path.display(), output_dir.display());
        println!("  mode={}", config.flatten_mode);
        println!("  page_count={}", summary.page_count);
        println!("  chapter_folders={}", summary.chapter_folder_count);
        if let Some(range) = planned_output_range(config.flatten_mode, summary.page_count) {
            println!("  writing {}", range);
        }
        extract_cbz(&cbz_path, &output_dir, config.flatten_mode)?;
    }

    Ok(())
}

fn resolve_input_dir(args: &Args, config: &Config, config_path: &Path) -> Result<PathBuf> {
    if let Some(path) = &args.input_dir {
        return Ok(path.clone());
    }

    if let Some(path) = &config.input_dir {
        if path.is_absolute() {
            return Ok(path.clone());
        }

        let base_dir = config_path.parent().unwrap_or_else(|| Path::new("."));
        return Ok(base_dir.join(path));
    }

    Ok(PathBuf::from("."))
}

fn load_config(path: &Path) -> Result<Config> {
    if !path.exists() {
        bail!(
            "config file not found: {}\nCreate it from manga-extract.toml.example",
            path.display()
        );
    }

    let content = fs::read_to_string(path)
        .with_context(|| format!("failed to read config {}", path.display()))?;
    let config: Config = toml::from_str(&content)
        .with_context(|| format!("failed to parse config {}", path.display()))?;
    Ok(config)
}

fn collect_cbz_files(input_dir: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in fs::read_dir(input_dir)
        .with_context(|| format!("failed to read input directory {}", input_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.is_file()
            && path
                .extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| ext.eq_ignore_ascii_case("cbz"))
                .unwrap_or(false)
        {
            files.push(path);
        }
    }
    Ok(files)
}

fn build_folder_name(cbz_path: &Path, regex: &Regex, config: &Config) -> Result<PathBuf> {
    let stem = cbz_path
        .file_stem()
        .and_then(|s| s.to_str())
        .with_context(|| format!("invalid UTF-8 filename: {}", cbz_path.display()))?;

    let captures = regex
        .captures(stem)
        .with_context(|| format!("filename did not match pattern: {stem}"))?;

    let mut values = HashMap::new();
    values.insert("stem".to_string(), stem.to_string());
    values.insert(
        "filename".to_string(),
        cbz_path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or_default()
            .to_string(),
    );

    for name in regex.capture_names().flatten() {
        if let Some(m) = captures.name(name) {
            values.insert(name.to_string(), m.as_str().trim().to_string());
        }
    }

    if let Some(volume) = values.get("volume").cloned() {
        let volume_padded = volume
            .parse::<u32>()
            .map(|n| format!("{n:02}"))
            .unwrap_or(volume.clone());
        values.insert("volume_padded".to_string(), volume_padded);
    }

    let mut folder_name = if let Some(def) = &config.folder_name_def {
        let volume = values
            .get("volume_padded")
            .or_else(|| values.get("volume"))
            .with_context(|| format!("missing volume capture for {}", cbz_path.display()))?;
        def.replace("xx", volume)
    } else {
        render_template(&config.folder_template, &values)
    };

    if config.sanitize_folder_name {
        folder_name = sanitize_folder_name(&folder_name);
    }

    let folder_name = folder_name.trim();
    if folder_name.is_empty() {
        bail!("generated folder name was empty for {}", cbz_path.display());
    }

    Ok(PathBuf::from(folder_name))
}

fn render_template(template: &str, values: &HashMap<String, String>) -> String {
    let mut rendered = template.to_string();
    for (key, value) in values {
        let placeholder = format!("{{{key}}}");
        rendered = rendered.replace(&placeholder, value);
    }
    rendered
}

fn sanitize_folder_name(name: &str) -> String {
    name.chars()
        .map(|ch| match ch {
            '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*' => '_',
            _ => ch,
        })
        .collect()
}

fn summarize_archive(cbz_path: &Path) -> Result<ArchiveSummary> {
    let file = File::open(cbz_path)
        .with_context(|| format!("failed to open archive {}", cbz_path.display()))?;
    let mut archive = ZipArchive::new(file)
        .with_context(|| format!("failed to read zip archive {}", cbz_path.display()))?;

    let file_entries = collect_file_entries(&mut archive)?;
    let mut chapter_folders = HashSet::new();

    for (_, path) in &file_entries {
        if let Some(parent) = path.parent() {
            let parent_str = parent.to_string_lossy();
            if !parent_str.is_empty() && parent != Path::new("") {
                chapter_folders.insert(parent.to_path_buf());
            }
        }
    }

    Ok(ArchiveSummary {
        page_count: file_entries.len(),
        chapter_folder_count: chapter_folders.len(),
    })
}

fn planned_output_range(flatten_mode: FlattenMode, page_count: usize) -> Option<String> {
    if page_count == 0 {
        return None;
    }

    match flatten_mode {
        FlattenMode::FlattenChapters => {
            let width = std::cmp::max(3, page_count.saturating_sub(1).to_string().len());
            Some(format!(
                "{:0width$}.<ext> ... {:0width$}.<ext>",
                0,
                page_count - 1,
                width = width
            ))
        }
        _ => None,
    }
}

fn extract_cbz(cbz_path: &Path, output_dir: &Path, flatten_mode: FlattenMode) -> Result<()> {
    if output_dir.exists() {
        bail!("output directory already exists: {}", output_dir.display());
    }

    fs::create_dir_all(output_dir)
        .with_context(|| format!("failed to create {}", output_dir.display()))?;

    let file = File::open(cbz_path)
        .with_context(|| format!("failed to open archive {}", cbz_path.display()))?;
    let mut archive = ZipArchive::new(file)
        .with_context(|| format!("failed to read zip archive {}", cbz_path.display()))?;

    match flatten_mode {
        FlattenMode::Preserve => extract_preserving_paths(&mut archive, output_dir)?,
        FlattenMode::FlattenChapterFolders => {
            extract_flatten_chapter_folders(&mut archive, output_dir)?
        }
        FlattenMode::FlattenChapters => extract_flatten_chapters(&mut archive, output_dir)?,
    }

    Ok(())
}

fn extract_preserving_paths<R: io::Read + io::Seek>(
    archive: &mut ZipArchive<R>,
    output_dir: &Path,
) -> Result<()> {
    for i in 0..archive.len() {
        let mut entry = archive.by_index(i)?;
        let enclosed = safe_enclosed_path(&entry)?;
        let out_path = output_dir.join(enclosed);

        if entry.is_dir() {
            fs::create_dir_all(&out_path)?;
            continue;
        }

        if let Some(parent) = out_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let mut outfile = File::create(&out_path)
            .with_context(|| format!("failed to create {}", out_path.display()))?;
        io::copy(&mut entry, &mut outfile)
            .with_context(|| format!("failed to extract to {}", out_path.display()))?;
    }

    Ok(())
}

fn extract_flatten_chapter_folders<R: io::Read + io::Seek>(
    archive: &mut ZipArchive<R>,
    output_dir: &Path,
) -> Result<()> {
    let mut file_entries = collect_file_entries(archive)?;
    let mut used_names: HashMap<String, usize> = HashMap::new();

    for (archive_index, original_path) in file_entries.drain(..) {
        let mut entry = archive.by_index(archive_index)?;
        let file_name = original_path
            .file_name()
            .and_then(|name| name.to_str())
            .with_context(|| format!("invalid UTF-8 entry path: {}", original_path.display()))?;
        let output_name = unique_flattened_name(file_name, &mut used_names);
        let out_path = output_dir.join(output_name);

        let mut outfile = File::create(&out_path)
            .with_context(|| format!("failed to create {}", out_path.display()))?;
        io::copy(&mut entry, &mut outfile)
            .with_context(|| format!("failed to extract to {}", out_path.display()))?;
    }

    Ok(())
}

fn extract_flatten_chapters<R: io::Read + io::Seek>(
    archive: &mut ZipArchive<R>,
    output_dir: &Path,
) -> Result<()> {
    let file_entries = collect_file_entries(archive)?;

    if file_entries.is_empty() {
        return Ok(());
    }

    let width = std::cmp::max(3, file_entries.len().saturating_sub(1).to_string().len());

    for (page_index, (archive_index, original_path)) in file_entries.iter().enumerate() {
        let mut entry = archive.by_index(*archive_index)?;
        let extension = original_path
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|s| s.to_string());

        let output_name = match extension {
            Some(ext) if !ext.is_empty() => format!("{:0width$}.{}", page_index, ext, width = width),
            _ => format!("{:0width$}", page_index, width = width),
        };

        let out_path = output_dir.join(output_name);
        let mut outfile = File::create(&out_path)
            .with_context(|| format!("failed to create {}", out_path.display()))?;
        io::copy(&mut entry, &mut outfile)
            .with_context(|| format!("failed to extract to {}", out_path.display()))?;
    }

    Ok(())
}

fn collect_file_entries<R: io::Read + io::Seek>(
    archive: &mut ZipArchive<R>,
) -> Result<Vec<(usize, PathBuf)>> {
    let mut file_entries = Vec::new();

    for i in 0..archive.len() {
        let entry = archive.by_index(i)?;
        let enclosed = safe_enclosed_path(&entry)?;
        if !entry.is_dir() {
            file_entries.push((i, enclosed));
        }
    }

    file_entries.sort_by(|a, b| a.1.cmp(&b.1));
    Ok(file_entries)
}

fn unique_flattened_name(file_name: &str, used_names: &mut HashMap<String, usize>) -> String {
    match used_names.get_mut(file_name) {
        None => {
            used_names.insert(file_name.to_string(), 1);
            file_name.to_string()
        }
        Some(counter) => {
            let current = *counter;
            *counter += 1;

            let path = Path::new(file_name);
            let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or(file_name);
            let ext = path.extension().and_then(|s| s.to_str());

            match ext {
                Some(ext) if !ext.is_empty() => format!("{}_{:03}.{}", stem, current, ext),
                _ => format!("{}_{:03}", stem, current),
            }
        }
    }
}

fn safe_enclosed_path(entry: &zip::read::ZipFile<'_>) -> Result<PathBuf> {
    let enclosed = entry
        .enclosed_name()
        .map(PathBuf::from)
        .with_context(|| format!("archive entry has unsafe path: {}", entry.name()))?;

    if enclosed.components().any(|c| {
        matches!(
            c,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        )
    }) {
        bail!("archive entry has invalid path: {}", entry.name());
    }

    Ok(enclosed)
}
