use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use flate2::read::GzDecoder;
use tar::Archive;
use zip::CompressionMethod;
use zip::read::{ArchiveOffset, Config as ZipReadConfig};

use crate::error::{PyUdfError, PyUdfResult};

const MAX_ARCHIVE_PATH_BYTES: usize = 4096;
const MAX_ARCHIVE_COMPONENT_BYTES: usize = 255;
const DEFAULT_MAX_ARCHIVE_INPUT_BYTES: u64 = 1024 * 1024 * 1024;
const MAX_TAR_METADATA_ENTRY_BYTES: u64 = 1024 * 1024;
const MAX_TAR_METADATA_TOTAL_BYTES: u64 = 8 * 1024 * 1024;
const ZIP_EOCD_MIN_BYTES: usize = 22;
const ZIP_EOCD_MAX_BYTES: usize = ZIP_EOCD_MIN_BYTES + u16::MAX as usize;
const ZIP_CENTRAL_HEADER_BYTES: usize = 46;

#[derive(Debug, Clone, Copy)]
pub(crate) struct ArchiveExtractionLimits {
    pub max_input_bytes: u64,
    pub max_records: usize,
    pub max_entries: usize,
    pub max_path_components: usize,
    pub max_entry_bytes: u64,
    pub max_total_bytes: u64,
    pub max_expansion_ratio: u64,
}

#[derive(Debug)]
pub(crate) struct ArchiveExtractionSummary {
    pub expanded_bytes: u64,
    pub entries: usize,
    pub records: usize,
}

impl Default for ArchiveExtractionLimits {
    fn default() -> Self {
        Self {
            max_input_bytes: DEFAULT_MAX_ARCHIVE_INPUT_BYTES,
            max_records: 10_000,
            max_entries: 10_000,
            max_path_components: 32,
            max_entry_bytes: 256 * 1024 * 1024,
            max_total_bytes: 1024 * 1024 * 1024,
            max_expansion_ratio: 100,
        }
    }
}

#[derive(Default)]
struct ExtractionUsage {
    records: usize,
    entries: usize,
    total_bytes: u64,
    compressed_bytes: u64,
    paths: HashSet<PathBuf>,
}

struct ZipPreflight {
    archive_offset: u64,
    central_directory_start: u64,
    records: usize,
}

impl ExtractionUsage {
    fn observe_record(&mut self, limits: ArchiveExtractionLimits) -> PyUdfResult<()> {
        self.records = self
            .records
            .checked_add(1)
            .ok_or_else(|| PyUdfError::invalid("archive record count overflow"))?;
        if self.records > limits.max_records {
            return Err(PyUdfError::invalid(format!(
                "archive exceeded the limit of {} records",
                limits.max_records
            )));
        }
        Ok(())
    }

    fn observe_path(&mut self, path: &Path, limits: ArchiveExtractionLimits) -> PyUdfResult<()> {
        let mut ancestor = PathBuf::new();
        for component in path.components() {
            ancestor.push(component);
            if self.paths.insert(ancestor.clone()) {
                self.entries = self
                    .entries
                    .checked_add(1)
                    .ok_or_else(|| PyUdfError::invalid("archive entry count overflow"))?;
                if self.entries > limits.max_entries {
                    return Err(PyUdfError::invalid(format!(
                        "archive exceeded the limit of {} entries (including implicit directories)",
                        limits.max_entries
                    )));
                }
            }
        }
        Ok(())
    }

    fn observe_entry(
        &mut self,
        name: &str,
        size: u64,
        compressed_size: u64,
        limits: ArchiveExtractionLimits,
    ) -> PyUdfResult<()> {
        if size > limits.max_entry_bytes {
            return Err(PyUdfError::invalid(format!(
                "archive entry {name} exceeded the limit of {} bytes",
                limits.max_entry_bytes
            )));
        }
        let total_bytes = self
            .total_bytes
            .checked_add(size)
            .ok_or_else(|| PyUdfError::invalid("archive expanded byte count overflow"))?;
        if total_bytes > limits.max_total_bytes {
            return Err(PyUdfError::invalid(format!(
                "archive exceeded the expanded size limit of {} bytes",
                limits.max_total_bytes
            )));
        }
        validate_expansion_ratio(name, size, compressed_size, limits.max_expansion_ratio)?;
        let compressed_bytes = self
            .compressed_bytes
            .checked_add(compressed_size)
            .ok_or_else(|| PyUdfError::invalid("archive compressed byte count overflow"))?;
        validate_expansion_ratio(
            "aggregate contents",
            total_bytes,
            compressed_bytes,
            limits.max_expansion_ratio,
        )?;
        self.total_bytes = total_bytes;
        self.compressed_bytes = compressed_bytes;
        Ok(())
    }
}

fn validate_expansion_ratio(
    name: &str,
    expanded_bytes: u64,
    compressed_bytes: u64,
    max_ratio: u64,
) -> PyUdfResult<()> {
    if expanded_bytes == 0 {
        return Ok(());
    }
    let allowed = u128::from(compressed_bytes)
        .checked_mul(u128::from(max_ratio))
        .ok_or_else(|| PyUdfError::invalid("archive expansion ratio overflow"))?;
    if u128::from(expanded_bytes) > allowed {
        return Err(PyUdfError::invalid(format!(
            "archive {name} exceeded the expansion ratio limit of {max_ratio}:1"
        )));
    }
    Ok(())
}

pub(crate) fn extract_archive(
    archive_name: &str,
    archive_path: &Path,
    destination: &Path,
    limits: ArchiveExtractionLimits,
) -> PyUdfResult<ArchiveExtractionSummary> {
    let input_bytes = std::fs::metadata(archive_path)?.len();
    if input_bytes > limits.max_input_bytes {
        return Err(PyUdfError::invalid(format!(
            "archive exceeded the input size limit of {} bytes",
            limits.max_input_bytes
        )));
    }
    if archive_name.ends_with(".zip") || archive_name.ends_with(".jar") {
        extract_zip(archive_path, destination, limits)
    } else if archive_name.ends_with(".tar.gz") || archive_name.ends_with(".tgz") {
        extract_tar(archive_path, destination, limits, true)
    } else if archive_name.ends_with(".tar") {
        extract_tar(archive_path, destination, limits, false)
    } else {
        Err(PyUdfError::invalid(format!(
            "unsupported archive artifact type: {archive_name}"
        )))
    }
}

fn extract_zip(
    archive_path: &Path,
    destination: &Path,
    limits: ArchiveExtractionLimits,
) -> PyUdfResult<ArchiveExtractionSummary> {
    let mut file = File::open(archive_path)?;
    let preflight = preflight_zip(&mut file, limits)?;
    file.seek(SeekFrom::Start(0))?;
    let mut archive = zip::ZipArchive::with_config(
        ZipReadConfig {
            archive_offset: ArchiveOffset::Known(preflight.archive_offset),
        },
        file,
    )
    .map_err(|error| PyUdfError::invalid(format!("invalid ZIP archive: {error}")))?;
    if archive.offset() != preflight.archive_offset
        || archive.central_directory_start() != preflight.central_directory_start
        || archive.len() != preflight.records
    {
        return Err(PyUdfError::invalid(
            "invalid ZIP archive: parser selected a different central directory",
        ));
    }
    let mut usage = ExtractionUsage::default();
    for index in 0..archive.len() {
        usage.observe_record(limits)?;
        let mut entry = archive
            .by_index(index)
            .map_err(|error| PyUdfError::invalid(format!("invalid ZIP entry: {error}")))?;
        let name = entry.name().to_string();
        if entry.encrypted() {
            return Err(PyUdfError::invalid(format!(
                "encrypted ZIP entry is not supported: {name}"
            )));
        }
        if !matches!(
            entry.compression(),
            CompressionMethod::Stored | CompressionMethod::Deflated
        ) {
            return Err(PyUdfError::invalid(format!(
                "unsupported ZIP compression for entry {name}: {:?}",
                entry.compression()
            )));
        }
        if entry.is_symlink() || zip_mode_is_special(entry.unix_mode(), entry.is_dir()) {
            return Err(PyUdfError::invalid(format!(
                "unsafe ZIP entry type is not supported: {name}"
            )));
        }
        if entry.is_dir() && (entry.size() != 0 || entry.compressed_size() != 0) {
            return Err(PyUdfError::invalid(format!(
                "ZIP directory entry must not contain data: {name}"
            )));
        }
        let relative = strict_relative_path(&name, entry.is_dir(), limits.max_path_components)?;
        usage.observe_path(&relative, limits)?;
        usage.observe_entry(
            &name,
            if entry.is_dir() { 0 } else { entry.size() },
            if entry.is_dir() {
                0
            } else {
                entry.compressed_size()
            },
            limits,
        )?;
        let output = destination.join(relative);
        if entry.is_dir() {
            create_directory(&output)?;
        } else {
            create_parent_directories(&output)?;
            let mut file = create_new_file(&output)?;
            let entry_size = entry.size();
            copy_bounded(&mut entry, &mut file, &name, entry_size, &mut usage, limits)?;
        }
    }
    ensure_nonempty_archive(usage.records)?;
    Ok(ArchiveExtractionSummary {
        expanded_bytes: usage.total_bytes,
        entries: usage.entries,
        records: usage.records,
    })
}

fn preflight_zip(file: &mut File, limits: ArchiveExtractionLimits) -> PyUdfResult<ZipPreflight> {
    let file_len = file.metadata()?.len();
    if file_len < ZIP_EOCD_MIN_BYTES as u64 {
        return Err(PyUdfError::invalid(
            "invalid ZIP archive: missing end record",
        ));
    }
    let tail_len = usize::try_from(file_len.min(ZIP_EOCD_MAX_BYTES as u64))
        .map_err(|_| PyUdfError::invalid("ZIP end record size overflow"))?;
    let tail_start = file_len - tail_len as u64;
    file.seek(SeekFrom::Start(tail_start))?;
    let mut tail = vec![0_u8; tail_len];
    file.read_exact(&mut tail)
        .map_err(|_| PyUdfError::invalid("ZIP archive changed while it was being validated"))?;
    let mut extra = [0_u8; 1];
    if file.read(&mut extra)? != 0 {
        return Err(PyUdfError::invalid(
            "ZIP archive changed while it was being validated",
        ));
    }

    let eocd_candidates = tail
        .windows(4)
        .enumerate()
        .filter_map(|(offset, signature)| {
            if signature != b"PK\x05\x06" {
                return None;
            }
            let record = tail.get(offset..offset + ZIP_EOCD_MIN_BYTES)?;
            let comment_bytes = usize::from(read_zip_u16(record, 20));
            (offset
                .checked_add(ZIP_EOCD_MIN_BYTES)?
                .checked_add(comment_bytes)?
                == tail.len())
            .then_some(tail_start + offset as u64)
        })
        .collect::<Vec<_>>();
    let Some((eocd_offset, earlier_candidates)) = eocd_candidates.split_last() else {
        return Err(PyUdfError::invalid(
            "invalid ZIP archive: missing final end record",
        ));
    };
    let record_offset = usize::try_from(*eocd_offset - tail_start)
        .map_err(|_| PyUdfError::invalid("ZIP end record offset overflow"))?;
    let preflight = validate_zip_end_record(
        file,
        &tail[record_offset..record_offset + ZIP_EOCD_MIN_BYTES],
        *eocd_offset,
        limits,
    )?;
    for offset in earlier_candidates {
        let record_offset = usize::try_from(*offset - tail_start)
            .map_err(|_| PyUdfError::invalid("ZIP end record offset overflow"))?;
        if validate_zip_end_record(
            file,
            &tail[record_offset..record_offset + ZIP_EOCD_MIN_BYTES],
            *offset,
            limits,
        )
        .is_ok()
        {
            return Err(PyUdfError::invalid(
                "invalid ZIP archive: multiple structurally valid final end records",
            ));
        }
    }
    if file.metadata()?.len() != file_len {
        return Err(PyUdfError::invalid(
            "ZIP archive changed while it was being validated",
        ));
    }
    Ok(preflight)
}

fn validate_zip_end_record(
    file: &mut File,
    record: &[u8],
    eocd_offset: u64,
    limits: ArchiveExtractionLimits,
) -> PyUdfResult<ZipPreflight> {
    let disk = read_zip_u16(record, 4);
    let directory_disk = read_zip_u16(record, 6);
    let disk_entries = read_zip_u16(record, 8);
    let entries = read_zip_u16(record, 10);
    let directory_bytes = read_zip_u32(record, 12);
    let directory_offset = read_zip_u32(record, 16);
    if disk != 0 || directory_disk != 0 || disk_entries != entries {
        return Err(PyUdfError::invalid(
            "multi-disk ZIP archives are not supported",
        ));
    }
    if entries == u16::MAX || directory_bytes == u32::MAX || directory_offset == u32::MAX {
        return Err(PyUdfError::invalid("ZIP64 archives are not supported"));
    }
    if usize::from(entries) > limits.max_records {
        return Err(PyUdfError::invalid(format!(
            "archive exceeded the limit of {} records",
            limits.max_records
        )));
    }
    let directory_bytes = u64::from(directory_bytes);
    let central_directory_start = eocd_offset.checked_sub(directory_bytes).ok_or_else(|| {
        PyUdfError::invalid("invalid ZIP archive: central directory exceeds the end record")
    })?;
    let archive_offset = central_directory_start
        .checked_sub(u64::from(directory_offset))
        .ok_or_else(|| {
            PyUdfError::invalid("invalid ZIP archive: central directory offset is inconsistent")
        })?;
    validate_zip_central_directory(
        file,
        central_directory_start,
        directory_bytes,
        usize::from(entries),
    )?;
    Ok(ZipPreflight {
        archive_offset,
        central_directory_start,
        records: usize::from(entries),
    })
}

fn validate_zip_central_directory(
    file: &mut File,
    start: u64,
    directory_bytes: u64,
    records: usize,
) -> PyUdfResult<()> {
    let minimum_bytes = records
        .checked_mul(ZIP_CENTRAL_HEADER_BYTES)
        .ok_or_else(|| PyUdfError::invalid("ZIP central directory record count overflow"))?;
    if minimum_bytes as u64 > directory_bytes {
        return Err(PyUdfError::invalid(
            "invalid ZIP archive: central directory is too short for its declared records",
        ));
    }
    file.seek(SeekFrom::Start(start))?;
    let mut directory = file.take(directory_bytes);
    let mut consumed = 0_u64;
    for _ in 0..records {
        let mut fixed = [0_u8; ZIP_CENTRAL_HEADER_BYTES];
        directory.read_exact(&mut fixed).map_err(|_| {
            PyUdfError::invalid("invalid ZIP archive: truncated central directory header")
        })?;
        consumed = consumed
            .checked_add(ZIP_CENTRAL_HEADER_BYTES as u64)
            .ok_or_else(|| PyUdfError::invalid("ZIP central directory offset overflow"))?;
        if fixed[..4] != *b"PK\x01\x02" {
            return Err(PyUdfError::invalid(
                "invalid ZIP archive: invalid central directory header",
            ));
        }
        let compressed_bytes = read_zip_u32(&fixed, 20);
        let expanded_bytes = read_zip_u32(&fixed, 24);
        let name_bytes = usize::from(read_zip_u16(&fixed, 28));
        let extra_bytes = usize::from(read_zip_u16(&fixed, 30));
        let comment_bytes = usize::from(read_zip_u16(&fixed, 32));
        let disk = read_zip_u16(&fixed, 34);
        let local_header_offset = read_zip_u32(&fixed, 42);
        if compressed_bytes == u32::MAX
            || expanded_bytes == u32::MAX
            || disk == u16::MAX
            || local_header_offset == u32::MAX
        {
            return Err(PyUdfError::invalid("ZIP64 archives are not supported"));
        }
        if disk != 0 {
            return Err(PyUdfError::invalid(
                "multi-disk ZIP archives are not supported",
            ));
        }
        let variable_bytes = name_bytes
            .checked_add(extra_bytes)
            .and_then(|size| size.checked_add(comment_bytes))
            .ok_or_else(|| PyUdfError::invalid("ZIP central directory field size overflow"))?;
        consumed = consumed
            .checked_add(variable_bytes as u64)
            .ok_or_else(|| PyUdfError::invalid("ZIP central directory record size overflow"))?;
        if consumed > directory_bytes {
            return Err(PyUdfError::invalid(
                "invalid ZIP archive: truncated central directory record",
            ));
        }
        skip_zip_bytes(&mut directory, name_bytes)?;
        let mut extra = vec![0_u8; extra_bytes];
        directory.read_exact(&mut extra).map_err(|_| {
            PyUdfError::invalid("invalid ZIP archive: truncated central directory extra field")
        })?;
        reject_zip64_extra_field(&extra)?;
        skip_zip_bytes(&mut directory, comment_bytes)?;
    }
    if consumed != directory_bytes {
        return Err(PyUdfError::invalid(
            "invalid ZIP archive: central directory size does not match its records",
        ));
    }
    Ok(())
}

fn skip_zip_bytes(reader: &mut impl Read, bytes: usize) -> PyUdfResult<()> {
    let copied = std::io::copy(&mut reader.take(bytes as u64), &mut std::io::sink())?;
    if copied != bytes as u64 {
        return Err(PyUdfError::invalid(
            "invalid ZIP archive: truncated central directory record",
        ));
    }
    Ok(())
}

fn reject_zip64_extra_field(extra: &[u8]) -> PyUdfResult<()> {
    let mut cursor = 0_usize;
    while extra.len().saturating_sub(cursor) >= 4 {
        let field = &extra[cursor..];
        let kind = read_zip_u16(field, 0);
        let length = usize::from(read_zip_u16(field, 2));
        let end = cursor
            .checked_add(4)
            .and_then(|offset| offset.checked_add(length))
            .ok_or_else(|| PyUdfError::invalid("ZIP extra field size overflow"))?;
        if end > extra.len() {
            return Err(PyUdfError::invalid(
                "invalid ZIP archive: truncated central directory extra field",
            ));
        }
        if kind == 0x0001 {
            return Err(PyUdfError::invalid("ZIP64 archives are not supported"));
        }
        cursor = end;
    }
    Ok(())
}

fn read_zip_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([bytes[offset], bytes[offset + 1]])
}

fn read_zip_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ])
}

struct TarPreflight {
    records: usize,
    materialized_entries: usize,
}

fn open_tar_reader(archive_path: &Path, gzip: bool) -> PyUdfResult<Box<dyn Read>> {
    let file = File::open(archive_path)?;
    if gzip {
        Ok(Box::new(GzDecoder::new(file)))
    } else {
        Ok(Box::new(file))
    }
}

fn preflight_tar(
    reader: Box<dyn Read>,
    limits: ArchiveExtractionLimits,
) -> PyUdfResult<TarPreflight> {
    let mut archive = Archive::new(reader);
    let entries = archive
        .entries()
        .map_err(|error| PyUdfError::invalid(format!("invalid tar archive: {error}")))?
        .raw(true);
    let mut usage = ExtractionUsage::default();
    let mut materialized_entries = 0_usize;
    let mut materialized_bytes = 0_u64;
    let mut metadata_bytes = 0_u64;
    let mut pending_extension = false;
    let mut pending_local_pax = false;
    let mut pending_pax_size = None;
    let mut pending_long_name = false;
    let mut pending_long_link = false;
    for entry in entries {
        usage.observe_record(limits)?;
        let mut entry =
            entry.map_err(|error| PyUdfError::invalid(format!("invalid tar entry: {error}")))?;
        let entry_type = entry.header().entry_type();
        let size = entry.size();
        if entry_type.is_gnu_longname()
            || entry_type.is_gnu_longlink()
            || entry_type.is_pax_local_extensions()
            || entry_type.is_pax_global_extensions()
        {
            if size > MAX_TAR_METADATA_ENTRY_BYTES {
                return Err(PyUdfError::invalid(format!(
                    "tar metadata entry exceeded the limit of {MAX_TAR_METADATA_ENTRY_BYTES} bytes"
                )));
            }
            metadata_bytes = metadata_bytes
                .checked_add(size)
                .ok_or_else(|| PyUdfError::invalid("tar metadata size overflow"))?;
            if metadata_bytes > MAX_TAR_METADATA_TOTAL_BYTES {
                return Err(PyUdfError::invalid(format!(
                    "tar metadata exceeded the aggregate limit of {MAX_TAR_METADATA_TOTAL_BYTES} bytes"
                )));
            }
            if entry_type.is_gnu_longname() {
                if pending_long_name {
                    return Err(PyUdfError::invalid(
                        "tar archive contains duplicate GNU long-name metadata",
                    ));
                }
                validate_gnu_long_name(&mut entry, limits.max_path_components)?;
                pending_long_name = true;
                pending_extension = true;
            } else if entry_type.is_gnu_longlink() {
                if pending_long_link {
                    return Err(PyUdfError::invalid(
                        "tar archive contains duplicate GNU long-link metadata",
                    ));
                }
                drain_tar_metadata(&mut entry, size)?;
                pending_long_link = true;
                pending_extension = true;
            } else {
                let global = entry_type.is_pax_global_extensions();
                if !global && pending_local_pax {
                    return Err(PyUdfError::invalid(
                        "tar archive contains duplicate local PAX metadata",
                    ));
                }
                let pax_size = validate_pax_extensions(&mut entry, global, limits)?;
                if global {
                    if pending_extension {
                        return Err(PyUdfError::invalid(
                            "global PAX metadata cannot interrupt per-entry metadata",
                        ));
                    }
                } else {
                    pending_local_pax = true;
                    pending_pax_size = pax_size;
                    pending_extension = true;
                }
            }
            continue;
        }
        if !entry_type.is_file() && !entry_type.is_dir() {
            return Err(PyUdfError::invalid(format!(
                "unsafe tar entry type is not supported: {:?}",
                entry_type
            )));
        }
        if let Some(pax_size) = pending_pax_size.take()
            && pax_size != size
        {
            return Err(PyUdfError::invalid(
                "PAX size metadata must match the bounded tar entry size",
            ));
        }
        if entry_type.is_dir() && size != 0 {
            return Err(PyUdfError::invalid(
                "tar directory entry must not contain data",
            ));
        }
        if size > limits.max_entry_bytes {
            return Err(PyUdfError::invalid(format!(
                "tar entry exceeded the limit of {} bytes",
                limits.max_entry_bytes
            )));
        }
        materialized_bytes = materialized_bytes
            .checked_add(size)
            .ok_or_else(|| PyUdfError::invalid("tar expanded byte count overflow"))?;
        if materialized_bytes > limits.max_total_bytes {
            return Err(PyUdfError::invalid(format!(
                "archive exceeded the expanded size limit of {} bytes",
                limits.max_total_bytes
            )));
        }
        materialized_entries = materialized_entries
            .checked_add(1)
            .ok_or_else(|| PyUdfError::invalid("tar materialized entry count overflow"))?;
        pending_extension = false;
        pending_local_pax = false;
        pending_long_name = false;
        pending_long_link = false;
    }
    if pending_extension {
        return Err(PyUdfError::invalid(
            "tar metadata did not describe a following entry",
        ));
    }
    ensure_nonempty_archive(materialized_entries)?;
    Ok(TarPreflight {
        records: usage.records,
        materialized_entries,
    })
}

fn extract_tar(
    archive_path: &Path,
    destination: &Path,
    limits: ArchiveExtractionLimits,
    gzip: bool,
) -> PyUdfResult<ArchiveExtractionSummary> {
    let compressed_bytes = std::fs::metadata(archive_path)?.len();
    let ratio_bytes = compressed_bytes.saturating_mul(limits.max_expansion_ratio);
    let limits = ArchiveExtractionLimits {
        max_total_bytes: limits.max_total_bytes.min(ratio_bytes),
        ..limits
    };
    let preflight = preflight_tar(open_tar_reader(archive_path, gzip)?, limits)?;
    let mut archive = Archive::new(open_tar_reader(archive_path, gzip)?);
    let entries = archive
        .entries()
        .map_err(|error| PyUdfError::invalid(format!("invalid tar archive: {error}")))?;
    let mut usage = ExtractionUsage::default();
    let mut materialized_entries = 0_usize;
    for entry in entries {
        let mut entry =
            entry.map_err(|error| PyUdfError::invalid(format!("invalid tar entry: {error}")))?;
        let entry_type = entry.header().entry_type();
        if entry_type.is_pax_global_extensions() {
            validate_pax_extensions(&mut entry, true, limits)?;
            continue;
        }
        if !entry_type.is_file() && !entry_type.is_dir() {
            return Err(PyUdfError::invalid(format!(
                "unsafe tar entry type is not supported: {:?}",
                entry_type
            )));
        }
        validate_pax_extensions(&mut entry, false, limits)?;
        let path = entry
            .path()
            .map_err(|error| PyUdfError::invalid(format!("invalid tar entry path: {error}")))?;
        let name = path
            .to_str()
            .ok_or_else(|| PyUdfError::invalid("tar entry path must be valid UTF-8"))?
            .to_string();
        let relative =
            strict_relative_path(&name, entry_type.is_dir(), limits.max_path_components)?;
        usage.observe_path(&relative, limits)?;
        let size = entry.size();
        if entry_type.is_dir() && size != 0 {
            return Err(PyUdfError::invalid(format!(
                "tar directory entry must not contain data: {name}"
            )));
        }
        usage.observe_entry(&name, size, size, limits)?;
        let output = destination.join(relative);
        if entry_type.is_dir() {
            create_directory(&output)?;
        } else {
            create_parent_directories(&output)?;
            let mut file = create_new_file(&output)?;
            copy_bounded(&mut entry, &mut file, &name, size, &mut usage, limits)?;
        }
        materialized_entries += 1;
    }
    ensure_nonempty_archive(materialized_entries)?;
    if materialized_entries != preflight.materialized_entries {
        return Err(PyUdfError::invalid(
            "tar extension processing changed the materialized entry count",
        ));
    }
    validate_expansion_ratio(
        "aggregate contents",
        usage.total_bytes,
        compressed_bytes,
        limits.max_expansion_ratio,
    )?;
    Ok(ArchiveExtractionSummary {
        expanded_bytes: usage.total_bytes,
        entries: usage.entries,
        records: preflight.records,
    })
}

fn drain_tar_metadata<R: Read>(entry: &mut tar::Entry<'_, R>, size: u64) -> PyUdfResult<Vec<u8>> {
    let capacity = usize::try_from(size)
        .map_err(|_| PyUdfError::invalid("tar metadata size does not fit in memory"))?;
    let mut data = Vec::with_capacity(capacity);
    entry.read_to_end(&mut data)?;
    if data.len() as u64 != size {
        return Err(PyUdfError::invalid("tar metadata entry size mismatch"));
    }
    Ok(data)
}

fn validate_gnu_long_name<R: Read>(
    entry: &mut tar::Entry<'_, R>,
    max_path_components: usize,
) -> PyUdfResult<()> {
    let size = entry.size();
    if size > (MAX_ARCHIVE_PATH_BYTES as u64).saturating_add(1) {
        return Err(PyUdfError::invalid(format!(
            "GNU tar long name exceeded the limit of {MAX_ARCHIVE_PATH_BYTES} bytes"
        )));
    }
    let data = drain_tar_metadata(entry, size)?;
    let name = data.strip_suffix(&[0]).unwrap_or(&data);
    let name = std::str::from_utf8(name)
        .map_err(|_| PyUdfError::invalid("GNU tar long name must be valid UTF-8"))?;
    strict_relative_path(name, name.ends_with('/'), max_path_components)?;
    Ok(())
}

fn validate_pax_extensions<R: Read>(
    entry: &mut tar::Entry<'_, R>,
    global: bool,
    limits: ArchiveExtractionLimits,
) -> PyUdfResult<Option<u64>> {
    let Some(extensions) = entry
        .pax_extensions()
        .map_err(|error| PyUdfError::invalid(format!("invalid PAX metadata: {error}")))?
    else {
        return Ok(None);
    };
    let mut path_seen = false;
    let mut link_path_seen = false;
    let mut size = None;
    for extension in extensions {
        let extension = extension
            .map_err(|error| PyUdfError::invalid(format!("invalid PAX metadata: {error}")))?;
        let key = extension
            .key()
            .map_err(|_| PyUdfError::invalid("PAX metadata key must be valid UTF-8"))?;
        if key.starts_with("GNU.sparse.") {
            return Err(PyUdfError::invalid(
                "GNU sparse PAX metadata is not supported",
            ));
        }
        match key {
            "path" => {
                if global || path_seen {
                    return Err(PyUdfError::invalid(
                        "ambiguous PAX path metadata is not supported",
                    ));
                }
                path_seen = true;
                let value = extension
                    .value()
                    .map_err(|_| PyUdfError::invalid("PAX path must be valid UTF-8"))?;
                strict_relative_path(value, value.ends_with('/'), limits.max_path_components)?;
            }
            "linkpath" => {
                if global || link_path_seen {
                    return Err(PyUdfError::invalid(
                        "ambiguous PAX link path metadata is not supported",
                    ));
                }
                link_path_seen = true;
                extension
                    .value()
                    .map_err(|_| PyUdfError::invalid("PAX link path must be valid UTF-8"))?;
            }
            "size" => {
                if global || size.is_some() {
                    return Err(PyUdfError::invalid(
                        "ambiguous PAX size metadata is not supported",
                    ));
                }
                let value = extension
                    .value()
                    .map_err(|_| PyUdfError::invalid("PAX size must be valid UTF-8"))?;
                let value = value
                    .parse::<u64>()
                    .map_err(|_| PyUdfError::invalid("PAX size must be an unsigned integer"))?;
                if value > limits.max_entry_bytes {
                    return Err(PyUdfError::invalid(format!(
                        "PAX size exceeded the limit of {} bytes",
                        limits.max_entry_bytes
                    )));
                }
                size = Some(value);
            }
            "uid" | "gid" | "uname" | "gname" | "mtime" | "atime" | "ctime" | "charset"
            | "comment" => {}
            _ if key.starts_with("SCHILY.xattr.") => {}
            _ if global => {
                return Err(PyUdfError::invalid(format!(
                    "unsupported global PAX metadata key: {key}"
                )));
            }
            _ => {}
        }
    }
    Ok(size)
}

fn strict_relative_path(
    name: &str,
    directory: bool,
    max_components: usize,
) -> PyUdfResult<PathBuf> {
    if name.len() > MAX_ARCHIVE_PATH_BYTES
        || name.contains('\0')
        || name.contains('\\')
        || name.contains(':')
        || name.starts_with('/')
    {
        return Err(PyUdfError::invalid(format!(
            "archive entry must use a safe relative path: {name:?}"
        )));
    }
    let path = if directory {
        name.strip_suffix('/').unwrap_or(name)
    } else {
        name
    };
    let components = path.split('/').collect::<Vec<_>>();
    if components.is_empty()
        || components.len() > max_components
        || components.iter().any(|component| {
            component.is_empty()
                || component.len() > MAX_ARCHIVE_COMPONENT_BYTES
                || *component == "."
                || *component == ".."
                || component.trim_end_matches([' ', '.']) != *component
                || is_windows_reserved_name(component)
        })
        || components
            .first()
            .is_some_and(|component| component.ends_with(':'))
        || components.iter().any(|component| {
            matches!(
                component.to_ascii_lowercase().as_str(),
                ".archives" | ".sail-artifact.sha256" | ".sail-locks"
            )
        })
    {
        return Err(PyUdfError::invalid(format!(
            "archive entry must use a safe relative path with at most {max_components} components: {name:?}"
        )));
    }
    Ok(components.iter().collect())
}

fn is_windows_reserved_name(component: &str) -> bool {
    let stem = component
        .split_once('.')
        .map_or(component, |(stem, _extension)| stem)
        .to_ascii_uppercase();
    matches!(stem.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || stem
            .strip_prefix("COM")
            .or_else(|| stem.strip_prefix("LPT"))
            .is_some_and(|number| {
                matches!(number, "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9")
            })
}

fn zip_mode_is_special(mode: Option<u32>, directory: bool) -> bool {
    let Some(mode) = mode else {
        return false;
    };
    let file_type = mode & 0o170000;
    if directory {
        file_type != 0 && file_type != 0o040000
    } else {
        file_type != 0 && file_type != 0o100000
    }
}

fn create_parent_directories(path: &Path) -> PyUdfResult<()> {
    let parent = path.parent().ok_or_else(|| {
        PyUdfError::invalid(format!("archive entry has no parent: {}", path.display()))
    })?;
    std::fs::create_dir_all(parent)?;
    Ok(())
}

fn create_directory(path: &Path) -> PyUdfResult<()> {
    if path.exists() && !path.is_dir() {
        return Err(PyUdfError::invalid(format!(
            "archive entry conflicts with an existing file: {}",
            path.display()
        )));
    }
    std::fs::create_dir_all(path)?;
    Ok(())
}

fn create_new_file(path: &Path) -> PyUdfResult<File> {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| {
            PyUdfError::invalid(format!(
                "archive contains a duplicate or conflicting entry {}: {error}",
                path.display()
            ))
        })
}

fn copy_bounded(
    reader: &mut impl Read,
    writer: &mut impl Write,
    name: &str,
    declared_size: u64,
    usage: &mut ExtractionUsage,
    limits: ArchiveExtractionLimits,
) -> PyUdfResult<()> {
    let mut observed = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        observed = observed
            .checked_add(read as u64)
            .ok_or_else(|| PyUdfError::invalid("archive entry byte count overflow"))?;
        if observed > declared_size || observed > limits.max_entry_bytes {
            return Err(PyUdfError::invalid(format!(
                "archive entry {name} exceeded its declared or configured size"
            )));
        }
        writer.write_all(&buffer[..read])?;
    }
    if observed != declared_size {
        return Err(PyUdfError::invalid(format!(
            "archive entry {name} size mismatch: expected {declared_size}, got {observed}"
        )));
    }
    if usage.total_bytes > limits.max_total_bytes {
        return Err(PyUdfError::invalid(format!(
            "archive exceeded the expanded size limit of {} bytes",
            limits.max_total_bytes
        )));
    }
    Ok(())
}

fn ensure_nonempty_archive(entries: usize) -> PyUdfResult<()> {
    if entries == 0 {
        Err(PyUdfError::invalid("archive did not contain any entries"))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used)]

    use flate2::Compression;
    use flate2::write::GzEncoder;
    use zip::ZipWriter;
    use zip::write::SimpleFileOptions;

    use super::*;

    fn test_limits() -> ArchiveExtractionLimits {
        ArchiveExtractionLimits {
            max_input_bytes: 1024 * 1024,
            max_records: 8,
            max_entries: 8,
            max_path_components: 4,
            max_entry_bytes: 64,
            max_total_bytes: 96,
            max_expansion_ratio: 1_000,
        }
    }

    fn write_zip(path: &Path, entries: &[(&str, &[u8])], compression: CompressionMethod) {
        let mut archive = ZipWriter::new(File::create(path).unwrap());
        let options = SimpleFileOptions::default().compression_method(compression);
        for (name, data) in entries {
            archive.start_file(*name, options).unwrap();
            archive.write_all(data).unwrap();
        }
        archive.finish().unwrap();
    }

    #[test]
    fn zip_extracts_regular_files() {
        let temp = tempfile::tempdir().unwrap();
        let archive = temp.path().join("artifact.zip");
        let destination = temp.path().join("output");
        std::fs::create_dir(&destination).unwrap();
        write_zip(
            &archive,
            &[("package/value.txt", b"value")],
            CompressionMethod::Stored,
        );

        let summary =
            extract_archive("artifact.zip", &archive, &destination, test_limits()).unwrap();
        assert_eq!(summary.records, 1);
        assert_eq!(summary.entries, 2);
        assert_eq!(summary.expanded_bytes, 5);
        assert_eq!(
            std::fs::read(destination.join("package/value.txt")).unwrap(),
            b"value"
        );
    }

    #[test]
    fn zip_rejects_parent_traversal() {
        let temp = tempfile::tempdir().unwrap();
        let archive = temp.path().join("artifact.zip");
        let destination = temp.path().join("output");
        std::fs::create_dir(&destination).unwrap();
        write_zip(
            &archive,
            &[("../escape.txt", b"escape")],
            CompressionMethod::Stored,
        );

        let error =
            extract_archive("artifact.zip", &archive, &destination, test_limits()).unwrap_err();
        assert!(error.to_string().contains("safe relative path"));
        assert!(!temp.path().join("escape.txt").exists());
    }

    #[test]
    fn zip_enforces_entry_and_total_limits() {
        let temp = tempfile::tempdir().unwrap();
        let archive = temp.path().join("artifact.zip");
        let destination = temp.path().join("output");
        std::fs::create_dir(&destination).unwrap();
        write_zip(
            &archive,
            &[("first", &[1; 49]), ("second", &[2; 49])],
            CompressionMethod::Stored,
        );

        let error =
            extract_archive("artifact.zip", &archive, &destination, test_limits()).unwrap_err();
        assert!(error.to_string().contains("expanded size limit"));
    }

    #[test]
    fn zip_enforces_entry_count_limit() {
        let temp = tempfile::tempdir().unwrap();
        let archive = temp.path().join("artifact.zip");
        let destination = temp.path().join("output");
        std::fs::create_dir(&destination).unwrap();
        write_zip(
            &archive,
            &[("first", b""), ("second", b"")],
            CompressionMethod::Stored,
        );
        let limits = ArchiveExtractionLimits {
            max_entries: 1,
            ..test_limits()
        };

        let error = extract_archive("artifact.zip", &archive, &destination, limits).unwrap_err();
        assert!(error.to_string().contains("limit of 1 entries"));
    }

    #[test]
    fn zip_counts_repeated_headers_against_record_limit() {
        let temp = tempfile::tempdir().unwrap();
        let archive = temp.path().join("artifact.zip");
        let destination = temp.path().join("output");
        std::fs::create_dir(&destination).unwrap();
        write_zip(&archive, &[("same", b"value")], CompressionMethod::Stored);
        let bytes = std::fs::read(&archive).unwrap();
        let directory = bytes
            .windows(4)
            .position(|window| window == b"PK\x01\x02")
            .unwrap();
        let eocd = bytes
            .windows(4)
            .position(|window| window == b"PK\x05\x06")
            .unwrap();
        let central_record = &bytes[directory..eocd];
        let mut repeated = Vec::with_capacity(bytes.len() + central_record.len());
        repeated.extend_from_slice(&bytes[..eocd]);
        repeated.extend_from_slice(central_record);
        repeated.extend_from_slice(&bytes[eocd..]);
        let repeated_eocd = eocd + central_record.len();
        repeated[repeated_eocd + 8..repeated_eocd + 12]
            .copy_from_slice(&2_u16.to_le_bytes().repeat(2));
        repeated[repeated_eocd + 12..repeated_eocd + 16]
            .copy_from_slice(&(central_record.len() as u32 * 2).to_le_bytes());
        std::fs::write(&archive, repeated).unwrap();
        let limits = ArchiveExtractionLimits {
            max_records: 1,
            ..test_limits()
        };

        let error = extract_archive("artifact.zip", &archive, &destination, limits).unwrap_err();
        assert!(error.to_string().contains("limit of 1 records"));
    }

    #[test]
    fn zip_preflights_declared_entry_count_before_parser_allocation() {
        let temp = tempfile::tempdir().unwrap();
        let archive = temp.path().join("artifact.zip");
        let destination = temp.path().join("output");
        std::fs::create_dir(&destination).unwrap();
        write_zip(&archive, &[("entry", b"value")], CompressionMethod::Stored);
        let mut bytes = std::fs::read(&archive).unwrap();
        let eocd = bytes
            .windows(4)
            .rposition(|window| window == b"PK\x05\x06")
            .unwrap();
        bytes[eocd + 8..eocd + 12].copy_from_slice(&5000_u16.to_le_bytes().repeat(2));
        std::fs::write(&archive, bytes).unwrap();

        let error =
            extract_archive("artifact.zip", &archive, &destination, test_limits()).unwrap_err();
        assert!(error.to_string().contains("limit of 8 records"));
    }

    #[test]
    fn zip_rejects_invalid_final_end_record_before_parser_fallback() {
        let temp = tempfile::tempdir().unwrap();
        let archive = temp.path().join("artifact.zip");
        let destination = temp.path().join("output");
        std::fs::create_dir(&destination).unwrap();
        write_zip(&archive, &[("entry", b"value")], CompressionMethod::Stored);
        let mut bytes = std::fs::read(&archive).unwrap();
        bytes.extend_from_slice(b"PK\x05\x06");
        bytes.extend_from_slice(&0_u16.to_le_bytes());
        bytes.extend_from_slice(&0_u16.to_le_bytes());
        bytes.extend_from_slice(&2_u16.to_le_bytes());
        bytes.extend_from_slice(&2_u16.to_le_bytes());
        bytes.extend_from_slice(&1_u32.to_le_bytes());
        bytes.extend_from_slice(&0_u32.to_le_bytes());
        bytes.extend_from_slice(&0_u16.to_le_bytes());
        std::fs::write(&archive, bytes).unwrap();
        let fallback = zip::ZipArchive::new(File::open(&archive).unwrap()).unwrap();
        assert_eq!(fallback.len(), 1);

        let error =
            extract_archive("artifact.zip", &archive, &destination, test_limits()).unwrap_err();
        assert!(error.to_string().contains("central directory is too short"));
    }

    #[test]
    fn zip_allows_eocd_and_zip64_magic_in_stored_payload() {
        let temp = tempfile::tempdir().unwrap();
        let nested = temp.path().join("nested.zip");
        write_zip(&nested, &[("value", b"nested")], CompressionMethod::Stored);
        let mut payload = std::fs::read(&nested).unwrap();
        payload.extend_from_slice(b"PK\x06\x06PK\x06\x07");
        let archive = temp.path().join("artifact.zip");
        let destination = temp.path().join("output");
        std::fs::create_dir(&destination).unwrap();
        write_zip(
            &archive,
            &[("nested.jar", payload.as_slice())],
            CompressionMethod::Stored,
        );
        let limits = ArchiveExtractionLimits {
            max_entry_bytes: 1024 * 1024,
            max_total_bytes: 1024 * 1024,
            ..test_limits()
        };

        extract_archive("artifact.zip", &archive, &destination, limits).unwrap();
        assert_eq!(
            std::fs::read(destination.join("nested.jar")).unwrap(),
            payload
        );
    }

    #[test]
    fn zip_rejects_structurally_valid_alternate_end_record() {
        let temp = tempfile::tempdir().unwrap();
        let first = temp.path().join("first.zip");
        let second = temp.path().join("second.zip");
        write_zip(&first, &[("first", b"value")], CompressionMethod::Stored);
        write_zip(&second, &[("second", b"value")], CompressionMethod::Stored);
        let mut bytes = std::fs::read(&first).unwrap();
        let first_eocd = bytes
            .windows(4)
            .rposition(|window| window == b"PK\x05\x06")
            .unwrap();
        let second_bytes = std::fs::read(&second).unwrap();
        bytes[first_eocd + 20..first_eocd + 22]
            .copy_from_slice(&(second_bytes.len() as u16).to_le_bytes());
        bytes.extend_from_slice(&second_bytes);
        let archive = temp.path().join("artifact.zip");
        std::fs::write(&archive, bytes).unwrap();
        let destination = temp.path().join("output");
        std::fs::create_dir(&destination).unwrap();

        let error =
            extract_archive("artifact.zip", &archive, &destination, test_limits()).unwrap_err();
        assert!(error.to_string().contains("multiple structurally valid"));
    }

    #[test]
    fn zip_enforces_input_size_before_reading_archive() {
        let temp = tempfile::tempdir().unwrap();
        let archive = temp.path().join("artifact.zip");
        File::create(&archive)
            .unwrap()
            .set_len(test_limits().max_input_bytes + 1)
            .unwrap();
        let destination = temp.path().join("output");
        std::fs::create_dir(&destination).unwrap();

        let error =
            extract_archive("artifact.zip", &archive, &destination, test_limits()).unwrap_err();
        assert!(error.to_string().contains("input size limit"));
    }

    #[test]
    fn zip_enforces_entry_size_limit() {
        let temp = tempfile::tempdir().unwrap();
        let archive = temp.path().join("artifact.zip");
        let destination = temp.path().join("output");
        std::fs::create_dir(&destination).unwrap();
        write_zip(&archive, &[("large", &[1; 65])], CompressionMethod::Stored);

        let error =
            extract_archive("artifact.zip", &archive, &destination, test_limits()).unwrap_err();
        assert!(error.to_string().contains("limit of 64 bytes"));
    }

    #[test]
    fn zip_enforces_path_component_limit() {
        let temp = tempfile::tempdir().unwrap();
        let archive = temp.path().join("artifact.zip");
        let destination = temp.path().join("output");
        std::fs::create_dir(&destination).unwrap();
        write_zip(
            &archive,
            &[("a/b/c/d/e", b"value")],
            CompressionMethod::Stored,
        );

        let error =
            extract_archive("artifact.zip", &archive, &destination, test_limits()).unwrap_err();
        assert!(error.to_string().contains("at most 4 components"));
    }

    #[test]
    fn zip_enforces_expansion_ratio() {
        let temp = tempfile::tempdir().unwrap();
        let archive = temp.path().join("artifact.zip");
        let destination = temp.path().join("output");
        std::fs::create_dir(&destination).unwrap();
        write_zip(
            &archive,
            &[("zeros", &[0; 4096])],
            CompressionMethod::Deflated,
        );
        let limits = ArchiveExtractionLimits {
            max_entry_bytes: 8192,
            max_total_bytes: 8192,
            max_expansion_ratio: 10,
            ..test_limits()
        };

        let error = extract_archive("artifact.zip", &archive, &destination, limits).unwrap_err();
        assert!(error.to_string().contains("expansion ratio"));
    }

    #[test]
    fn tar_rejects_symlinks() {
        let temp = tempfile::tempdir().unwrap();
        let archive_path = temp.path().join("artifact.tar");
        let mut archive = tar::Builder::new(File::create(&archive_path).unwrap());
        let mut header = tar::Header::new_gnu();
        header.set_size(0);
        header.set_entry_type(tar::EntryType::Symlink);
        header.set_path("link").unwrap();
        header.set_link_name("target").unwrap();
        header.set_cksum();
        archive.append(&header, &[][..]).unwrap();
        archive.finish().unwrap();
        let destination = temp.path().join("output");
        std::fs::create_dir(&destination).unwrap();

        let error = extract_archive("artifact.tar", &archive_path, &destination, test_limits())
            .unwrap_err();
        assert!(error.to_string().contains("unsafe tar entry type"));
    }

    #[test]
    fn tar_extracts_local_pax_path() {
        let temp = tempfile::tempdir().unwrap();
        let archive_path = temp.path().join("artifact.tar");
        let mut archive = tar::Builder::new(File::create(&archive_path).unwrap());
        let path = "p".repeat(120);
        archive
            .append_pax_extensions([("path", path.as_bytes())])
            .unwrap();
        let mut header = tar::Header::new_gnu();
        header.set_size(5);
        header.set_entry_type(tar::EntryType::Regular);
        header.set_path("placeholder").unwrap();
        header.set_cksum();
        archive.append(&header, &b"value"[..]).unwrap();
        archive.finish().unwrap();
        drop(archive);
        let destination = temp.path().join("output");
        std::fs::create_dir(&destination).unwrap();

        let summary =
            extract_archive("artifact.tar", &archive_path, &destination, test_limits()).unwrap();
        assert_eq!(summary.records, 2);
        assert_eq!(std::fs::read(destination.join(path)).unwrap(), b"value");
    }

    #[test]
    fn tar_enforces_input_size_before_parsing_archive() {
        let temp = tempfile::tempdir().unwrap();
        let archive = temp.path().join("artifact.tar");
        File::create(&archive)
            .unwrap()
            .set_len(test_limits().max_input_bytes + 1)
            .unwrap();
        let destination = temp.path().join("output");
        std::fs::create_dir(&destination).unwrap();

        let error =
            extract_archive("artifact.tar", &archive, &destination, test_limits()).unwrap_err();
        assert!(error.to_string().contains("input size limit"));
    }

    #[test]
    fn tar_extracts_gnu_long_name() {
        let temp = tempfile::tempdir().unwrap();
        let archive_path = temp.path().join("artifact.tar");
        let mut archive = tar::Builder::new(File::create(&archive_path).unwrap());
        let path = "g".repeat(120);
        let mut header = tar::Header::new_gnu();
        header.set_size(5);
        header.set_entry_type(tar::EntryType::Regular);
        header.set_cksum();
        archive
            .append_data(&mut header, &path, &b"value"[..])
            .unwrap();
        archive.finish().unwrap();
        drop(archive);
        let destination = temp.path().join("output");
        std::fs::create_dir(&destination).unwrap();

        let summary =
            extract_archive("artifact.tar", &archive_path, &destination, test_limits()).unwrap();
        assert_eq!(summary.records, 2);
        assert_eq!(std::fs::read(destination.join(path)).unwrap(), b"value");
    }

    #[test]
    fn tar_rejects_oversized_pax_metadata_before_cooked_parsing() {
        let temp = tempfile::tempdir().unwrap();
        let archive_path = temp.path().join("artifact.tar");
        let mut archive = tar::Builder::new(File::create(&archive_path).unwrap());
        let mut header = tar::Header::new_gnu();
        header.set_size(MAX_TAR_METADATA_ENTRY_BYTES + 1);
        header.set_entry_type(tar::EntryType::XHeader);
        header.set_path("pax-header").unwrap();
        header.set_cksum();
        archive
            .append(
                &header,
                std::io::repeat(0).take(MAX_TAR_METADATA_ENTRY_BYTES + 1),
            )
            .unwrap();
        archive.finish().unwrap();
        drop(archive);
        let destination = temp.path().join("output");
        std::fs::create_dir(&destination).unwrap();

        let limits = ArchiveExtractionLimits {
            max_input_bytes: 2 * 1024 * 1024,
            ..test_limits()
        };
        let error =
            extract_archive("artifact.tar", &archive_path, &destination, limits).unwrap_err();
        assert!(error.to_string().contains("metadata entry exceeded"));
    }

    #[test]
    fn tar_rejects_directory_payload() {
        let temp = tempfile::tempdir().unwrap();
        let archive_path = temp.path().join("artifact.tar");
        let mut archive = tar::Builder::new(File::create(&archive_path).unwrap());
        let mut header = tar::Header::new_gnu();
        header.set_size(1);
        header.set_entry_type(tar::EntryType::Directory);
        header.set_path("directory").unwrap();
        header.set_cksum();
        archive.append(&header, &b"x"[..]).unwrap();
        archive.finish().unwrap();
        let destination = temp.path().join("output");
        std::fs::create_dir(&destination).unwrap();

        let error = extract_archive("artifact.tar", &archive_path, &destination, test_limits())
            .unwrap_err();
        assert!(error.to_string().contains("must not contain data"));
    }

    #[test]
    fn tar_counts_repeated_headers_against_record_limit() {
        let temp = tempfile::tempdir().unwrap();
        let archive_path = temp.path().join("artifact.tar");
        let mut archive = tar::Builder::new(File::create(&archive_path).unwrap());
        for data in [b"first".as_slice(), b"second".as_slice()] {
            let mut header = tar::Header::new_gnu();
            header.set_size(data.len() as u64);
            header.set_entry_type(tar::EntryType::Regular);
            header.set_path("same").unwrap();
            header.set_cksum();
            archive.append(&header, data).unwrap();
        }
        archive.finish().unwrap();
        drop(archive);
        let destination = temp.path().join("output");
        std::fs::create_dir(&destination).unwrap();
        let limits = ArchiveExtractionLimits {
            max_records: 1,
            ..test_limits()
        };

        let error =
            extract_archive("artifact.tar", &archive_path, &destination, limits).unwrap_err();
        assert!(error.to_string().contains("limit of 1 records"));
    }

    #[test]
    fn compressed_tar_enforces_ratio_before_expanding_limit() {
        let temp = tempfile::tempdir().unwrap();
        let archive_path = temp.path().join("artifact.tar.gz");
        let encoder = GzEncoder::new(File::create(&archive_path).unwrap(), Compression::best());
        let mut archive = tar::Builder::new(encoder);
        let data = vec![0_u8; 4096];
        let mut header = tar::Header::new_gnu();
        header.set_size(data.len() as u64);
        header.set_entry_type(tar::EntryType::Regular);
        header.set_path("zeros").unwrap();
        header.set_cksum();
        archive.append(&header, data.as_slice()).unwrap();
        let encoder = archive.into_inner().unwrap();
        encoder.finish().unwrap();
        let destination = temp.path().join("output");
        std::fs::create_dir(&destination).unwrap();
        let limits = ArchiveExtractionLimits {
            max_entry_bytes: 8192,
            max_total_bytes: 8192,
            max_expansion_ratio: 1,
            ..test_limits()
        };

        let error =
            extract_archive("artifact.tar.gz", &archive_path, &destination, limits).unwrap_err();
        assert!(error.to_string().contains("expanded size limit"));
    }
}
