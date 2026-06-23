use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use serde::{Deserialize, Serialize};
use undr9_common::{crc32, Result, Undr9Error};
use undr9_config::WalConfig;

pub const WAL_FILE_EXTENSION: &str = "wal";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct LogSequenceNumber(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WalRecordKind {
    WriteBatch,
    Checkpoint,
    ManifestSync,
    ConsolidationEvent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DurabilityMode {
    FsyncOnWrite,
    Buffered,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalRecordHeader {
    pub lsn: LogSequenceNumber,
    pub kind: WalRecordKind,
    pub payload_len: u32,
    pub payload_crc32: u32,
    pub format_version: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalRecord {
    pub header: WalRecordHeader,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalRuntimeConfig {
    pub segment_size_bytes: u64,
    pub durability_mode: DurabilityMode,
    pub max_replay_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointMarker {
    pub last_applied_lsn: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Wal {
    root_dir: PathBuf,
    runtime: WalRuntimeConfig,
    active_segment_id: u64,
    next_lsn: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WalIoFailpoint {
    operation: &'static str,
    path_fragment: String,
}

static WAL_IO_FAILPOINT: OnceLock<Mutex<Option<WalIoFailpoint>>> = OnceLock::new();

impl WalRecord {
    pub fn new(lsn: LogSequenceNumber, kind: WalRecordKind, payload: Vec<u8>) -> Result<Self> {
        let payload_len = u32::try_from(payload.len()).map_err(|_| {
            Undr9Error::Validation("WAL payload length exceeds supported u32 range".to_owned())
        })?;

        let header = WalRecordHeader {
            lsn,
            kind,
            payload_len,
            payload_crc32: crc32(&payload),
            format_version: 1,
        };

        Ok(Self { header, payload })
    }

    pub fn verify_checksum(&self) -> bool {
        crc32(&self.payload) == self.header.payload_crc32
    }
}

impl From<&WalConfig> for WalRuntimeConfig {
    fn from(config: &WalConfig) -> Self {
        Self {
            segment_size_bytes: config.segment_size_bytes,
            durability_mode: if config.fsync_on_write {
                DurabilityMode::FsyncOnWrite
            } else {
                DurabilityMode::Buffered
            },
            max_replay_bytes: config.max_replay_bytes,
        }
    }
}

impl Wal {
    pub fn open(root_dir: impl Into<PathBuf>, config: &WalConfig) -> Result<Self> {
        let root_dir = root_dir.into();
        fs::create_dir_all(&root_dir)
            .map_err(|error| Undr9Error::Io(format!("failed to create WAL directory: {error}")))?;

        let runtime = WalRuntimeConfig::from(config);
        let records = replay_from_dir(&root_dir, runtime.max_replay_bytes)?;
        let next_lsn = records
            .last()
            .map(|record| record.header.lsn.0 + 1)
            .unwrap_or(1);
        let active_segment_id = list_segment_paths(&root_dir)?
            .last()
            .and_then(|path| segment_id_from_path(path))
            .unwrap_or(1);

        Ok(Self {
            root_dir,
            runtime,
            active_segment_id,
            next_lsn,
        })
    }

    pub fn append(&mut self, kind: WalRecordKind, payload: Vec<u8>) -> Result<WalRecord> {
        let record = WalRecord::new(LogSequenceNumber(self.next_lsn), kind, payload)?;
        let frame = encode_record(&record)?;
        self.rotate_if_needed(frame.len() as u64)?;

        let path = self.active_segment_path();
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|error| {
                Undr9Error::Io(format!(
                    "failed to open WAL segment '{}': {error}",
                    path.display()
                ))
            })?;

        maybe_fail_wal_io("append_write", &path)?;
        file.write_all(&frame)
            .map_err(|error| Undr9Error::Io(format!("failed to append WAL frame: {error}")))?;

        if self.runtime.durability_mode == DurabilityMode::FsyncOnWrite {
            maybe_fail_wal_io("append_fsync", &path)?;
            file.sync_data()
                .map_err(|error| Undr9Error::Io(format!("failed to fsync WAL segment: {error}")))?;
        }

        self.next_lsn += 1;
        Ok(record)
    }

    pub fn append_checkpoint(&mut self, marker: CheckpointMarker) -> Result<WalRecord> {
        let payload = postcard::to_allocvec(&marker).map_err(|error| {
            Undr9Error::Serialization(format!("failed to serialize checkpoint marker: {error}"))
        })?;
        self.append(WalRecordKind::Checkpoint, payload)
    }

    pub fn replay(&self) -> Result<Vec<WalRecord>> {
        replay_from_dir(&self.root_dir, self.runtime.max_replay_bytes)
    }

    pub fn truncate_all_segments(&mut self) -> Result<()> {
        for path in list_segment_paths(&self.root_dir)? {
            fs::remove_file(&path).map_err(|error| {
                Undr9Error::Io(format!(
                    "failed to remove WAL segment '{}': {error}",
                    path.display()
                ))
            })?;
        }

        self.active_segment_id = 1;
        self.next_lsn = 1;
        Ok(())
    }

    pub fn active_segment_path(&self) -> PathBuf {
        self.root_dir
            .join(segment_file_name(self.active_segment_id))
    }

    fn rotate_if_needed(&mut self, incoming_frame_bytes: u64) -> Result<()> {
        let path = self.active_segment_path();
        let current_len = fs::metadata(&path)
            .map(|metadata| metadata.len())
            .unwrap_or(0);

        if current_len > 0 && current_len + incoming_frame_bytes > self.runtime.segment_size_bytes {
            self.active_segment_id += 1;
        }

        Ok(())
    }
}

#[doc(hidden)]
pub fn install_wal_io_failpoint(operation: &'static str, path_fragment: impl Into<String>) {
    let mut guard = wal_io_failpoint_slot()
        .lock()
        .expect("wal failpoint mutex should not be poisoned");
    *guard = Some(WalIoFailpoint {
        operation,
        path_fragment: path_fragment.into(),
    });
}

#[doc(hidden)]
pub fn clear_wal_io_failpoint() {
    let mut guard = wal_io_failpoint_slot()
        .lock()
        .expect("wal failpoint mutex should not be poisoned");
    *guard = None;
}

pub fn replay_from_dir(root_dir: &Path, max_replay_bytes: u64) -> Result<Vec<WalRecord>> {
    let mut records = Vec::new();
    let mut replayed_bytes = 0_u64;

    for path in list_segment_paths(root_dir)? {
        let bytes = fs::read(&path).map_err(|error| {
            Undr9Error::Io(format!(
                "failed to read WAL segment '{}': {error}",
                path.display()
            ))
        })?;

        replayed_bytes += u64::try_from(bytes.len()).map_err(|_| {
            Undr9Error::Validation("WAL replay byte count exceeded supported range".to_owned())
        })?;

        if replayed_bytes > max_replay_bytes {
            return Err(Undr9Error::Validation(format!(
                "WAL replay exceeded configured limit of {max_replay_bytes} bytes"
            )));
        }

        let mut offset = 0_usize;
        while offset + 8 <= bytes.len() {
            let frame_len = u64::from_le_bytes(
                bytes[offset..offset + 8]
                    .try_into()
                    .expect("slice length is checked"),
            ) as usize;
            let frame_start = offset + 8;
            let frame_end = frame_start + frame_len;

            if frame_end > bytes.len() {
                break;
            }

            let record: WalRecord =
                postcard::from_bytes(&bytes[frame_start..frame_end]).map_err(|error| {
                    Undr9Error::Corruption(format!(
                        "failed to deserialize WAL frame in '{}': {error}",
                        path.display()
                    ))
                })?;

            if !record.verify_checksum() {
                return Err(Undr9Error::Corruption(format!(
                    "WAL checksum mismatch in '{}'",
                    path.display()
                )));
            }

            records.push(record);
            offset = frame_end;
        }
    }

    Ok(records)
}

pub fn rewrite_dir_from_records(
    root_dir: &Path,
    config: &WalConfig,
    records: &[WalRecord],
) -> Result<()> {
    let mut wal = Wal::open(root_dir, config)?;
    wal.truncate_all_segments()?;
    for record in records {
        let appended = wal.append(record.header.kind, record.payload.clone())?;
        if appended.header.lsn != record.header.lsn {
            return Err(Undr9Error::Validation(format!(
                "failed to rewrite WAL record with original lsn {}",
                record.header.lsn.0
            )));
        }
    }
    Ok(())
}

pub fn list_segment_paths(root_dir: &Path) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for entry in fs::read_dir(root_dir)
        .map_err(|error| Undr9Error::Io(format!("failed to enumerate WAL directory: {error}")))?
    {
        let entry = entry.map_err(|error| {
            Undr9Error::Io(format!("failed to read WAL directory entry: {error}"))
        })?;
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) == Some(WAL_FILE_EXTENSION) {
            paths.push(path);
        }
    }

    paths.sort();
    Ok(paths)
}

pub fn segment_file_name(segment_id: u64) -> String {
    format!("{segment_id:020}.{WAL_FILE_EXTENSION}")
}

fn segment_id_from_path(path: &Path) -> Option<u64> {
    path.file_stem()
        .and_then(|value| value.to_str())
        .and_then(|value| value.parse::<u64>().ok())
}

fn wal_io_failpoint_slot() -> &'static Mutex<Option<WalIoFailpoint>> {
    WAL_IO_FAILPOINT.get_or_init(|| Mutex::new(None))
}

fn maybe_fail_wal_io(operation: &'static str, path: &Path) -> Result<()> {
    let guard = wal_io_failpoint_slot()
        .lock()
        .expect("wal failpoint mutex should not be poisoned");
    if let Some(failpoint) = guard.as_ref() {
        let matches_operation = failpoint.operation == operation;
        let matches_path = path
            .to_string_lossy()
            .contains(failpoint.path_fragment.as_str());
        if matches_operation && matches_path {
            return Err(Undr9Error::Io(format!(
                "injected WAL I/O failure for operation '{operation}' on '{}': No space left on device",
                path.display()
            )));
        }
    }
    Ok(())
}

fn encode_record(record: &WalRecord) -> Result<Vec<u8>> {
    let payload = postcard::to_allocvec(record).map_err(|error| {
        Undr9Error::Serialization(format!("failed to serialize WAL record: {error}"))
    })?;
    let payload_len = u64::try_from(payload.len())
        .map_err(|_| Undr9Error::Validation("WAL frame exceeded supported range".to_owned()))?;

    let mut frame = Vec::with_capacity(payload.len() + 8);
    frame.extend_from_slice(&payload_len.to_le_bytes());
    frame.extend_from_slice(&payload);
    Ok(frame)
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;
    use std::io::Write;

    use tempfile::tempdir;
    use undr9_config::AppConfig;

    #[test]
    fn wal_record_checksums_round_trip() {
        let record = super::WalRecord::new(
            super::LogSequenceNumber(7),
            super::WalRecordKind::WriteBatch,
            br#"{"op":"upsert"}"#.to_vec(),
        )
        .expect("record should be created");

        assert_eq!(record.header.payload_len, 15);
        assert!(record.verify_checksum());
    }

    #[test]
    fn runtime_config_respects_durability_settings() {
        let config = AppConfig::default();
        let runtime = super::WalRuntimeConfig::from(&config.wal);

        assert_eq!(runtime.durability_mode, super::DurabilityMode::FsyncOnWrite);
    }

    #[test]
    fn appends_and_replays_wal_records() {
        let tempdir = tempdir().expect("tempdir should be created");
        let config = AppConfig::default();
        let mut wal = super::Wal::open(tempdir.path(), &config.wal).expect("WAL should open");

        wal.append(
            super::WalRecordKind::WriteBatch,
            br#"{"node":"1"}"#.to_vec(),
        )
        .expect("first record should append");
        wal.append(super::WalRecordKind::ManifestSync, br#"{}"#.to_vec())
            .expect("second record should append");

        let replayed = wal.replay().expect("records should replay");
        assert_eq!(replayed.len(), 2);
        assert_eq!(replayed[0].header.lsn.0, 1);
        assert_eq!(replayed[1].header.lsn.0, 2);
    }

    #[test]
    fn ignores_trailing_partial_record_during_replay() {
        let tempdir = tempdir().expect("tempdir should be created");
        let config = AppConfig::default();
        let mut wal = super::Wal::open(tempdir.path(), &config.wal).expect("WAL should open");
        wal.append(
            super::WalRecordKind::WriteBatch,
            br#"{"node":"1"}"#.to_vec(),
        )
        .expect("record should append");

        let path = wal.active_segment_path();
        let mut file = OpenOptions::new()
            .append(true)
            .open(&path)
            .expect("segment should open");
        file.write_all(&[1, 2, 3, 4])
            .expect("partial bytes should append");

        let replayed = wal.replay().expect("replay should ignore trailing bytes");
        assert_eq!(replayed.len(), 1);
    }

    #[test]
    fn truncates_all_segments() {
        let tempdir = tempdir().expect("tempdir should be created");
        let config = AppConfig::default();
        let mut wal = super::Wal::open(tempdir.path(), &config.wal).expect("WAL should open");
        wal.append(
            super::WalRecordKind::WriteBatch,
            br#"{"node":"1"}"#.to_vec(),
        )
        .expect("record should append");

        wal.truncate_all_segments()
            .expect("WAL segments should be truncated");

        assert!(super::list_segment_paths(tempdir.path())
            .expect("segments should list")
            .is_empty());
        assert!(wal.replay().expect("replay should succeed").is_empty());
    }

    #[test]
    fn rejects_record_with_checksum_mismatch() {
        let tempdir = tempdir().expect("tempdir should be created");
        let record = super::WalRecord {
            header: super::WalRecordHeader {
                lsn: super::LogSequenceNumber(1),
                kind: super::WalRecordKind::WriteBatch,
                payload_len: 4,
                payload_crc32: 0,
                format_version: 1,
            },
            payload: vec![1, 2, 3, 4],
        };
        let frame = super::encode_record(&record).expect("frame should encode");
        let path = tempdir.path().join(super::segment_file_name(1));
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .expect("segment should open");
        file.write_all(&frame).expect("frame should write");

        let error =
            super::replay_from_dir(tempdir.path(), u64::MAX).expect_err("checksum mismatch must fail");
        assert!(error.to_string().contains("checksum mismatch"));
    }
}
