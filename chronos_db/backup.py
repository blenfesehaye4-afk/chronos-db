"""Point-in-time hot backup snapshot export and atomic restore for ChronosDB."""

import hashlib
import json
import shutil
import time
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Union

if TYPE_CHECKING:
    from .engine import ChronosEngine


class SnapshotManager:
    """Manages consistent point-in-time snapshots and restores."""

    @staticmethod
    def _compute_sha256(filepath: Path) -> str:
        hasher = hashlib.sha256()
        with open(filepath, "rb") as f:
            while chunk := f.read(65536):
                hasher.update(chunk)
        return hasher.hexdigest()

    @classmethod
    def export_snapshot(cls, engine: "ChronosEngine", target_dir: Union[str, Path]) -> dict:
        """
        Take a consistent point-in-time snapshot of the database.
        Flushes active MemTable to disk and copies immutable SSTables with checksum manifest.
        """
        backup_path = Path(target_dir)
        backup_path.mkdir(parents=True, exist_ok=True)

        with engine._lock:
            # 1. Flush any pending memory mutations to SSTable
            engine._flush_memtable()

            # 2. Gather active SSTables
            sst_files = [s.filepath for s in engine.sstables if s.filepath.exists()]
            files_manifest: List[Dict[str, Union[str, int]]] = []

            for sst in sst_files:
                dest = backup_path / sst.name
                shutil.copy2(sst, dest)
                sha = cls._compute_sha256(dest)
                files_manifest.append({
                    "filename": sst.name,
                    "size_bytes": dest.stat().st_size,
                    "sha256": sha,
                })

            manifest = {
                "format_version": "1.0",
                "timestamp_ms": int(time.time() * 1000),
                "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "sstable_count": len(files_manifest),
                "total_bytes": sum(int(f["size_bytes"]) for f in files_manifest),
                "files": files_manifest,
                "engine_stats": engine.stats(),
            }

            manifest_file = backup_path / "manifest.json"
            with open(manifest_file, "w", encoding="utf-8") as f:
                json.dump(manifest, f, indent=2)

            return manifest

    @classmethod
    def restore_snapshot(cls, engine: "ChronosEngine", source_dir: Union[str, Path]) -> dict:
        """
        Restore the database from a snapshot directory after verifying checksum integrity.
        """
        source_path = Path(source_dir)
        manifest_file = source_path / "manifest.json"
        if not manifest_file.exists():
            raise FileNotFoundError(f"Missing snapshot manifest at {manifest_file}")

        with open(manifest_file, "r", encoding="utf-8") as f:
            manifest = json.load(f)

        # Verify checksums before modifying database state
        for item in manifest.get("files", []):
            file_path = source_path / str(item["filename"])
            if not file_path.exists():
                raise FileNotFoundError(f"Snapshot file missing: {file_path}")
            actual_sha = cls._compute_sha256(file_path)
            if actual_sha != item["sha256"]:
                raise ValueError(
                    f"Integrity check failed for {item['filename']}: expected {item['sha256']}, got {actual_sha}"
                )

        with engine._lock:
            # Close existing SSTables and clear memtable & WAL
            for sst in engine.sstables:
                sst.close()
            engine.sstables.clear()
            engine.memtable.clear()
            engine.wal.truncate()
            engine.cache.clear()
            engine.ttl.clear()

            # Remove existing SSTables in data directory
            for old_sst in engine.data_dir.glob("*.sst"):
                try:
                    old_sst.unlink()
                except OSError:
                    pass

            # Copy files from snapshot to data directory
            for item in manifest.get("files", []):
                src = source_path / str(item["filename"])
                dst = engine.data_dir / str(item["filename"])
                shutil.copy2(src, dst)

            # Reload SSTables
            engine._load_existing_sstables()

            return {
                "status": "success",
                "restored_sstables": len(engine.sstables),
                "timestamp_ms": manifest.get("timestamp_ms"),
            }
