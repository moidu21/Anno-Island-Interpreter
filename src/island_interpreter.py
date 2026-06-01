from __future__ import annotations

import ctypes
import hashlib
import json
import os
import struct
import subprocess
import sys
import threading
import time
import zlib
import shutil
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Callable, Iterable, Optional

APP_TITLE = "Anno Island Interpreter"
DECODE_MODE = "decode"
ENCODE_MODE = "encode"


class InterpreterError(RuntimeError):
    pass


@dataclass
class InterpreterConfig:
    source_island_dir: Path
    output_root_dir: Path
    mode: str = DECODE_MODE
    exclude_river: bool = False
    max_workers: int | None = None
    detailed_log: bool = False
    fast_zip: bool = True
    ctt_zlib_level: int = 1
    ctt_hash_audit: bool = False
    ctt_serial: bool = False


@dataclass
class WorkRecord:
    source: str
    target: str = ""
    status: str = ""
    note: str = ""


@dataclass
class InterpreterReport:
    mode: str
    source_island: str
    output_dir: str
    a7m_files_seen: int = 0
    a7m_files_extracted: int = 0
    tmc_files_seen: int = 0
    tmc_files_decoded: int = 0
    tmc_files_encoded: int = 0
    ctt_files_seen: int = 0
    ctt_files_decoded: int = 0
    ctt_files_encoded: int = 0
    a7minfo_files_seen: int = 0
    a7minfo_files_decoded: int = 0
    generic_files_decoded: int = 0
    dds_files_copied: int = 0
    png_files_copied: int = 0
    a7me_files_copied: int = 0
    tintmap_files_copied: int = 0
    files_encoded: int = 0
    a7m_files_packed: int = 0
    skipped_encode_records: int = 0
    records: list[dict] = field(default_factory=list)
    river_filter_enabled: bool = False
    river_paths_removed: int = 0
    river_dirs_removed: int = 0
    river_files_removed: int = 0
    notes: list[str] = field(default_factory=list)
    max_workers_used: int = 1
    duration_seconds: float = 0.0
    isolated_workdirs_used: int = 0
    fast_zip_enabled: bool = True
    detailed_log_enabled: bool = False
    ctt_unique_fdbr_sizes: list[int] = field(default_factory=list)
    ctt_unique_fdbr_hashes: int = 0
    ctt_same_fdbr_size_notice: bool = False
    ctt_unique_zlib_sizes: list[int] = field(default_factory=list)
    ctt_unique_zlib_hashes: int = 0
    ctt_same_zlib_size_notice: bool = False
    v13_gamedata_scan_enabled: bool = False
    v13_mesh_xml_tasks_added: int = 0
    v13_normalmap_xml_tasks_added: int = 0
    v13_tintmap_extra_files_copied: int = 0
    v13_targeted_scan_enabled: bool = False
    ctt_parallel_encode_enabled: bool = True
    ctt_zlib_level: int = 1
    ctt_hash_audit_enabled: bool = False


class ResourceResolver:
    def __init__(self) -> None:
        if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
            self.base_dir = Path(sys._MEIPASS)
        else:
            self.base_dir = Path(__file__).resolve().parent.parent
        self.third_party = self.base_dir / "third_party"
        self.tools_dir = self.third_party / "tools"
        self.helpers_dir = self.third_party / "helpers"


class IslandInterpreter:
    def __init__(self, cfg: InterpreterConfig, resources: ResourceResolver, log: Callable[[str], None]) -> None:
        self.cfg = cfg
        self.resources = resources
        self.log = log
        self.source_name = cfg.source_island_dir.name
        self._hidden_console_allocated = False
        self._hidden_console_ready = False
        self._current_output_dir: Path | None = None
        self._river_filtered_roots: set[Path] = set()
        self._report_lock = threading.RLock()
        self._path_locks: dict[Path, threading.Lock] = {}
        self._path_locks_guard = threading.Lock()
        self._max_workers = self._resolve_max_workers(cfg.max_workers)
        self._fileformat_cache: dict[str, Optional[Path]] = {}
        self._interpreter_cache: dict[str, Optional[Path]] = {}
        self._task_dir_counter = 0
        self._task_dir_lock = threading.Lock()
        # Mode secours disponible si un environnement particulier rend FileDBReader instable en parallèle.
        # Par défaut, les CTT restent parallèles: chaque tâche a son propre dossier de travail.
        env_ctt_serial = os.environ.get("ANNO_CTT_SERIAL", "").strip().lower() in {"1", "true", "yes", "on"}
        self._ctt_serial = bool(cfg.ctt_serial or env_ctt_serial)
        self._ctt_filedb_lock = threading.Lock()
        self._ctt_audit_lock = threading.Lock()
        self._ctt_fdbr_sizes: set[int] = set()
        self._ctt_fdbr_hashes: set[str] = set()
        self._ctt_zlib_sizes: set[int] = set()
        self._ctt_zlib_hashes: set[str] = set()
        self.report = InterpreterReport(
            mode=cfg.mode,
            source_island=self.source_name,
            output_dir="",
            river_filter_enabled=bool(cfg.exclude_river and cfg.mode == DECODE_MODE),
            max_workers_used=self._max_workers,
            fast_zip_enabled=bool(cfg.fast_zip),
            detailed_log_enabled=bool(cfg.detailed_log),
            v13_targeted_scan_enabled=True,
            ctt_parallel_encode_enabled=not self._ctt_serial,
            ctt_zlib_level=int(cfg.ctt_zlib_level),
            ctt_hash_audit_enabled=bool(cfg.ctt_hash_audit or cfg.detailed_log),
        )

    def run(self) -> Path:
        started_at = time.perf_counter()
        self._validate_inputs()
        out_dir = self._prepare_output_dir()
        self._current_output_dir = out_dir
        self.report.output_dir = str(out_dir)
        self._add_note(f"Scan _gamedata + Header FDBR: traitement parallèle avec {self._max_workers} thread(s) de travail.")
        if self.cfg.fast_zip and self.cfg.mode == DECODE_MODE:
            self._add_note("ZIP final en compression rapide niveau 1 pour réduire le temps de finalisation.")
        if self.cfg.mode == ENCODE_MODE:
            with self._report_lock:
                self.report.v13_gamedata_scan_enabled = True
            if self._ctt_serial:
                self._add_note("Sécurité CTT: mode secours série activé via configuration/ANNO_CTT_SERIAL. Plus sûr, mais plus lent.")
            else:
                self._add_note("Performance CTT: compression FileDBReader relancée en parallèle avec dossiers isolés par XML; header _CTT basé sur la taille FDBR décompressée.")
            self._add_note("Scan ciblé _gamedata réactivé: meshes/*.xml -> .tmc, normalmaps/*.xml -> .ctt, tintmaps/* -> copie. Les CTT restent en header propre _CTT + taille_FDBR.")
        if not self.cfg.detailed_log:
            self._add_note("Journal détaillé par fichier désactivé pour éviter de ralentir les gros lots.")

        if self.cfg.mode == DECODE_MODE:
            self._run_decode(out_dir)
        elif self.cfg.mode == ENCODE_MODE:
            self._run_encode(out_dir)
        else:
            raise InterpreterError(f"Mode inconnu: {self.cfg.mode}")

        self._finalize_ctt_audit()
        self.report.duration_seconds = round(time.perf_counter() - started_at, 3)
        report_path = out_dir / f"{out_dir.name}_report.json"
        if self.cfg.mode == DECODE_MODE:
            archive_path = self._get_decode_archive_path(out_dir)
            self._add_note(f"Archive finale du décodage: {archive_path.name}")
        report_path.write_text(json.dumps(asdict(self.report), indent=2, ensure_ascii=False), encoding="utf-8")
        self.log(f"Rapport généré: {report_path}")

        if self.cfg.mode == DECODE_MODE:
            return self._finalize_decode_archive(out_dir)
        return out_dir

    def _run_decode(self, out_dir: Path) -> None:
        self._prepare_decode_filters()
        source_files = self._scan_source_files()
        self.log(f"Scan initial terminé: {len(source_files)} fichier(s) utile(s) détecté(s).")
        tmp_root = out_dir / "__v12_parallel_tmp"
        tmp_root.mkdir(parents=True, exist_ok=True)
        try:
            self._decode_a7m_files(out_dir, source_files, tmp_root)
            self._decode_a7minfo_files(out_dir, source_files, tmp_root)
            self._decode_tmc_files(out_dir, source_files, tmp_root)
            self._decode_ctt_files(out_dir, source_files, tmp_root)
            self._decode_generic_files(out_dir, source_files, tmp_root)
            self._copy_passthrough_assets(out_dir, source_files)
        finally:
            shutil.rmtree(tmp_root, ignore_errors=True)

        removed = self._cleanup_intermediates(out_dir)
        if removed:
            self.log(f"Fichiers temporaires nettoyés automatiquement: {removed} élément(s)")

    def _run_encode(self, out_dir: Path) -> None:
        work_root = out_dir / "__encode_work"
        if work_root.exists():
            shutil.rmtree(work_root, ignore_errors=True)
        work_root.mkdir(parents=True, exist_ok=True)

        source_report = self._load_source_report()
        if source_report is not None:
            report_path, report_data = source_report
            self.log(f"Rapport source détecté pour le recodage: {report_path.name}")
            self._encode_from_report(report_data, out_dir, work_root)
        else:
            self._add_note(
                "Aucun rapport d'interprétation détecté dans le dossier source. "
                "Recodage heuristique des bundles .a7m + scan direct de _gamedata."
            )
            self.log("Aucun rapport source trouvé ; tentative heuristique des bundles .a7m + scan _gamedata.")
            self._encode_a7m_heuristic(out_dir, work_root)
            self._encode_v13_gamedata_scan(out_dir, work_root)

        self._copy_passthrough_assets(out_dir)
        shutil.rmtree(work_root, ignore_errors=True)

    def _resolve_max_workers(self, requested: int | None) -> int:
        if requested is not None and requested > 0:
            return max(1, min(int(requested), 32))
        cpu_count = os.cpu_count() or 1
        return max(1, min(cpu_count, 32))

    def _inc_report(self, field_name: str, amount: int = 1) -> None:
        with self._report_lock:
            setattr(self.report, field_name, getattr(self.report, field_name) + amount)

    def _add_note(self, note: str) -> None:
        with self._report_lock:
            self.report.notes.append(note)

    def _log_detail(self, message: str) -> None:
        if self.cfg.detailed_log:
            self.log(message)

    def _make_task_dir(self, root: Path, category: str, source: Path | str) -> Path:
        with self._task_dir_lock:
            self._task_dir_counter += 1
            idx = self._task_dir_counter
        safe = self._safe_task_name(source)
        task_dir = root / category / f"{idx:06d}_{safe}"
        task_dir.mkdir(parents=True, exist_ok=True)
        self._inc_report("isolated_workdirs_used")
        return task_dir

    def _safe_task_name(self, source: Path | str) -> str:
        text = source.as_posix() if isinstance(source, Path) else str(source)
        cleaned = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in text)[-80:]
        return cleaned or "task"

    def _get_path_lock(self, cwd: Path) -> threading.Lock:
        key = cwd.resolve()
        with self._path_locks_guard:
            lock = self._path_locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._path_locks[key] = lock
            return lock

    def _scan_source_files(self) -> list[Path]:
        base = self.cfg.source_island_dir
        files: list[Path] = []
        for root, dirnames, filenames in os.walk(base):
            root_path = Path(root)
            if self.cfg.mode == DECODE_MODE and self.cfg.exclude_river:
                dirnames[:] = [
                    dirname for dirname in dirnames
                    if not self._should_skip_source_path(root_path / dirname)
                ]
            for filename in filenames:
                path = root_path / filename
                if not self._should_skip_source_path(path):
                    files.append(path)
        return sorted(files)

    def _run_parallel(self, label: str, items: list[Path] | list[tuple], worker: Callable, *, max_workers: int | None = None) -> None:
        if not items:
            return
        worker_count = max_workers or self._max_workers
        worker_count = max(1, min(worker_count, len(items)))
        if worker_count <= 1:
            for item in items:
                worker(item)
            return

        self.log(f"{label}: {len(items)} tâche(s) avec {worker_count} thread(s).")
        errors: list[str] = []
        with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="anno-v13") as executor:
            future_to_item = {executor.submit(worker, item): item for item in items}
            for future in as_completed(future_to_item):
                item = future_to_item[future]
                try:
                    future.result()
                except Exception as exc:
                    errors.append(f"{item}: {exc}")
        if errors:
            preview = "\n".join(errors[:5])
            if len(errors) > 5:
                preview += f"\n... {len(errors) - 5} autre(s) erreur(s)"
            raise InterpreterError(f"{label}: {len(errors)} tâche(s) en échec.\n{preview}")

    def _validate_inputs(self) -> None:
        if not self.cfg.source_island_dir.exists() or not self.cfg.source_island_dir.is_dir():
            raise InterpreterError(f"Dossier source introuvable: {self.cfg.source_island_dir}")
        if not self.cfg.output_root_dir.exists() or not self.cfg.output_root_dir.is_dir():
            raise InterpreterError(f"Dossier de sortie introuvable: {self.cfg.output_root_dir}")
        if self.cfg.mode not in {DECODE_MODE, ENCODE_MODE}:
            raise InterpreterError(f"Mode non pris en charge: {self.cfg.mode}")
        if not self._find_executable("FileDBReader").exists():
            raise InterpreterError("FileDBReader.exe est introuvable dans third_party/tools.")
        if not self._find_executable("RdaConsole").exists():
            raise InterpreterError("RdaConsole.exe est introuvable dans third_party/tools.")

    def _prepare_output_dir(self) -> Path:
        if self.cfg.mode == DECODE_MODE:
            out_name = f"{self.source_name}__interpreted"
        else:
            base_name = self.source_name
            if base_name.endswith("__interpreted"):
                base_name = base_name[: -len("__interpreted")]
            out_name = f"{base_name}__reencoded"
        out_dir = self.cfg.output_root_dir / out_name
        if out_dir.exists():
            shutil.rmtree(out_dir, ignore_errors=True)
        out_dir.mkdir(parents=True, exist_ok=True)
        self.log(f"Création du dossier de sortie: {out_dir}")
        return out_dir

    def _get_decode_archive_path(self, out_dir: Path) -> Path:
        return out_dir.parent / f"{out_dir.name}.zip"

    def _finalize_decode_archive(self, out_dir: Path) -> Path:
        archive_path = self._get_decode_archive_path(out_dir)
        archive_path.unlink(missing_ok=True)
        self.log(f"Création de l'archive ZIP finale: {archive_path}")
        if self.cfg.fast_zip:
            self._make_fast_zip(out_dir, archive_path)
        else:
            archive_base = archive_path.with_suffix("")
            shutil.make_archive(str(archive_base), "zip", root_dir=out_dir.parent, base_dir=out_dir.name)
        shutil.rmtree(out_dir, ignore_errors=True)
        self.log(f"Dossier interprété supprimé après archivage: {out_dir}")
        return archive_path

    def _make_fast_zip(self, source_dir: Path, archive_path: Path) -> None:
        # Niveau 1 : nettement plus rapide que le ZIP deflate par défaut, tout en gardant une archive compatible.
        compression = zipfile.ZIP_DEFLATED
        with zipfile.ZipFile(archive_path, "w", compression=compression, compresslevel=1) as zf:
            for path in sorted(source_dir.rglob("*")):
                if path.is_file():
                    zf.write(path, path.relative_to(source_dir.parent))

    def _prepare_decode_filters(self) -> None:
        self._river_filtered_roots.clear()
        if not (self.cfg.mode == DECODE_MODE and self.cfg.exclude_river):
            return

        removed_dirs = 0
        removed_files = 0
        base = self.cfg.source_island_dir
        for root, dirnames, filenames in os.walk(base):
            root_path = Path(root)

            kept_dirs: list[str] = []
            for dirname in dirnames:
                path = root_path / dirname
                try:
                    rel = path.relative_to(base)
                except ValueError:
                    continue
                if self._name_contains_river(dirname):
                    self._river_filtered_roots.add(rel)
                    removed_dirs += 1
                else:
                    kept_dirs.append(dirname)
            dirnames[:] = kept_dirs

            for filename in filenames:
                if not self._name_contains_river(filename):
                    continue
                path = root_path / filename
                try:
                    rel = path.relative_to(base)
                except ValueError:
                    continue
                self._river_filtered_roots.add(rel)
                removed_files += 1

        total_removed = removed_dirs + removed_files
        with self._report_lock:
            self.report.river_dirs_removed = removed_dirs
            self.report.river_files_removed = removed_files
            self.report.river_paths_removed = total_removed

        if total_removed:
            self._add_note(
                f"Filtre river activé: {removed_dirs} dossier(s) et {removed_files} fichier(s) exclus avant décodage."
            )
            self.log(
                f"Filtre river activé: {removed_dirs} dossier(s) et {removed_files} fichier(s) exclus avant décodage."
            )
        else:
            self._add_note("Filtre river activé: aucun fichier ou dossier contenant 'river' trouvé.")
            self.log("Filtre river activé: aucun fichier ou dossier contenant 'river' trouvé.")

    def _name_contains_river(self, value: str) -> bool:
        return "river" in value.lower()

    def _should_skip_source_path(self, path: Path) -> bool:
        if not (self.cfg.mode == DECODE_MODE and self.cfg.exclude_river):
            return False
        try:
            rel = path.relative_to(self.cfg.source_island_dir)
        except ValueError:
            return False
        return any(root == rel or root in rel.parents for root in self._river_filtered_roots)

    def _is_ctt_like_record(self, source_rel: Path, xml_rel: Path | None = None) -> bool:
        candidates = [source_rel.as_posix().lower()]
        if xml_rel is not None:
            candidates.append(xml_rel.as_posix().lower())
        return source_rel.suffix.lower() == ".ctt" or any("normalmap" in candidate for candidate in candidates)

    def _rel_parts_lower(self, rel: Path) -> list[str]:
        return [part.lower() for part in rel.parts]

    def _is_under_gamedata(self, rel: Path) -> bool:
        return "_gamedata" in self._rel_parts_lower(rel)

    def _is_under_named_folder(self, rel: Path, folder_name: str) -> bool:
        return folder_name.lower() in self._rel_parts_lower(rel)

    def _is_v13_mesh_xml(self, rel: Path) -> bool:
        return (
            rel.suffix.lower() == ".xml"
            and self._is_under_gamedata(rel)
            and self._is_under_named_folder(rel, "meshes")
        )

    def _is_v13_normalmap_xml(self, rel: Path) -> bool:
        return (
            rel.suffix.lower() == ".xml"
            and self._is_under_gamedata(rel)
            and self._is_under_named_folder(rel, "normalmaps")
        )

    def _is_v13_tintmap_file(self, rel: Path) -> bool:
        return (
            self._is_under_gamedata(rel)
            and self._is_under_named_folder(rel, "tintmaps")
        )

    def _iter_gamedata_roots(self) -> list[Path]:
        direct = self.cfg.source_island_dir / "_gamedata"
        if direct.exists() and direct.is_dir():
            return [direct]
        roots: list[Path] = []
        for root, dirnames, _filenames in os.walk(self.cfg.source_island_dir):
            root_path = Path(root)
            # Ne descend pas dans les dossiers de travail ou caches éventuels.
            dirnames[:] = [d for d in dirnames if d not in {"__encode_work", "__decode_tmp", "__pycache__"}]
            for dirname in list(dirnames):
                if dirname.lower() == "_gamedata":
                    roots.append(root_path / dirname)
            if roots:
                # Dans un dossier d'île normal il n'y a qu'un _gamedata; éviter de scanner plus loin.
                dirnames[:] = []
        return roots

    def _scan_v13_gamedata_xml_tasks(
        self,
        known_ctt_outputs: set[str],
        known_standalone_outputs: set[str],
    ) -> tuple[list[tuple[Path, Path]], list[tuple[Path, Path]]]:
        ctt_tasks: list[tuple[Path, Path]] = []
        mesh_tasks: list[tuple[Path, Path]] = []
        seen_ctt = set(known_ctt_outputs)
        seen_mesh = set(known_standalone_outputs)
        wanted_dirs = {"normalmaps", "meshes"}

        for gamedata_root in self._iter_gamedata_roots():
            for root, dirnames, filenames in os.walk(gamedata_root):
                root_path = Path(root)
                parts_lower = {part.lower() for part in root_path.relative_to(self.cfg.source_island_dir).parts}
                inside_target = bool(parts_lower & wanted_dirs)
                if not inside_target:
                    # Avant d'être dans normalmaps/meshes, on ne garde que les branches pouvant y mener.
                    dirnames[:] = [d for d in dirnames if d.lower() in wanted_dirs or d.lower() not in {"tintmaps"}]
                for filename in filenames:
                    if not filename.lower().endswith(".xml"):
                        continue
                    src = root_path / filename
                    rel = src.relative_to(self.cfg.source_island_dir)
                    parts = self._rel_parts_lower(rel)
                    if "normalmaps" in parts:
                        source_rel = rel.with_suffix(".ctt")
                        key = source_rel.as_posix().lower()
                        if key not in seen_ctt:
                            seen_ctt.add(key)
                            ctt_tasks.append((source_rel, rel))
                    elif "meshes" in parts:
                        source_rel = rel.with_suffix(".tmc")
                        key = source_rel.as_posix().lower()
                        if key not in seen_mesh:
                            seen_mesh.add(key)
                            mesh_tasks.append((source_rel, rel))
        return ctt_tasks, mesh_tasks

    def _scan_passthrough_assets_fast(self) -> list[Path]:
        passthrough_exts = {".dds", ".png", ".a7me"}
        items: list[Path] = []
        seen: set[str] = set()
        for root, dirnames, filenames in os.walk(self.cfg.source_island_dir):
            root_path = Path(root)
            dirnames[:] = [d for d in dirnames if d not in {"__encode_work", "__decode_tmp", "__pycache__"}]
            try:
                root_rel = root_path.relative_to(self.cfg.source_island_dir)
            except ValueError:
                root_rel = Path()
            root_is_tintmap = self._is_v13_tintmap_file(root_rel)
            for filename in filenames:
                path = root_path / filename
                suffix = path.suffix.lower()
                if not root_is_tintmap and suffix not in passthrough_exts:
                    continue
                rel = path.relative_to(self.cfg.source_island_dir)
                key = rel.as_posix().lower()
                if key in seen:
                    continue
                seen.add(key)
                items.append(path)
        return items

    def _record(self, source: Path | str, target: Path | str | None, status: str, note: str = "") -> None:
        rec = WorkRecord(
            source=self._format_source_ref(source),
            target=self._format_target_ref(target),
            status=status,
            note=note,
        )
        with self._report_lock:
            self.report.records.append(asdict(rec))

    def _format_source_ref(self, source: Path | str) -> str:
        if isinstance(source, str):
            return source
        try:
            return str(source.relative_to(self.cfg.source_island_dir))
        except ValueError:
            return str(source)

    def _format_target_ref(self, target: Path | str | None) -> str:
        if target is None:
            return ""
        if isinstance(target, str):
            return target
        for base in (self._current_output_dir, self.cfg.source_island_dir):
            if base is None:
                continue
            try:
                return str(target.relative_to(base))
            except ValueError:
                continue
        return str(target)

    def _mirror_path(self, source_path: Path, output_root: Path) -> Path:
        return output_root / source_path.relative_to(self.cfg.source_island_dir)

    def _find_executable(self, stem: str) -> Path:
        base = self.resources.tools_dir / stem
        candidates = [base / f"{stem}.exe"]
        for candidate in candidates:
            if candidate.exists():
                return candidate
        return candidates[0]

    def _find_helper(self, filename: str) -> Path:
        candidate = self.resources.helpers_dir / filename
        if candidate.exists():
            return candidate
        raise InterpreterError(f"Helper introuvable: {filename}")

    def _find_filedb_fileformat(self, filename: str) -> Optional[Path]:
        cached = self._fileformat_cache.get(filename)
        if filename in self._fileformat_cache:
            return cached
        filedb_dir = self._find_executable("FileDBReader").parent
        for candidate in (filedb_dir / "FileFormats" / filename, filedb_dir / filename):
            if candidate.exists():
                self._fileformat_cache[filename] = candidate
                return candidate
        for path in filedb_dir.rglob(filename):
            if path.exists():
                self._fileformat_cache[filename] = path
                return path
        self._fileformat_cache[filename] = None
        return None

    def _find_interpreter_for_extension(self, ext: str) -> Optional[Path]:
        normalized = ext.lower()
        cached = self._interpreter_cache.get(normalized)
        if normalized in self._interpreter_cache:
            return cached
        if normalized == ".a7minfo":
            result = self._find_helper("a7minfo.xml")
        elif normalized == ".tmc":
            result = self._find_helper("tmc.xml")
        elif normalized == ".ctt":
            result = self._find_filedb_fileformat("ctt.xml")
        else:
            key = normalized[1:] if normalized.startswith(".") else normalized
            result = self._find_filedb_fileformat(f"{key}.xml")
        self._interpreter_cache[normalized] = result
        return result

    def _use_hidden_console_mode(self) -> bool:
        return bool(getattr(sys, "frozen", False)) and sys.platform.startswith("win")

    def _build_windows_startupinfo(self):
        if not sys.platform.startswith("win"):
            return None
        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        startupinfo.wShowWindow = 0
        return startupinfo

    def _ensure_hidden_console(self) -> None:
        if not self._use_hidden_console_mode() or self._hidden_console_ready:
            return
        try:
            kernel32 = ctypes.windll.kernel32
            user32 = ctypes.windll.user32
            get_console_window = getattr(kernel32, "GetConsoleWindow")
            hwnd = get_console_window()
            if not hwnd:
                if not kernel32.AllocConsole():
                    raise ctypes.WinError()
                hwnd = get_console_window()
                self._hidden_console_allocated = True
            if hwnd:
                user32.ShowWindow(hwnd, 0)
            self._hidden_console_ready = True
        except Exception as exc:
            self._add_note(f"Console cachée non initialisée: {exc}")

    def _run_cmd(self, cmd: list[str], cwd: Path) -> str:
        with self._get_path_lock(cwd):
            if self._use_hidden_console_mode():
                self._ensure_hidden_console()
                completed = subprocess.run(
                    cmd,
                    cwd=str(cwd),
                    text=True,
                    shell=False,
                    check=False,
                    startupinfo=self._build_windows_startupinfo(),
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                )
            else:
                flags = 0
                if hasattr(subprocess, "CREATE_NO_WINDOW"):
                    flags = subprocess.CREATE_NO_WINDOW
                completed = subprocess.run(
                    cmd,
                    cwd=str(cwd),
                    capture_output=True,
                    text=True,
                    shell=False,
                    check=False,
                    creationflags=flags,
                )
            output = (completed.stdout or "") + (completed.stderr or "") if not self._use_hidden_console_mode() else (completed.stdout or "")
            if completed.returncode != 0:
                raise InterpreterError(f"Commande en échec dans {cwd}: {' '.join(cmd)}\n{output.strip()}")
            return output

    def _run_rda_console(self, cmd: list[str], cwd: Path, success_paths: Iterable[Path]) -> None:
        success_paths = list(success_paths)
        with self._get_path_lock(cwd):
            if self._use_hidden_console_mode():
                self._ensure_hidden_console()
                completed = subprocess.run(
                    cmd,
                    cwd=str(cwd),
                    text=True,
                    shell=False,
                    check=False,
                    startupinfo=self._build_windows_startupinfo(),
                    stdin=subprocess.DEVNULL,
                )
                output = ""
            else:
                flags = 0
                if hasattr(subprocess, "CREATE_NO_WINDOW"):
                    flags = subprocess.CREATE_NO_WINDOW
                completed = subprocess.run(
                    cmd,
                    cwd=str(cwd),
                    capture_output=True,
                    text=True,
                    shell=False,
                    check=False,
                    creationflags=flags,
                )
                output = (completed.stderr or completed.stdout or "").strip()
            if completed.returncode == 0:
                return

            existing = [path for path in success_paths if path.exists() and (path.is_dir() or path.stat().st_size > 0)]
            if len(existing) == len(success_paths):
                self._add_note("RdaConsole a retourné un code d'erreur mais les fichiers attendus existent ; poursuite du traitement.")
                return
            raise InterpreterError(f"Commande en échec dans {cwd}: {' '.join(cmd)}\n{output or f'Code retour: {completed.returncode}'}")

    def _decode_a7m_files(self, out_dir: Path, source_files: list[Path], tmp_root: Path) -> None:
        filedb = self._find_executable("FileDBReader")
        rda = self._find_executable("RdaConsole")
        rd3d_fmt = self._find_helper("Island_RD3D.xml")
        gamedata_fmt = self._find_helper("Island_Gamedata_v3.xml")
        items = [path for path in source_files if path.suffix.lower() == ".a7m"]

        def worker(a7m: Path) -> None:
            self._inc_report("a7m_files_seen")
            rel = a7m.relative_to(self.cfg.source_island_dir)
            final_extract_dir = out_dir / rel.parent / a7m.stem
            task_dir = self._make_task_dir(tmp_root, "a7m", rel)
            work_a7m = task_dir / a7m.name
            shutil.copy2(a7m, work_a7m)
            extract_dir = task_dir / a7m.stem
            self._log_detail(f"Extraction du .a7m: {rel}")
            try:
                self._run_rda_console(
                    [str(rda), "extract", "-f", work_a7m.name, "-o", a7m.stem, "-y"],
                    cwd=task_dir,
                    success_paths=[extract_dir / "rd3d.data", extract_dir / "gamedata.data"],
                )

                decoded_any = False
                for data_name, interp in (("rd3d.data", rd3d_fmt), ("gamedata.data", gamedata_fmt)):
                    data_path = extract_dir / data_name
                    if not data_path.exists():
                        continue
                    self._run_cmd([
                        str(filedb), "decompress", "-f", data_name, "-i", str(interp), "-y"
                    ], cwd=extract_dir)
                    xml_path = extract_dir / f"{data_path.stem}.xml"
                    final_xml = final_extract_dir / xml_path.name
                    if xml_path.exists():
                        final_extract_dir.mkdir(parents=True, exist_ok=True)
                        shutil.move(str(xml_path), str(final_xml))
                        decoded_any = True
                    self._record(a7m, final_xml if final_xml.exists() else None, "decoded" if final_xml.exists() else "failed", f"depuis {data_name}")

                if decoded_any:
                    self._inc_report("a7m_files_extracted")
            finally:
                shutil.rmtree(task_dir, ignore_errors=True)

        self._run_parallel("Décodage .a7m", items, worker)

    def _decode_a7minfo_files(self, out_dir: Path, source_files: list[Path], tmp_root: Path) -> None:
        filedb = self._find_executable("FileDBReader")
        interp = self._find_helper("a7minfo.xml")
        items = [path for path in source_files if path.suffix.lower() == ".a7minfo"]

        def worker(src: Path) -> None:
            self._inc_report("a7minfo_files_seen")
            rel = src.relative_to(self.cfg.source_island_dir)
            task_dir = self._make_task_dir(tmp_root, "a7minfo", rel)
            work_src = task_dir / src.name
            final_xml = (out_dir / rel).with_suffix(".xml")
            shutil.copy2(src, work_src)
            self._log_detail(f"Interprétation a7minfo: {rel}")
            try:
                self._run_cmd([
                    str(filedb), "decompress", "-f", work_src.name, "-i", str(interp), "-y"
                ], cwd=task_dir)
                xml = work_src.with_suffix(".xml")
                if xml.exists():
                    final_xml.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(xml), str(final_xml))
                    self._inc_report("a7minfo_files_decoded")
                self._record(src, final_xml if final_xml.exists() else None, "decoded" if final_xml.exists() else "failed")
            finally:
                shutil.rmtree(task_dir, ignore_errors=True)

        self._run_parallel("Décodage .a7minfo", items, worker)

    def _decode_tmc_files(self, out_dir: Path, source_files: list[Path], tmp_root: Path) -> None:
        filedb = self._find_executable("FileDBReader")
        interp = self._find_helper("tmc.xml")
        items = [path for path in source_files if path.suffix.lower() == ".tmc"]

        def worker(src: Path) -> None:
            self._inc_report("tmc_files_seen")
            rel = src.relative_to(self.cfg.source_island_dir)
            task_dir = self._make_task_dir(tmp_root, "tmc", rel)
            work_src = task_dir / src.name
            final_xml = (out_dir / rel).with_suffix(".xml")
            shutil.copy2(src, work_src)
            self._log_detail(f"Interprétation TMC: {rel}")
            try:
                self._run_cmd([
                    str(filedb), "decompress", "-f", work_src.name, "-i", str(interp), "-y"
                ], cwd=task_dir)
                xml = work_src.with_suffix(".xml")
                if xml.exists():
                    final_xml.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(xml), str(final_xml))
                    self._inc_report("tmc_files_decoded")
                self._record(src, final_xml if final_xml.exists() else None, "decoded" if final_xml.exists() else "failed")
            finally:
                shutil.rmtree(task_dir, ignore_errors=True)

        self._run_parallel("Décodage .tmc", items, worker)

    def _decode_ctt_files(self, out_dir: Path, source_files: list[Path], tmp_root: Path) -> None:
        filedb = self._find_executable("FileDBReader")
        interp = self._find_filedb_fileformat("ctt.xml")
        if interp is None:
            self._add_note("ctt.xml introuvable dans FileDBReader/FileFormats: les .ctt sont ignorés.")
            return
        items = [path for path in source_files if path.suffix.lower() == ".ctt"]

        def worker(src: Path) -> None:
            self._inc_report("ctt_files_seen")
            rel = src.relative_to(self.cfg.source_island_dir)
            task_dir = self._make_task_dir(tmp_root, "ctt", rel)
            stem = src.stem
            final_xml = out_dir / rel.parent / f"{stem}.xml"
            raw = src.read_bytes()
            if len(raw) < 8:
                self._record(src, None, "failed", "fichier trop court")
                shutil.rmtree(task_dir, ignore_errors=True)
                return
            header = raw[:8]
            payload = raw[8:]
            header_path = task_dir / f"{stem}.header8.bin"
            zlib_path = task_dir / f"{stem}.payload.zlib"
            fdbr_path = task_dir / f"{stem}.fdbr"
            xml_path = task_dir / f"{stem}.xml"
            header_path.write_bytes(header)
            zlib_path.write_bytes(payload)
            try:
                fdbr_path.write_bytes(zlib.decompress(payload))
            except zlib.error as exc:
                self._record(src, None, "failed", f"zlib: {exc}")
                shutil.rmtree(task_dir, ignore_errors=True)
                return
            try:
                self._log_detail(f"Interprétation CTT: {rel}")
                self._run_cmd([
                    str(filedb), "decompress", "-f", fdbr_path.name, "-i", str(interp), "-y"
                ], cwd=task_dir)
                if xml_path.exists():
                    final_xml.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(xml_path), str(final_xml))
                    self._inc_report("ctt_files_decoded")
                self._record(src, final_xml if final_xml.exists() else None, "decoded" if final_xml.exists() else "failed")
            finally:
                shutil.rmtree(task_dir, ignore_errors=True)

        self._run_parallel("Décodage .ctt", items, worker)

    def _decode_generic_files(self, out_dir: Path, source_files: list[Path], tmp_root: Path) -> None:
        filedb = self._find_executable("FileDBReader")
        handled_exts = {".a7m", ".a7me", ".a7minfo", ".tmc", ".ctt", ".xml", ".dds", ".png"}
        filedb_dir = self._find_executable("FileDBReader").parent
        fileformats_dir = filedb_dir / "FileFormats"
        if not fileformats_dir.exists():
            return
        available = {path.stem.lower(): path for path in fileformats_dir.glob("*.xml")}
        items = [
            path for path in source_files
            if path.suffix.lower() not in handled_exts and available.get(path.suffix.lower().lstrip(".")) is not None
        ]

        def worker(src: Path) -> None:
            rel = src.relative_to(self.cfg.source_island_dir)
            ext = src.suffix.lower()
            interp = available[ext.lstrip(".")]
            final_xml = (out_dir / rel).with_suffix(".xml")
            if final_xml.exists():
                return
            task_dir = self._make_task_dir(tmp_root, "generic", rel)
            work_src = task_dir / src.name
            shutil.copy2(src, work_src)
            try:
                self._log_detail(f"Interprétation générique {ext}: {rel}")
                self._run_cmd([
                    str(filedb), "decompress", "-f", work_src.name, "-i", str(interp), "-y"
                ], cwd=task_dir)
            except Exception as exc:
                self._record(src, None, "skipped", f"interprète trouvé mais échec: {exc}")
                return
            finally:
                produced_xml = work_src.with_suffix(".xml")
                if produced_xml.exists():
                    final_xml.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(produced_xml), str(final_xml))
                shutil.rmtree(task_dir, ignore_errors=True)
            if final_xml.exists():
                self._inc_report("generic_files_decoded")
                self._record(src, final_xml, "decoded", "interprète générique")
            else:
                self._record(src, None, "failed", "interprète générique sans xml")

        self._run_parallel("Décodage générique", items, worker)

    def _copy_passthrough_assets(self, out_dir: Path, source_files: list[Path] | None = None) -> None:
        passthrough_exts = {".dds", ".png", ".a7me"}
        if source_files is None:
            items = self._scan_passthrough_assets_fast()
        else:
            items: list[Path] = []
            seen: set[str] = set()
            for path in source_files:
                rel = path.relative_to(self.cfg.source_island_dir)
                is_standard = path.suffix.lower() in passthrough_exts
                is_tintmap = self._is_v13_tintmap_file(rel)
                if not (is_standard or is_tintmap):
                    continue
                key = rel.as_posix().lower()
                if key in seen:
                    continue
                seen.add(key)
                items.append(path)

        def worker(src: Path) -> tuple[str, bool]:
            mirrored = self._mirror_path(src, out_dir)
            mirrored.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, mirrored)
            rel = src.relative_to(self.cfg.source_island_dir)
            return src.suffix.lower(), self._is_v13_tintmap_file(rel)

        if not items:
            return
        counts = {".dds": 0, ".png": 0, ".a7me": 0}
        tintmap_count = 0
        if self._max_workers <= 1 or len(items) == 1:
            for src in items:
                suffix, is_tintmap = worker(src)
                if suffix in counts:
                    counts[suffix] += 1
                if is_tintmap:
                    tintmap_count += 1
        else:
            worker_count = min(self._max_workers, len(items))
            self.log(f"Copie des assets passthrough/tintmaps: {len(items)} tâche(s) avec {worker_count} thread(s).")
            with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="anno-copy-v13") as executor:
                for suffix, is_tintmap in executor.map(worker, items):
                    if suffix in counts:
                        counts[suffix] += 1
                    if is_tintmap:
                        tintmap_count += 1

        if counts[".dds"]:
            self._inc_report("dds_files_copied", counts[".dds"])
            self.log(f"Copie des textures DDS: {counts['.dds']} fichier(s)")
        if counts[".png"]:
            self._inc_report("png_files_copied", counts[".png"])
            self.log(f"Copie des textures PNG: {counts['.png']} fichier(s)")
        if counts[".a7me"]:
            self._inc_report("a7me_files_copied", counts[".a7me"])
            self.log(f"Copie des fichiers A7ME: {counts['.a7me']} fichier(s)")
        if tintmap_count:
            self._inc_report("tintmap_files_copied", tintmap_count)
            with self._report_lock:
                self.report.v13_tintmap_extra_files_copied += tintmap_count
            self.log(f"Copie du dossier tintmaps: {tintmap_count} fichier(s)")

    def _load_source_report(self) -> tuple[Path, dict] | None:
        candidates = sorted(self.cfg.source_island_dir.glob("*_report.json"))
        if not candidates:
            return None
        report_path = max(candidates, key=lambda p: p.stat().st_mtime)
        try:
            return report_path, json.loads(report_path.read_text(encoding="utf-8"))
        except Exception as exc:
            self._add_note(f"Impossible de lire le rapport source {report_path.name}: {exc}")
            return None

    def _collect_v13_gamedata_tasks(
        self,
        known_ctt_outputs: set[str] | None = None,
        known_standalone_outputs: set[str] | None = None,
    ) -> tuple[list[tuple[Path, Path]], list[tuple[Path, Path]]]:
        """Ajoute les fichiers _gamedata créés après le décodage.

        Règles v13 demandées :
        - _gamedata/**/meshes/**/*.xml      -> recoder en .tmc
        - _gamedata/**/normalmaps/**/*.xml -> recoder en .ctt
        Les sets connus contiennent les chemins de sortie déjà prévus par le rapport,
        afin d'éviter de traiter deux fois les fichiers historiques.
        """
        known_ctt_outputs = known_ctt_outputs or set()
        known_standalone_outputs = known_standalone_outputs or set()
        return self._scan_v13_gamedata_xml_tasks(known_ctt_outputs, known_standalone_outputs)

    def _encode_v13_gamedata_scan(
        self,
        out_dir: Path,
        work_root: Path,
        known_ctt_outputs: set[str] | None = None,
        known_standalone_outputs: set[str] | None = None,
    ) -> None:
        ctt_extra, mesh_extra = self._collect_v13_gamedata_tasks(known_ctt_outputs, known_standalone_outputs)
        if not ctt_extra and not mesh_extra:
            self._add_note("Scan _gamedata: aucun XML supplémentaire à recoder dans meshes/normalmaps.")
            return

        with self._report_lock:
            self.report.v13_normalmap_xml_tasks_added += len(ctt_extra)
            self.report.v13_mesh_xml_tasks_added += len(mesh_extra)
        self._add_note(
            f"Scan _gamedata: {len(mesh_extra)} XML mesh(es) ajouté(s) comme .tmc, "
            f"{len(ctt_extra)} XML normalmap(s) ajouté(s) comme .ctt."
        )

        self._run_parallel(
            "Recodage _gamedata normalmaps -> CTT",
            ctt_extra,
            lambda item: self._encode_ctt_record(item[0], item[1], out_dir, work_root),
        )
        self._run_parallel(
            "Recodage _gamedata meshes -> TMC",
            mesh_extra,
            lambda item: self._encode_standalone_record(item[0], item[1], out_dir, work_root),
        )

    def _encode_from_report(self, report_data: dict, out_dir: Path, work_root: Path) -> None:
        records = report_data.get("records") or []
        if not isinstance(records, list) or not records:
            self._add_note("Le rapport source ne contient aucun enregistrement exploitable pour le recodage. Scan v13 _gamedata activé.")
            self._encode_a7m_heuristic(out_dir, work_root)
            self._encode_v13_gamedata_scan(out_dir, work_root)
            return

        a7m_groups: dict[str, list[tuple[Path, str]]] = {}
        ctt_tasks: list[tuple[Path, Path]] = []
        standalone_tasks: list[tuple[Path, Path]] = []
        known_ctt_outputs: set[str] = set()
        known_standalone_outputs: set[str] = set()

        for rec in records:
            if not isinstance(rec, dict):
                continue
            source_str = str(rec.get("source") or "").strip()
            target_str = str(rec.get("target") or rec.get("xml") or "").strip()
            note = str(rec.get("note") or "")
            if not source_str or not target_str:
                continue
            source_rel = Path(source_str)
            target_rel = Path(target_str)
            if not (self.cfg.source_island_dir / target_rel).exists():
                self._inc_report("skipped_encode_records")
                self._record(source_rel, target_rel, "skipped", "xml source introuvable pour le recodage")
                continue

            if self._is_ctt_like_record(source_rel, target_rel):
                key = source_rel.as_posix().lower()
                known_ctt_outputs.add(key)
                ctt_tasks.append((source_rel, target_rel))
                continue

            if source_rel.suffix.lower() == ".a7m":
                a7m_groups.setdefault(source_rel.as_posix(), []).append((target_rel, note))
                continue

            key = source_rel.as_posix().lower()
            known_standalone_outputs.add(key)
            standalone_tasks.append((source_rel, target_rel))

        ctt_extra, mesh_extra = self._collect_v13_gamedata_tasks(known_ctt_outputs, known_standalone_outputs)
        if ctt_extra or mesh_extra:
            with self._report_lock:
                self.report.v13_normalmap_xml_tasks_added += len(ctt_extra)
                self.report.v13_mesh_xml_tasks_added += len(mesh_extra)
            self._add_note(
                f"Scan _gamedata: {len(mesh_extra)} nouveau(x) XML mesh(es) ajouté(s) comme .tmc, "
                f"{len(ctt_extra)} nouveau(x) XML normalmap(s) ajouté(s) comme .ctt, en plus du rapport."
            )
            ctt_tasks.extend(ctt_extra)
            standalone_tasks.extend(mesh_extra)
        else:
            self._add_note("Scan _gamedata: aucun fichier supplémentaire à ajouter hors rapport.")

        self._run_parallel(
            "Recodage CTT/NormalMap",
            ctt_tasks,
            lambda item: self._encode_ctt_record(item[0], item[1], out_dir, work_root),
        )
        self._run_parallel(
            "Recodage fichiers autonomes",
            standalone_tasks,
            lambda item: self._encode_standalone_record(item[0], item[1], out_dir, work_root),
        )

        a7m_tasks = [(Path(source_rel_str), items) for source_rel_str, items in sorted(a7m_groups.items())]
        self._run_parallel(
            "Repack .a7m",
            a7m_tasks,
            lambda item: self._encode_a7m_group(item[0], item[1], out_dir, work_root),
        )

    def _list_matching_ctt_outputs(self, work_dir: Path, stem: str) -> list[Path]:
        matches: list[Path] = []
        lower_stem = stem.lower()
        for path in sorted(work_dir.iterdir()):
            if not path.is_file():
                continue
            name_lower = path.name.lower()
            if not name_lower.startswith(lower_stem):
                continue
            if name_lower.endswith(".xml"):
                continue
            matches.append(path)
        return matches

    def _cleanup_ctt_work_outputs(self, work_dir: Path, keep_xml_name: str) -> None:
        """Nettoie toute sortie résiduelle avant une tentative CTT.

        Même si chaque tâche a son propre dossier, cette étape rend le choix du .fdbr
        strict: après FileDBReader, le seul .fdbr admissible doit être celui produit
        pour le XML courant.
        """
        for path in work_dir.iterdir():
            if not path.is_file() or path.name == keep_xml_name:
                continue
            if path.suffix.lower() in {".fdbr", ".data", ".ctt", ".bin", ".tmp"}:
                path.unlink(missing_ok=True)

    def _pick_ctt_fdbr_candidate(self, work_dir: Path, stem: str, known_before: set[str], started_ns: int) -> Path | None:
        """Retourne uniquement le FDBR produit pour ce XML après le début de l'appel.

        Le header .ctt est ensuite calculé depuis CE fichier, pas depuis un nom
        générique ni depuis une sortie d'une autre tâche. Chaque tâche utilisant un
        dossier isolé et nettoyé, le test "nouveau fichier" suffit et évite les
        faux négatifs liés à la granularité des timestamps Windows.
        """
        fdbr_matches = [
            path for path in work_dir.iterdir()
            if (
                path.is_file()
                and path.suffix.lower() == ".fdbr"
                and path.name not in known_before
                and path.stat().st_size > 0
            )
        ]
        if not fdbr_matches:
            return None

        preferred = [p for p in fdbr_matches if p.stem.lower() == stem.lower()]
        if len(preferred) == 1:
            return preferred[0]
        if len(fdbr_matches) == 1:
            return fdbr_matches[0]

        visible = ", ".join(sorted(p.name for p in fdbr_matches))
        raise InterpreterError(f"Sortie FDBR ambiguë pour {stem}.xml: {visible}")

    def _build_ctt_bytes_from_fdbr(self, fdbr_path: Path) -> tuple[bytes, int, int, str, str, str]:
        """Construit _CTT + taille FDBR + zlib(FDBR).

        La taille stockée dans le header est la taille du .fdbr décompressé
        réellement généré par FileDBReader depuis le XML utilisé. Le payload
        qui suit le header reste le .fdbr compressé en zlib.
        """
        fdbr_size = fdbr_path.stat().st_size
        fdbr_bytes = fdbr_path.read_bytes()
        if len(fdbr_bytes) != fdbr_size:
            raise InterpreterError(
                f"Taille FDBR incohérente pour {fdbr_path.name}: stat={fdbr_size}, lu={len(fdbr_bytes)}"
            )
        level = max(0, min(int(self.cfg.ctt_zlib_level), 9))
        payload = zlib.compress(fdbr_bytes, level=level)
        zlib_size = len(payload)
        # Header demandé: octets 0..3 = _CTT, octets 4..7 = taille_FDBR
        # Le payload reste zlib(FDBR), mais la taille écrite dans le header
        # est maintenant la taille décompressée du FDBR.
        header = b"_CTT" + struct.pack("<I", fdbr_size)
        if self.cfg.ctt_hash_audit or self.cfg.detailed_log:
            fdbr_hash = hashlib.sha256(fdbr_bytes).hexdigest()
            zlib_hash = hashlib.sha256(payload).hexdigest()
        else:
            fdbr_hash = ""
            zlib_hash = ""
        return header + payload, fdbr_size, zlib_size, header.hex(), fdbr_hash, zlib_hash

    def _audit_ctt_payload(self, fdbr_size: int, fdbr_hash: str, zlib_size: int, zlib_hash: str) -> None:
        with self._ctt_audit_lock:
            self._ctt_fdbr_sizes.add(fdbr_size)
            self._ctt_zlib_sizes.add(zlib_size)
            if fdbr_hash:
                self._ctt_fdbr_hashes.add(fdbr_hash)
            if zlib_hash:
                self._ctt_zlib_hashes.add(zlib_hash)

    def _finalize_ctt_audit(self) -> None:
        with self._ctt_audit_lock:
            self.report.ctt_unique_fdbr_sizes = sorted(self._ctt_fdbr_sizes)
            self.report.ctt_unique_fdbr_hashes = len(self._ctt_fdbr_hashes)
            self.report.ctt_same_fdbr_size_notice = bool(
                self.report.ctt_files_encoded > 1 and len(self._ctt_fdbr_sizes) == 1
            )
            self.report.ctt_unique_zlib_sizes = sorted(self._ctt_zlib_sizes)
            self.report.ctt_unique_zlib_hashes = len(self._ctt_zlib_hashes)
            self.report.ctt_same_zlib_size_notice = bool(
                self.report.ctt_files_encoded > 1 and len(self._ctt_zlib_sizes) == 1
            )
        if self.report.ctt_same_zlib_size_notice:
            if self.cfg.ctt_hash_audit or self.cfg.detailed_log:
                self._add_note(
                    "Info CTT: plusieurs headers peuvent être identiques si les payloads zlib ont la même taille. "
                    "Le contrôle à utiliser pour vérifier que chaque CTT est propre est le hash FDBR/zlib/CTT, pas seulement la taille."
                )
            else:
                self._add_note(
                    "Info CTT: plusieurs headers peuvent être identiques si les payloads zlib ont la même taille. "
                    "L'audit hash complet est désactivé par défaut pour gagner du temps."
                )

    def _list_matching_outputs(self, work_dir: Path, stem: str) -> list[Path]:
        matches: list[Path] = []
        lower_stem = stem.lower()
        for path in sorted(work_dir.iterdir()):
            if not path.is_file():
                continue
            name_lower = path.name.lower()
            if not name_lower.startswith(lower_stem):
                continue
            if name_lower.endswith('.xml'):
                continue
            matches.append(path)
        return matches

    def _pick_encoded_output_candidate(
        self,
        work_dir: Path,
        stem: str,
        expected_name: str,
        known_before: set[str],
    ) -> Path | None:
        preferred_names = [
            expected_name,
            f"{stem}.data",
            f"{stem}.DATA",
            f"{stem}.xml.data",
            f"{stem}.xml.DATA",
        ]
        for name in preferred_names:
            candidate = work_dir / name
            if candidate.exists() and candidate.is_file() and candidate.stat().st_size > 0:
                return candidate

        new_matches = [
            path for path in self._list_matching_outputs(work_dir, stem)
            if path.name not in known_before and path.stat().st_size > 0
        ]
        if new_matches:
            data_like = [path for path in new_matches if path.suffix.lower() == '.data']
            if data_like:
                return max(data_like, key=lambda p: p.stat().st_size)
            return max(new_matches, key=lambda p: p.stat().st_size)

        existing_matches = [
            path for path in self._list_matching_outputs(work_dir, stem)
            if path.stat().st_size > 0
        ]
        if existing_matches:
            data_like = [path for path in existing_matches if path.suffix.lower() == '.data']
            if data_like:
                return max(data_like, key=lambda p: p.stat().st_size)
            return max(existing_matches, key=lambda p: p.stat().st_size)
        return None

    def _encode_ctt_record(self, source_rel: Path, xml_rel: Path, out_dir: Path, work_root: Path) -> None:
        interp = self._find_filedb_fileformat("ctt.xml")
        if interp is None:
            self._inc_report("skipped_encode_records")
            self._record(source_rel, xml_rel, "skipped", "ctt.xml introuvable pour le recodage")
            return

        xml_src = self.cfg.source_island_dir / xml_rel
        work_dir = self._make_task_dir(work_root, "ctt", source_rel)
        work_xml = work_dir / f"{source_rel.stem}.xml"
        primary_fdbr = work_xml.with_suffix(".fdbr")
        shutil.copy2(xml_src, work_xml)
        filedb = self._find_executable("FileDBReader")
        self._log_detail(f"Recodage CTT/NormalMap: {source_rel} <= {xml_rel}")

        compress_attempts = [
            [str(filedb), "compress", "-f", work_xml.name, "-o", "fdbr", "-i", str(interp), "-y"],
            [str(filedb), "compress", "-f", work_xml.name, "-o", "fdbr", "-c", "0", "-i", str(interp), "-y"],
            [str(filedb), "compress", "-f", work_xml.name, "-o", "fdbr", "-c", "2", "-i", str(interp), "-y"],
            [str(filedb), "compress", "-f", work_xml.name, "-i", str(interp), "-y"],
            [str(filedb), "compress", "-f", work_xml.name, "-c", "0", "-i", str(interp), "-y"],
        ]
        last_error: Exception | None = None
        produced_fdbr: Path | None = None
        try:
            for cmd in compress_attempts:
                primary_fdbr.unlink(missing_ok=True)
                self._cleanup_ctt_work_outputs(work_dir, work_xml.name)
                known_before = {path.name for path in work_dir.iterdir() if path.is_file()}
                try:
                    started_ns = time.time_ns()
                    if self._ctt_serial:
                        with self._ctt_filedb_lock:
                            self._run_cmd(cmd, cwd=work_dir)
                    else:
                        self._run_cmd(cmd, cwd=work_dir)
                except Exception as exc:
                    last_error = exc
                    continue

                produced_fdbr = self._pick_ctt_fdbr_candidate(work_dir, work_xml.stem, known_before, started_ns)
                if produced_fdbr is not None:
                    break

            if produced_fdbr is None:
                visible = ", ".join(path.name for path in self._list_matching_ctt_outputs(work_dir, work_xml.stem)) or "aucun"
                if last_error is not None:
                    raise InterpreterError(f"{last_error}\nAucun .fdbr exploitable détecté après compression CTT. Fichiers visibles: {visible}")
                raise InterpreterError(f"FileDBReader n'a produit aucun .fdbr exploitable. Fichiers visibles: {visible}")

            canonical_fdbr = work_dir / f"{work_xml.stem}.fdbr"
            if produced_fdbr.resolve() != canonical_fdbr.resolve():
                canonical_fdbr.unlink(missing_ok=True)
                shutil.move(str(produced_fdbr), str(canonical_fdbr))
                produced_fdbr = canonical_fdbr

            ctt_bytes, fdbr_size, zlib_size, header_hex, fdbr_hash, zlib_hash = self._build_ctt_bytes_from_fdbr(produced_fdbr)
            out_path = out_dir / source_rel
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_bytes(ctt_bytes)
            written_header = ctt_bytes[:8]
            expected_header = b"_CTT" + struct.pack("<I", fdbr_size)
            if written_header != expected_header:
                raise InterpreterError(
                    f"Header CTT écrit incorrect pour {source_rel}: attendu={expected_header.hex()} lu={written_header.hex()}"
                )
            ctt_hash = hashlib.sha256(ctt_bytes).hexdigest() if (self.cfg.ctt_hash_audit or self.cfg.detailed_log) else ""
            self._audit_ctt_payload(fdbr_size, fdbr_hash, zlib_size, zlib_hash)
            self._inc_report("files_encoded")
            self._inc_report("ctt_files_encoded")
            note = (
                f"source xml={xml_rel.as_posix()} -> interpréteur={interp.name} -> {produced_fdbr.name} -> "
                f"header propre {header_hex} taille_FDBR_header={fdbr_size} taille_ZLIB={zlib_size} taille_FDBR={fdbr_size} zlib_level={max(0, min(int(self.cfg.ctt_zlib_level), 9))}"
            )
            if fdbr_hash:
                note += f" fdbr_sha256={fdbr_hash[:16]} zlib_sha256={zlib_hash[:16]} ctt_sha256={ctt_hash[:16]}"
            note += " -> zlib"
            self._record(source_rel, out_path, "encoded", note)
        except Exception as exc:
            self._inc_report("skipped_encode_records")
            self._record(source_rel, xml_rel, "failed", str(exc))
        finally:
            shutil.rmtree(work_dir, ignore_errors=True)

    def _encode_standalone_record(self, source_rel: Path, xml_rel: Path, out_dir: Path, work_root: Path) -> None:
        ext = source_rel.suffix.lower()
        interp = self._find_interpreter_for_extension(ext)
        if interp is None:
            self._inc_report("skipped_encode_records")
            self._record(source_rel, xml_rel, "skipped", f"aucun interprète trouvé pour {ext}")
            return

        xml_src = self.cfg.source_island_dir / xml_rel
        work_dir = self._make_task_dir(work_root, "standalone", source_rel)
        temp_xml = work_dir / f"{source_rel.stem}.xml"
        shutil.copy2(xml_src, temp_xml)
        filedb = self._find_executable("FileDBReader")
        self._log_detail(f"Recodage: {source_rel}")
        try:
            self._run_cmd([
                str(filedb),
                "compress",
                "-f",
                temp_xml.name,
                "-o",
                ext.lstrip("."),
                "-c",
                "2",
                "-i",
                str(interp),
                "-y",
            ], cwd=work_dir)
            produced = temp_xml.with_suffix(ext)
            if not produced.exists():
                raise InterpreterError(f"Fichier attendu non produit: {produced.name}")
            out_path = out_dir / source_rel
            out_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(produced), out_path)
            self._inc_report("files_encoded")
            if ext == ".tmc":
                self._inc_report("tmc_files_encoded")
            self._record(source_rel, out_path, "encoded")
        except Exception as exc:
            self._inc_report("skipped_encode_records")
            self._record(source_rel, xml_rel, "failed", str(exc))
        finally:
            shutil.rmtree(work_dir, ignore_errors=True)

    def _encode_a7m_group(self, source_rel: Path, items: list[tuple[Path, str]], out_dir: Path, work_root: Path) -> None:
        filedb = self._find_executable("FileDBReader")
        rda = self._find_executable("RdaConsole")
        rd3d_fmt = self._find_helper("Island_RD3D.xml")
        gamedata_fmt = self._find_helper("Island_Gamedata_v3.xml")

        bundle_rel = source_rel.with_suffix("")
        bundle_work = work_root / "__a7m_build" / bundle_rel
        bundle_work.mkdir(parents=True, exist_ok=True)
        pack_stage = work_root / "__a7m_pack_stage" / bundle_rel
        if pack_stage.exists():
            shutil.rmtree(pack_stage, ignore_errors=True)
        pack_stage.mkdir(parents=True, exist_ok=True)
        encoded_parts = 0
        packed_components: list[str] = []

        for xml_rel, note in items:
            xml_src = self.cfg.source_island_dir / xml_rel
            lower_note = note.lower()
            lower_name = Path(xml_rel).name.lower()
            if "rd3d.data" in lower_note or lower_name.startswith("rd3d"):
                data_name = "rd3d.data"
                interp = rd3d_fmt
            elif "gamedata.data" in lower_note or lower_name.startswith("gamedata"):
                data_name = "gamedata.data"
                interp = gamedata_fmt
            else:
                self._inc_report("skipped_encode_records")
                self._record(source_rel, xml_rel, "skipped", "composant .a7m non reconnu")
                continue

            temp_xml = bundle_work / f"{Path(data_name).stem}.xml"
            desired_data = bundle_work / data_name
            staged_data = pack_stage / data_name
            shutil.copy2(xml_src, temp_xml)
            desired_data.unlink(missing_ok=True)
            staged_data.unlink(missing_ok=True)
            try:
                known_before = {path.name for path in bundle_work.iterdir() if path.is_file()}
                self._run_cmd([
                    str(filedb),
                    "compress",
                    "-f",
                    temp_xml.name,
                    "-o",
                    "data",
                    "-c",
                    "2",
                    "-i",
                    str(interp),
                    "-y",
                ], cwd=bundle_work)
                produced = self._pick_encoded_output_candidate(
                    bundle_work,
                    temp_xml.stem,
                    data_name,
                    known_before,
                )
                if produced is None:
                    visible = ", ".join(path.name for path in self._list_matching_outputs(bundle_work, temp_xml.stem)) or "aucun"
                    raise InterpreterError(
                        f"FileDBReader n'a produit aucun .data exploitable pour {data_name}. Fichiers visibles: {visible}"
                    )
                if produced.resolve() != desired_data.resolve():
                    produced.replace(desired_data)
                if not desired_data.exists() or desired_data.stat().st_size == 0:
                    raise InterpreterError(f"{data_name} n'a pas été généré correctement avant repack")
                shutil.copy2(desired_data, staged_data)
                if not staged_data.exists() or staged_data.stat().st_size == 0:
                    raise InterpreterError(f"{data_name} n'a pas pu être préparé pour le repack explicite")
                encoded_parts += 1
                packed_components.append(data_name)
            except Exception as exc:
                self._inc_report("skipped_encode_records")
                self._record(source_rel, xml_rel, "failed", f"échec du composant {data_name}: {exc}")

        if encoded_parts == 0:
            self._inc_report("skipped_encode_records")
            self._record(source_rel, None, "skipped", "aucun composant .a7m n'a pu être recodé")
            return

        explicit_files = [name for name in ("rd3d.data", "gamedata.data") if (pack_stage / name).exists()]
        if not explicit_files:
            self._inc_report("skipped_encode_records")
            self._record(source_rel, None, "failed", "repack .a7m refusé: aucun rd3d.data/gamedata.data préparé")
            return

        packed_name = source_rel.name
        temp_packed = bundle_work.parent / packed_name
        try:
            self._log_detail(f"Repack A7M sans dossier interne via fichiers explicites: {source_rel}")
            self._run_rda_console(
                [
                    str(rda),
                    "pack",
                    "-v",
                    "2",
                    "-f",
                    *explicit_files,
                    "-o",
                    str(temp_packed),
                ],
                cwd=pack_stage,
                success_paths=[temp_packed],
            )
            if not temp_packed.exists():
                raise InterpreterError(f"Fichier .a7m attendu non produit: {packed_name}")
            out_path = out_dir / source_rel
            out_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(temp_packed), out_path)
            self._inc_report("a7m_files_packed")
            packed_list = ", ".join(explicit_files) or "aucun"
            self._record(
                source_rel,
                out_path,
                "encoded",
                f"{encoded_parts} composant(s) repacké(s) directement dans le .a7m via RdaConsole pack -f fichiers explicites: {packed_list}",
            )
        except Exception as exc:
            self._inc_report("skipped_encode_records")
            self._record(source_rel, None, "failed", f"repack .a7m: {exc}")
        finally:
            shutil.rmtree(pack_stage, ignore_errors=True)

    def _encode_a7m_heuristic(self, out_dir: Path, work_root: Path) -> None:
        seen_bundles: set[str] = set()
        tasks: list[tuple[Path, list[tuple[Path, str]]]] = []
        for folder in sorted(self.cfg.source_island_dir.rglob("*")):
            if not folder.is_dir():
                continue
            rd3d_xml = folder / "rd3d.xml"
            gamedata_xml = folder / "gamedata.xml"
            items: list[tuple[Path, str]] = []
            if rd3d_xml.exists():
                items.append((rd3d_xml.relative_to(self.cfg.source_island_dir), "depuis rd3d.data"))
            if gamedata_xml.exists():
                items.append((gamedata_xml.relative_to(self.cfg.source_island_dir), "depuis gamedata.data"))
            if not items:
                continue
            rel_folder = folder.relative_to(self.cfg.source_island_dir)
            bundle_rel = rel_folder.parent / f"{rel_folder.name}.a7m"
            if bundle_rel.as_posix() in seen_bundles:
                continue
            seen_bundles.add(bundle_rel.as_posix())
            tasks.append((bundle_rel, items))

        self._run_parallel(
            "Repack .a7m heuristique",
            tasks,
            lambda item: self._encode_a7m_group(item[0], item[1], out_dir, work_root),
        )

    def _cleanup_intermediates(self, out_dir: Path) -> int:
        removed = 0
        patterns = ["*.a7m", "*.a7minfo", "*.tmc", "*.fdbr", "*.payload.zlib", "*.header8.bin", "*.data"]
        for pattern in patterns:
            for path in out_dir.rglob(pattern):
                try:
                    path.unlink(missing_ok=True)
                    removed += 1
                except OSError:
                    pass
        return removed


# Référence implicite pour PyInstaller qui analyse parfois les imports paresseux.
_ = struct.calcsize
