from __future__ import annotations

import json
import shutil
import urllib.request
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent
THIRD_PARTY = ROOT / "third_party"
TOOLS_DIR = THIRD_PARTY / "tools"
HELPERS_DIR = THIRD_PARTY / "helpers"

FILEDB_REPO = "anno-mods/FileDBReader"
RDA_REPO = "anno-mods/RdaConsole"
RDA_TAG = "v1.2"

REQUIRED_HELPER_FILES = (
    "Island_Gamedata_v3.xml",
    "Island_RD3D.xml",
    "a7minfo.xml",
    "tmc.xml",
)


def log(message: str) -> None:
    print(message, flush=True)


def fetch_json(url: str) -> dict:
    req = urllib.request.Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "User-Agent": "anno-island-interpreter-build-script",
        },
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read().decode("utf-8"))


def download(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    log(f"Téléchargement: {url}")
    req = urllib.request.Request(url, headers={"User-Agent": "anno-island-interpreter-build-script"})
    with urllib.request.urlopen(req) as resp, dest.open("wb") as fh:
        shutil.copyfileobj(resp, fh)


def pick_asset(release: dict, exact_name: str) -> dict:
    assets = release.get("assets") or []
    for asset in assets:
        if (asset.get("name") or "").lower() == exact_name.lower():
            return asset
    available = ", ".join((asset.get("name") or "<sans nom>") for asset in assets)
    raise RuntimeError(f"Asset GitHub introuvable: {exact_name}. Disponibles: {available}")


def download_release_asset(repo: str, *, latest: bool = True, tag: str | None = None, exact_name: str, out_dir: Path) -> Path:
    if latest:
        api_url = f"https://api.github.com/repos/{repo}/releases/latest"
    else:
        if not tag:
            raise ValueError("tag requis quand latest=False")
        api_url = f"https://api.github.com/repos/{repo}/releases/tags/{tag}"
    release = fetch_json(api_url)
    asset = pick_asset(release, exact_name)
    target = out_dir / asset["name"]
    download(asset["browser_download_url"], target)
    return target


def extract_archive(archive_path: Path, dest_dir: Path) -> None:
    dest_dir.mkdir(parents=True, exist_ok=True)
    if archive_path.suffix.lower() != ".zip":
        raise RuntimeError(f"Format d'archive non pris en charge: {archive_path}")
    with zipfile.ZipFile(archive_path) as zf:
        zf.extractall(dest_dir)


def flatten_if_single_child(folder: Path) -> None:
    children = [p for p in folder.iterdir()]
    if len(children) != 1 or not children[0].is_dir():
        return
    child = children[0]
    temp_dir = folder.parent / f"{folder.name}_tmp_flatten"
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    temp_dir.mkdir(parents=True, exist_ok=True)
    for item in child.iterdir():
        shutil.move(str(item), temp_dir / item.name)
    shutil.rmtree(folder)
    temp_dir.rename(folder)


def find_file_case_insensitive(root: Path, filename: str) -> Path | None:
    target = filename.lower()
    for path in root.rglob("*"):
        if path.is_file() and path.name.lower() == target:
            return path
    return None


def ensure_tool_layout(tool_dir: Path, exe_name: str) -> None:
    flatten_if_single_child(tool_dir)
    exe_path = find_file_case_insensitive(tool_dir, exe_name)
    if exe_path is None:
        raise RuntimeError(f"{exe_name} introuvable après extraction dans {tool_dir}")
    canonical_path = tool_dir / exe_name
    if exe_path.resolve() != canonical_path.resolve():
        shutil.copy2(exe_path, canonical_path)


def main() -> None:
    THIRD_PARTY.mkdir(parents=True, exist_ok=True)
    TOOLS_DIR.mkdir(parents=True, exist_ok=True)
    missing = [name for name in REQUIRED_HELPER_FILES if not (HELPERS_DIR / name).exists()]
    if missing:
        raise RuntimeError(f"Helpers manquants: {', '.join(missing)}")

    downloads_dir = ROOT / "downloads"
    downloads_dir.mkdir(exist_ok=True)

    filedb_archive = download_release_asset(
        FILEDB_REPO,
        latest=True,
        exact_name="FileDBReader.zip",
        out_dir=downloads_dir,
    )
    rda_archive = download_release_asset(
        RDA_REPO,
        latest=False,
        tag=RDA_TAG,
        exact_name="RdaConsole.zip",
        out_dir=downloads_dir,
    )

    filedb_out = TOOLS_DIR / "FileDBReader"
    rda_out = TOOLS_DIR / "RdaConsole"
    if filedb_out.exists():
        shutil.rmtree(filedb_out)
    if rda_out.exists():
        shutil.rmtree(rda_out)

    extract_archive(filedb_archive, filedb_out)
    extract_archive(rda_archive, rda_out)
    ensure_tool_layout(filedb_out, "FileDBReader.exe")
    ensure_tool_layout(rda_out, "RdaConsole.exe")
    log("Préparation terminée.")


if __name__ == "__main__":
    main()
