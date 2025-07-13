import os
import re
import subprocess
from datetime import datetime
from pathlib import Path
import getpass
# Import db-backed processor skip logic if needed
from db_connection import is_processed  # Optional — depends on integration

def extract_create_date(file_path):
    file_name = os.path.basename(file_path)

    # PXL fallback from filename
    pxl_match = re.match(r"PXL_(\d{6})_", file_name)
    if pxl_match:
        try:
            ts = "20" + pxl_match.group(1)
            return datetime.strptime(ts, "%Y%m%d").date()
        except Exception:
            pass

    # ExifTool fallback
    try:
        result = subprocess.run(["exiftool", "-CreateDate", file_path],
                                capture_output=True, text=True, check=True)
        line = result.stdout.strip().split(": ", 1)[-1].split()[0]
        cleaned = line.replace(":", "-", 2)
        return datetime.strptime(cleaned, "%Y-%m-%d").date()
    except Exception:
        return None

def is_tiktok(file_path):
    base = os.path.basename(file_path)
    return len(base) == 32 and base.isalnum()

def move_file(file_path, target_base, dry_run=False, verbose=False):
    date = extract_create_date(file_path)
    if not date:
        if verbose: print(f"[SKIP] No date found: {file_path}")
        return

    target_dir = Path(target_base) / str(date)
    target_dir.mkdir(parents=True, exist_ok=True)
    filename = os.path.basename(file_path)
    target_file = target_dir / filename

    if target_file.exists():
        if verbose: print(f"[SKIP] Already exists in {target_dir}: {filename}")
        return

    rsync_cmd = ["rsync", "-rltD"]
    if not dry_run:
        rsync_cmd.append("--remove-source-files")
    rsync_cmd += [file_path, str(target_dir)]

    print(f"{'[DRY_RUN]' if dry_run else '[MOVE]'} {file_path} → {target_dir}")
    subprocess.run(rsync_cmd)

    if not dry_run:
        subprocess.run(["exiftool",
                        f"-CreateDate={date}", f"-ModifyDate={date}", f"-DateTimeOriginal={date}",
                        "-overwrite_original", str(target_file)],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        subprocess.run(["touch", "-d", str(date), str(target_file)])

import getpass

def pick_sources_interactively():
    user = getpass.getuser()
    base_dirs = [
        "/mnt",
        str(Path.home() / "mnt"),
        f"/media/{user}",
        "/run/media",  # you could also try f"/run/media/{user}" if needed
    ]

    candidates = []

    for base in base_dirs:
        if os.path.exists(base):
            for entry in os.listdir(base):
                full_path = os.path.join(base, entry)
                if os.path.isdir(full_path):
                    candidates.append(full_path)

    if not candidates:
        print("No candidate media sources found.")
        return []

    print("\n📁 Found the following media source directories:")
    for i, path in enumerate(candidates, 1):
        print(f"  [{i}] {path}")

    choice = input("\nSelect one or more sources (e.g. 1 3 5 or press Enter to skip): ").strip()
    if not choice:
        return []

    selected = []
    try:
        indexes = [int(i) for i in choice.split()]
        for i in indexes:
            if 1 <= i <= len(candidates):
                selected.append(candidates[i - 1])
    except Exception:
        print("Invalid input. No sources selected.")

    return selected

def resolve_target(file_path, mode="local"):
    home = Path.home()
    remote_base = Path("/multimedia")

    if is_tiktok(file_path):
        return {
            "local": home / "Videos" / "TikTok",
            "remote": remote_base / "videos" / "TikTok"
        }.get(mode)

    if "Novatek" in file_path or "CARDV" in file_path or "DASHCAM" in file_path:
        return {
            "local": home / "Videos" / "DC",
            "remote": remote_base / "videos" / "DC"
        }.get(mode)

    if re.search(r"\.(jpg|jpeg|png|gif|heic|tif|tiff|nef|dng|raw)$", file_path, re.IGNORECASE):
        return {
            "local": home / "Pictures",
            "remote": remote_base / "photos"
        }.get(mode)

    if re.search(r"\.(mp4|mov|avi|mkv|webm|3gp|mpeg|mpg)$", file_path, re.IGNORECASE):
        return {
            "local": home / "Videos",
            "remote": remote_base / "videos"
        }.get(mode)

    return None

def process_sources(sources, mode="local", dry_run=False, verbose=False, debug=False, db_conn=None):
    if not sources:
        print("No media sources provided.")
        return

    for source in sources:
        print(f"\n🔍 Scanning: {source}")
        for root, _, files in os.walk(source):
            for name in files:
                file_path = os.path.join(root, name)

                if ".thumbnail" in file_path or ".thumbnails" in file_path:
                    if verbose: print(f"[SKIP] Thumbnail file: {file_path}")
                    continue

                if db_conn and is_processed(db_conn, file_path):
                    if verbose: print(f"[SKIP] Already processed: {file_path}")
                    continue

                target = resolve_target(file_path, mode=mode)
                if not target:
                    if debug: print(f"[SKIP] Unknown target for: {file_path}")
                    continue

                move_file(file_path, target, dry_run=dry_run, verbose=verbose)

