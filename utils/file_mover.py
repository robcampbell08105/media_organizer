import os
import subprocess
from datetime import datetime
from pathlib import Path

def extract_create_date(file_path):
    file_name = os.path.basename(file_path)
    if file_name.startswith("PXL_"):
        try:
            ts = file_name.split("_")[1]
            return datetime.strptime("20" + ts, "%Y%m%d").date()
        except Exception:
            pass
    try:
        result = subprocess.run(["exiftool", "-CreateDate", file_path],
                                capture_output=True, text=True, check=True)
        dt_line = result.stdout.strip().split(": ", 1)[-1].split()[0]
        return datetime.strptime(dt_line.replace(":", "-", 2), "%Y-%m-%d").date()
    except Exception:
        return None

def move_file(file_path, target_base, dry_run=False):
    date = extract_create_date(file_path)
    if not date:
        print(f"Skipping {file_path}: can't parse date")
        return

    target_dir = Path(target_base) / str(date)
    target_dir.mkdir(parents=True, exist_ok=True)
    filename = os.path.basename(file_path)
    target_file = target_dir / filename

    if target_file.exists():
        print(f"Skipping {filename}: already exists in {target_dir}")
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

def is_tiktok(file_path):
    base = os.path.basename(file_path)
    return len(base) == 32 and base.isalnum()

