import os
import shutil
import logging
from pathlib import Path

def transfer_files(sources, target, dry_run=False, remove_sources=False):
    target = Path(target).expanduser()
    os.makedirs(target, exist_ok=True)

    for source in sources:
        source_path = Path(source).expanduser()
        if not source_path.exists():
            logging.warning(f"Source path {source_path} does not exist.")
            continue

        for root, _, files in os.walk(source_path):
            for name in files:
                src = Path(root) / name
                dst = target / name

                prefix = "DRY_RUN: " if dry_run else ""
                logging.info(f"{prefix}Transfer {src} -> {dst}")

                if not dry_run:
                    shutil.copy2(src, dst)
                    if remove_sources:
                        try:
                            os.remove(src)
                            logging.info(f"Removed source file: {src}")
                        except Exception as e:
                            logging.error(f"Failed to remove source file {src}: {e}")
                elif remove_sources:
                    logging.info(f"DRY_RUN: Would remove {src}")


