# managers/media_manager.py

import argparse
import sys
import os
import subprocess
import json
from pathlib import Path
from datetime import datetime
import logging
from app_utils import setup_logging, app_failed, load_media_types, load_metadata_mappings
from metadata_parser import select_oldest_datetime
from db_connection import connect_to_database
import re
from utils.media_utils import (
    get_existing_media_record,
    insert_new_media_record,
    update_missing_media_fields
)
from utils.file_mover import move_file
from utils.file_mover import process_sources

sys.path.append(str(Path(__file__).resolve().parent.parent))

EXT_MAP = load_media_types()
IMAGE_EXTS = set(EXT_MAP.get("Photos", []))
VIDEO_EXTS = set(EXT_MAP.get("Videos", []))
AUDIO_EXTS = set(EXT_MAP.get("Audio", []))
DOCUMENT_EXTS = set(EXT_MAP.get("Documents", []))

MAPPINGS = load_metadata_mappings()

USER_HOME = Path.home()
LOCAL_MEDIA_ROOT = USER_HOME / "Videos"
REMOTE_MEDIA_ROOT = "/multimedia"

local_photos_dir = USER_HOME / "Pictures"
local_videos_dir = LOCAL_MEDIA_ROOT
local_dashcam_dir = LOCAL_MEDIA_ROOT / "DC"
local_tiktok_dir = LOCAL_MEDIA_ROOT / "TikTok"

remote_photos_dir = Path(REMOTE_MEDIA_ROOT) / "photos"
remote_videos_dir = Path(REMOTE_MEDIA_ROOT) / "videos"
remote_dashcam_dir = remote_videos_dir / "DC"
remote_tiktok_dir = remote_videos_dir / "TikTok"

def get_exiftool_data(file_path, logger):
    command = ["exiftool", "-j", file_path]
    logger.debug(f"Running command: {' '.join(command)}")
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        metadata = json.loads(result.stdout)[0]
        logger.debug(f"Raw ExifTool metadata: {metadata}")
        return metadata
    except Exception as e:
        logger.warning(f"ExifTool failed for {file_path}: {e}")
        return {}

"""
def update_media_record(db_conn, media_id, metadata, media_type, logger, dry_run=False):
    if not metadata:
        logger.debug(f"No metadata to update for {media_id}")
        return

    # Determine valid fields (assume DB columns match ExifTool keys in snake_case)
    columns = []
    values = []

    for key, value in metadata.items():
        # Skip complex/nested fields or nulls
        if isinstance(value, (list, dict)) or value in ("", None):
            continue

        # Convert ExifTool keys like "CreateDate" to "create_date"
        column_name = re.sub(r'(?<!^)(?=[A-Z])', '_', key).lower()

        if column_name == "source_file":
            continue  # skip filename

        columns.append(column_name)
        values.append(value)

    if not columns:
        logger.debug(f"No updatable metadata fields found for ID {media_id}")
        return

    set_clause = ", ".join([f"{col} = %s" for col in columns])
    query = f"UPDATE {media_type} SET {set_clause} WHERE id = %s"
    values.append(media_id)

    logger.debug(f"SQL UPDATE: {query}")
    logger.debug(f"Values: {values}")

    if dry_run:
        logger.info(f"DRY_RUN: Would update {media_type} ID {media_id} with metadata")
    else:
        try:
            execute_query(db_conn, query, tuple(values))
            logger.info(f"Updated {media_type} ID {media_id} with {len(columns)} metadata fields")
        except Exception as e:
            logger.error(f"Failed to update ID {media_id}: {e}")
"""

def list_valid_files(root_dir, extensions, logger):
    for root, _, files in os.walk(root_dir):
        logger.debug(f"Scanning: {root}")
        for file in files:
            ext = os.path.splitext(file)[1].lower()
            if ext in extensions:
                yield os.path.join(root, file)
            else:
                logger.debug(f"Skipping unsupported: {file}")

def process_media_files(logger, source_dirs, valid_exts, db_conn,
                        dry_run=False, debug=False, verbose=False, media_type="Videos"):

    if dry_run:
        debug = verbose = True

    media_files = [fp for d in source_dirs for fp in list_valid_files(d, valid_exts, logger)]
    total_files = len(media_files)
    logger.info(f"[{media_type}] Total files: {total_files}")

    updated = skipped = inserted = unmatched = 0
    batch_limit = 10
    insert_batch = []
    update_batch = []

    for idx, file_path in enumerate(media_files, 1):
        if is_processed(db_conn, file_path, media_type):
            logger.debug(f"{file_path} already processed. Skipping.")
            skipped += 1
            continue

        prefix = f"[{media_type}] [{idx}/{total_files}]"
        logger.info(f"{prefix} Processing: {file_path}" if verbose else f"{prefix}")

        file_name = os.path.basename(file_path)
        metadata = get_exiftool_data(file_path, logger)

        if "CreateDate" not in metadata:
            fallback_dt = select_oldest_datetime({}, logger, filename=file_name)
            if fallback_dt:
                metadata["CreateDate"] = fallback_dt.strftime("%Y:%m:%d %H:%M:%S")
                logger.debug(f"Using fallback datetime: {metadata['CreateDate']}")
                logger.debug("Running fallback datetime parser, not using execute_query")
                logger.debug(f"select_oldest_datetime received: {file_name}")

        if not db_conn:
            logger.info(f"DRY_RUN: Would process metadata for {file_name}")
            skipped += 1
            continue

        logger.debug(f"Checking DB for existing record: {file_name}")
        existing = get_existing_media_record(db_conn, file_name, media_type, logger)
        if existing:
            logger.debug(f"Found existing record for {file_name}, queuing update")
            update_batch.append((file_path, existing["id"], metadata, existing))
        else:
            logger.debug(f"No existing record found for {file_name}, queuing insert")
            insert_batch.append((file_path, metadata))

        # Batch flush
        if len(insert_batch) >= batch_limit:
            for file_path, record in insert_batch:
                insert_new_media_record(db_conn, record, media_type, logger, dry_run, file_path=file_path)
                inserted += 1
            insert_batch.clear()

        if len(update_batch) >= batch_limit:
            for file_path, media_id, record, existing_row in update_batch:
                update_missing_media_fields(db_conn, media_id, record, existing_row, media_type, logger, dry_run, file_path=file_path)
                updated += 1
            update_batch.clear()

    # Final flush
    for file_path, record in insert_batch:
        insert_new_media_record(db_conn, record, media_type, logger, dry_run, file_path=file_path)
        inserted += 1

    for file_path, media_id, record, existing_row in update_batch:
        update_missing_media_fields(db_conn, media_id, record, existing_row, media_type, logger, dry_run, file_path=file_path)
        updated += 1

    logger.info(f"[{media_type}] Summary: scanned={total_files}, inserted={inserted}, updated={updated}, skipped={skipped}, unmatched={unmatched}")

def handle_media(logger, media_type, source_dirs, ext_set,
                 dry_run=False, debug=False, verbose=False,
                 config_file=None, config_section=None):

    logger.info(f"Handling {media_type} files...")
    db_conn = None if dry_run else connect_to_database(config_file, config_section)
    process_media_files(logger, source_dirs, ext_set, db_conn,
                        dry_run=dry_run, debug=debug, verbose=verbose,
                        media_type=media_type)

def main():
    parser = argparse.ArgumentParser(description="Unified Media Manager")
    parser.add_argument("--videos", action="store_true", help="Process video files")
    parser.add_argument("--photos", action="store_true", help="Process photo files")
    parser.add_argument("--all", action="store_true", help="Process all media types")
    parser.add_argument("--dry-run", action="store_true", help="Simulate actions without writing to DB or moving files")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--move-only", action="store_true", help="Only move files, skip database ingest entirely")
    parser.add_argument("--sources", nargs="+", help="Specify one or more source directories to move files from")
    parser.add_argument("--target", choices=["local", "remote", "both"], default="local", help="Where to move files")
    args = parser.parse_args()

    # Initialize logging
    logger = setup_logging("media_manager")
    debug = args.debug or args.dry_run
    verbose = args.verbose or args.dry_run
    if debug:
        logger.setLevel(logging.DEBUG)

    config_file = os.path.expanduser("~/.my.cnf")
    config_section = "media"
    db_conn = None if args.dry_run else connect_to_database(config_file, config_section)

    # MOVE-ONLY mode
    if args.move_only:
        print("\n🔄 Move-only mode activated.")
        from utils.file_mover import pick_sources_interactively, process_sources
        sources = args.sources or pick_sources_interactively()
        if not sources:
            print("No sources selected. Exiting.")
            sys.exit(0)

        process_sources(
            sources=sources,
            mode=args.target,
            dry_run=args.dry_run,
            verbose=verbose,
            debug=debug,
            db_conn=db_conn
        )
        sys.exit(0)

    # Full media ingest mode
    try:
        if args.all or args.videos:
            handle_media(logger, "Videos",
                         ["/multimedia/Videos", "/multimedia/Home_Videos", "/multimedia/TikTok"],
                         VIDEO_EXTS,
                         dry_run=args.dry_run,
                         debug=debug,
                         verbose=verbose,
                         config_file=config_file,
                         config_section=config_section)

        if args.all or args.photos:
            handle_media(logger, "Photos",
                         ["/multimedia/Photos"],
                         IMAGE_EXTS,
                         dry_run=args.dry_run,
                         debug=debug,
                         verbose=verbose,
                         config_file=config_file,
                         config_section=config_section)

    except Exception as e:
        app_failed("media_manager", f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()


