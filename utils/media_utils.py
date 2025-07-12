# utils/media_utils.py

import os
import re
from datetime import datetime
from db_connection import safe_query as execute_query
from app_utils import load_metadata_mappings
import logger

MAPPINGS = load_metadata_mappings()

def get_existing_media_record(db_conn, file_name, media_type, logger):
    try:
        logger.debug("Using context-managed cursor to fetch existing record")
        query = f"SELECT * FROM {media_type} WHERE file_name = %s"
        with db_conn.cursor(buffered=True) as cursor:
            cursor.execute(query, (file_name,))
            row = cursor.fetchone()
            if not row:
                return None
            column_names = [desc[0] for desc in cursor.description]  # ← MOVE THIS HERE
            return dict(zip(column_names, row))                      # ← AND THIS
    except Exception as e:
        logger.error(f"Failed to fetch record for {file_name}: {e}")
        return None

def sanitize_metadata(raw_metadata, mapping, logger, file_path=None):
    clean = {}
    logger.debug(f"Raw metadata received: {raw_metadata}")
    logger.debug(f"Mapped fields: {mapping}")

    for exif_key, db_field in mapping.items():
        raw_value = raw_metadata.get(exif_key)
        if raw_value in ("", None, "0000:00:00 00:00:00", "N/A"):
            continue

        logger.debug(f"Sanitizing: {exif_key} → {db_field} with raw value: {raw_value}")
        try:
            value = raw_value

            if db_field == "date_taken":
                if isinstance(value, datetime):
                    pass  # already valid
                else:
                    value_str = str(value).strip().replace(":", "-", 2)
                    value = datetime.strptime(value_str[:19], "%Y-%m-%d %H:%M:%S")

            elif db_field == "flash":
                value = 1 if str(value).lower().strip() in {"yes", "true", "1", "on"} else 0

            elif db_field == "duration":
                match = re.search(r"(\d+):(\d+):(\d+)", str(value))
                if match:
                    h, m, s = map(int, match.groups())
                    value = h * 3600 + m * 60 + s
                else:
                    match = re.search(r"(\d+):(\d+)", str(value))
                    if match:
                        m, s = map(int, match.groups())
                        value = m * 60 + s
                    elif re.match(r"^\d+(\.\d+)?$", str(value)):
                        value = float(value)

            elif db_field == "size":
                match = re.match(r"([\d\.]+)\s*(MB|GB|KB|B)", str(value).strip(), re.IGNORECASE)
                if match:
                    num, unit = match.groups()
                    factor = {
                        "B": 1,
                        "KB": 1024,
                        "MB": 1024**2,
                        "GB": 1024**3
                    }.get(unit.upper(), 1)
                    value = int(float(num) * factor)

        except Exception as e:
            logger.debug(f"Failed to parse {db_field} from '{raw_value}': {e}")
            continue

        clean[db_field] = value

    # Ensure required fields
    clean.setdefault("file_name", raw_metadata.get("FileName") or os.path.basename(file_path or ""))
    clean.setdefault("file_location", raw_metadata.get("Directory") or os.path.dirname(file_path or ""))

    logger.debug(f"Sanitized metadata after parsing: {clean}")
    return clean

def insert_new_media_record(db_conn, metadata, media_type, logger, dry_run=False, file_path=None):
    mapping = MAPPINGS.get(media_type, {})
    sanitized = sanitize_metadata(metadata, mapping, logger, file_path=file_path)
    logger.debug(f"Sanitized fields for {media_type}: {sanitized}")

    if not sanitized or not sanitized.get("file_name"):
        logger.warning(f"Skipping insert — missing required metadata for {media_type}. Source file: {file_path}")
        return

    logger.info(f"Preparing insert for {media_type}: {sanitized.get('file_name')}")

    columns = list(sanitized.keys())
    values = list(sanitized.values())
    placeholders = ', '.join(['%s'] * len(values))
    sql = f"INSERT INTO {media_type} ({', '.join(columns)}) VALUES ({placeholders})"

    logger.debug(f"SQL INSERT: {sql}")
    logger.debug(f"Values: {values}")

    if dry_run:
        logger.info(f"DRY_RUN: Would insert new {media_type} record for {sanitized.get('file_name')}")
        return

    try:
        with db_conn.cursor() as cursor:
            cursor.execute(sql, tuple(values))
            db_conn.commit()
            logger.info(f"Inserted new {media_type} record: {sanitized.get('file_name')}")
    except Exception as e:
        logger.error(f"Insert failed for {media_type} file '{sanitized.get('file_name')}': {e}")

def update_missing_media_fields(db_conn, media_id, metadata, existing_row, media_type, logger, dry_run=False, file_path=None):
    mapping = MAPPINGS.get(media_type, {})
    sanitized = sanitize_metadata(metadata, mapping, logger, file_path=file_path)
    updates = {}

    for field, value in sanitized.items():
        if field not in existing_row or existing_row[field] in (None, '', 0):
            updates[field] = value

    if not updates:
        logger.debug(f"No new fields to update for ID {media_id}")
        return

    sql_parts = [f"`{field}` = %s" for field in updates]
    values = list(updates.values()) + [media_id]
    sql = f"UPDATE {media_type} SET {', '.join(sql_parts)} WHERE id = %s"

    logger.debug(f"SQL UPDATE: {sql}")
    logger.debug(f"Values: {values}")
    if dry_run:
        logger.info(f"DRY_RUN: Would update {media_type} ID {media_id} with {len(updates)} fields")
    else:
        try:
            with db_conn.cursor() as cursor:
                cursor.execute(sql, tuple(values))
                db_conn.commit()
                logger.info(f"Updated {media_type} ID {media_id} with {len(updates)} fields")
        except Exception as e:
            logger.error(f"Update failed for ID {media_id}: {e}")

