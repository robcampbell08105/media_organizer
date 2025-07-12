import subprocess
import re
from datetime import datetime
import json

EXIFTOOL_FIELDS = [
    "DateTimeOriginal", "CreateDate", "ModifyDate",
    "FileCreateDate", "MediaCreateDate", "MediaModifyDate",
    "TrackCreateDate", "TrackModifyDate", "FileModifyDate"
]

def extract_datetimes(file_path, logger):
    logger.debug(f"Executing exiftool metadata extraction on: {file_path}")
    command = ["exiftool", "-j"] + [f"-{field}" for field in EXIFTOOL_FIELDS] + [file_path]
    logger.debug(f"COMMAND: {' '.join(command)}")

    date_map = {}
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        logger.debug(f"Raw ExifTool stdout: {result.stdout.strip()[:500]}")
        metadata_raw = json.loads(result.stdout)[0]
        normalized = {k.replace(" ", "").lower(): v for k, v in metadata_raw.items()}
        logger.debug(f"Normalized metadata keys: {list(normalized.keys())}")

        for field in EXIFTOOL_FIELDS:
            key = field.replace(" ", "").lower()
            raw = normalized.get(key)
            if raw:
                dt = sanitize_datetime(raw)
                if dt:
                    logger.debug(f"Accepted datetime for '{field}': {dt}")
                    date_map[field] = dt
                else:
                    logger.debug(f"Rejected datetime for '{field}': {raw}")
            else:
                logger.debug(f"Field '{field}' not found in metadata.")
    except Exception as e:
        logger.warning(f"ExifTool failed: {e}")
    return date_map

def sanitize_datetime(raw):
    try:
        if not raw or str(raw).startswith(("0000", "1970", "None")):
            return None
        match = re.search(r"\d{4}[:\-]\d{2}[:\-]\d{2} \d{2}:\d{2}:\d{2}", str(raw))
        if match:
            cleaned = match.group(0).replace(":", "-", 2)
            return datetime.strptime(cleaned, "%Y-%m-%d %H:%M:%S")
    except:
        return None
    return None

def select_oldest_datetime(date_map, logger, filename=None):
    if date_map:
        selected = min(date_map.values())
        logger.debug(f"Using metadata datetime: {selected}")
        return selected

    logger.debug(f"No metadata found. Trying filename fallback for {filename}")
    match = re.search(r'(\d{4})(\d{2})(\d{2})[_\-]?(\d{2})(\d{2})(\d{2})', filename)
    if match:
        try:
            ts = f"{match.group(1)}-{match.group(2)}-{match.group(3)} {match.group(4)}:{match.group(5)}:{match.group(6)}"
            fallback = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
            logger.debug(f"Parsed datetime from filename: {fallback}")
            return fallback
        except Exception as e:
            logger.debug(f"Filename fallback failed: {e}")
    return None

