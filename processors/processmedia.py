from managers.db_manager import store_metadata
from managers.media_transfer import organize_file
from utils.logger import log_action
from metadata_parser import parse_metadata

def process(file_paths):
    for path in file_paths:
        metadata = parse_metadata(path)
        store_metadata(metadata)
        organize_file(path, metadata)
        log_action(f"Processed: {path}")

