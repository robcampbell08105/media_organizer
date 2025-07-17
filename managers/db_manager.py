from db_connection import execute_query

def get_media_id_by_filename(db_conn, filename, media_type, logger):
    table = "Photos" if media_type == "Photos" else "Videos"
    query = f"SELECT id FROM {table} WHERE file_name = %s"
    logger.debug(f"Querying {table} for filename: {filename}")
    result = execute_query(db_conn, query, (filename,))
    return result[0][0] if result else None

def update_media_date_taken(db_conn, media_id, new_datetime, media_type, logger):
    table = "Photos" if media_type == "Photos" else "Videos"
    query = f"UPDATE {table} SET date_taken = %s WHERE id = %s"
    logger.debug(f"Executing SQL: {query} with params=({new_datetime}, {media_id})")
    try:
        execute_query(db_conn, query, (new_datetime, media_id))
        logger.info(f"Updated {media_type} ID {media_id} with date_taken={new_datetime}")
    except Exception as e:
        logger.error(f"Update failed for {media_type} ID {media_id}: {e}")

def store_metadata(db_conn, metadata):
    """
    Inserts or updates metadata for a media file in the database.
    Assumes a table like MediaProcessing with columns: file_path, processed, processed_at, etc.
    """
    try:
        query = """
            INSERT INTO MediaProcessing (file_path, processed, processed_at)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE processed = VALUES(processed), processed_at = VALUES(processed_at)
        """
        params = (
            metadata["file_path"],
            1,
            metadata.get("date_taken") or datetime.now().isoformat()
        )
        cursor = db_conn.cursor()
        cursor.execute(query, params)
        db_conn.commit()
        cursor.close()
    except Exception as e:
        print(f"[DB] Failed to store metadata for {metadata['file_path']}: {e}")

