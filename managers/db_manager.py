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

