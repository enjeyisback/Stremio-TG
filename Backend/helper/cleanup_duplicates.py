"""
Duplicate File Cleanup Script

This script scans the database for duplicate files (same filename)
and removes the older duplicates, keeping only the newest entry.
"""
import asyncio
from Backend import db
from Backend.helper.encrypt import decode_string
from Backend.logger import LOGGER

async def cleanup_movie_duplicates():
    """Remove duplicate files from all movies in the database."""
    total_deleted = 0
    
    for db_index in range(1, db.current_db_index + 1):
        db_key = f"storage_{db_index}"
        collection = db.dbs[db_key]["movie"]
        
        async for movie in collection.find({}):
            telegram_files = movie.get("telegram", [])
            if len(telegram_files) <= 1:
                continue
            
            # Group by filename
            filename_map = {}
            for file_obj in telegram_files:
                filename = file_obj.get("name", "")
                if filename:
                    if filename not in filename_map:
                        filename_map[filename] = []
                    filename_map[filename].append(file_obj)
            
            # Find duplicates and mark for deletion
            files_to_keep = []
            files_to_delete = []
            
            for filename, files in filename_map.items():
                if len(files) > 1:
                    # Keep the first one (newest due to insert(0)), delete rest
                    files_to_keep.append(files[0])
                    files_to_delete.extend(files[1:])
                    LOGGER.info(f"Found {len(files)-1} duplicate(s) of: {filename}")
                else:
                    files_to_keep.append(files[0])
            
            if files_to_delete:
                # Delete from Telegram
                for file_obj in files_to_delete:
                    try:
                        old_id = file_obj.get("id")
                        if old_id:
                            decoded = await decode_string(old_id)
                            chat_id = int(f"-100{decoded['chat_id']}")
                            msg_id = int(decoded['msg_id'])
                            
                            from Backend.pyrofork.bot import StreamBot
                            try:
                                await StreamBot.delete_messages(chat_id, msg_id)
                                LOGGER.info(f"Deleted message {msg_id} from {chat_id}")
                            except Exception as e:
                                LOGGER.warning(f"Could not delete message {msg_id}: {e}")
                    except Exception as e:
                        LOGGER.error(f"Error processing file for deletion: {e}")
                
                # Update database
                movie["telegram"] = files_to_keep
                await collection.replace_one({"_id": movie["_id"]}, movie)
                total_deleted += len(files_to_delete)
                LOGGER.info(f"Cleaned up movie: {movie.get('title')} - removed {len(files_to_delete)} duplicates")
    
    return total_deleted

async def cleanup_tv_duplicates():
    """Remove duplicate files from all TV episodes in the database."""
    total_deleted = 0
    
    for db_index in range(1, db.current_db_index + 1):
        db_key = f"storage_{db_index}"
        collection = db.dbs[db_key]["tv"]
        
        async for tv_show in collection.find({}):
            modified = False
            
            for season in tv_show.get("seasons", []):
                for episode in season.get("episodes", []):
                    telegram_files = episode.get("telegram", [])
                    if len(telegram_files) <= 1:
                        continue
                    
                    # Group by filename
                    filename_map = {}
                    for file_obj in telegram_files:
                        filename = file_obj.get("name", "")
                        if filename:
                            if filename not in filename_map:
                                filename_map[filename] = []
                            filename_map[filename].append(file_obj)
                    
                    # Find duplicates
                    files_to_keep = []
                    files_to_delete = []
                    
                    for filename, files in filename_map.items():
                        if len(files) > 1:
                            files_to_keep.append(files[0])
                            files_to_delete.extend(files[1:])
                        else:
                            files_to_keep.append(files[0])
                    
                    if files_to_delete:
                        # Delete from Telegram
                        for file_obj in files_to_delete:
                            try:
                                old_id = file_obj.get("id")
                                if old_id:
                                    decoded = await decode_string(old_id)
                                    chat_id = int(f"-100{decoded['chat_id']}")
                                    msg_id = int(decoded['msg_id'])
                                    
                                    from Backend.pyrofork.bot import StreamBot
                                    try:
                                        await StreamBot.delete_messages(chat_id, msg_id)
                                        LOGGER.info(f"Deleted message {msg_id} from {chat_id}")
                                    except Exception as e:
                                        LOGGER.warning(f"Could not delete message {msg_id}: {e}")
                            except Exception as e:
                                LOGGER.error(f"Error processing file for deletion: {e}")
                        
                        episode["telegram"] = files_to_keep
                        modified = True
                        total_deleted += len(files_to_delete)
            
            if modified:
                await collection.replace_one({"_id": tv_show["_id"]}, tv_show)
                LOGGER.info(f"Cleaned up TV show: {tv_show.get('title')}")
    
    return total_deleted

async def run_cleanup():
    """Run the full duplicate cleanup."""
    LOGGER.info("Starting duplicate cleanup...")
    
    movie_deleted = await cleanup_movie_duplicates()
    tv_deleted = await cleanup_tv_duplicates()
    
    total = movie_deleted + tv_deleted
    LOGGER.info(f"Cleanup complete! Removed {total} duplicate files ({movie_deleted} movies, {tv_deleted} TV)")
    
    return {
        "movies_cleaned": movie_deleted,
        "tv_cleaned": tv_deleted,
        "total": total
    }

if __name__ == "__main__":
    asyncio.run(run_cleanup())
