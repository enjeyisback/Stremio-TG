import asyncio
from pyrogram import utils, raw
from pyrogram.errors import AuthBytesInvalid, FloodWait, FileReferenceExpired, RPCError
from pyrogram.file_id import FileId, FileType, ThumbnailSource
from pyrogram.session import Session, Auth
from typing import Dict, Union, Optional
from Backend.logger import LOGGER
from Backend.helper.exceptions import FIleNotFound
from Backend.helper.pyro import get_file_ids
from Backend.pyrofork.bot import work_loads
from pyrogram import Client


class ByteStreamer:
    def __init__(self, client: Client):
        self.clean_timer = 30 * 60
        self.client: Client = client
        self.__cached_file_ids: Dict[int, FileId] = {}
        self.__session_locks: Dict[int, asyncio.Lock] = {}
        asyncio.create_task(self.clean_cache())

    def get_lock(self, dc_id: int) -> asyncio.Lock:
        if dc_id not in self.__session_locks:
            self.__session_locks[dc_id] = asyncio.Lock()
        return self.__session_locks[dc_id]

    async def get_file_properties(self, chat_id: int, message_id: int, refresh: bool = False) -> FileId:
        if refresh or message_id not in self.__cached_file_ids:
            file_id = await get_file_ids(self.client, int(chat_id), int(message_id))
            if not file_id:
                LOGGER.info('Message with ID %s not found!', message_id)
                raise FIleNotFound
            self.__cached_file_ids[message_id] = file_id
        return self.__cached_file_ids[message_id]

    async def yield_file(
        self, 
        file_id: FileId, 
        index: int, 
        offset: int, 
        first_part_cut: int, 
        last_part_cut: int, 
        part_count: int, 
        chunk_size: int,
        chat_id: int,
        message_id: int
    ) -> Union[bytes, None]: # type: ignore
        client = self.client
        work_loads[index] += 1
        LOGGER.info(f"DEBUG: yield_file (Turbo+Fixed) started for msg_id {message_id} on client {index}. DC: {file_id.dc_id}")
        
        queue = asyncio.Queue(maxsize=12) 
        fetcher_done = asyncio.Event()
        
        async def fetcher():
            nonlocal offset
            curr_part = 1
            try:
                location = await self.get_location(file_id)
                media_session = await self.generate_media_session(client, file_id)
                if not media_session:
                    return

                semaphore = asyncio.Semaphore(4)
                
                async def fetch_chunk(p_idx, p_offset):
                    async with semaphore:
                        for retry in range(5): # More retries for Turbo mode
                            try:
                                r = await media_session.send(
                                    raw.functions.upload.GetFile(location=location, offset=p_offset, limit=chunk_size)
                                )
                                if isinstance(r, raw.types.upload.File):
                                    return p_idx, r.bytes
                                break
                            except FileReferenceExpired:
                                raise
                            except FloodWait as e:
                                LOGGER.warning(f"DEBUG: FloodWait in fetch_chunk ({p_idx}): {e.value}s")
                                await asyncio.sleep(e.value + 1)
                            except RPCError as e:
                                LOGGER.error(f"DEBUG: RPCError in fetch_chunk ({p_idx}): {e}")
                                if retry == 4: raise e
                                await asyncio.sleep(1)
                            except Exception as e:
                                if retry == 4: raise e
                                await asyncio.sleep(1)
                        return p_idx, None

                pending_tasks = set()
                while curr_part <= part_count:
                    while len(pending_tasks) < 4 and curr_part <= part_count:
                        task = asyncio.create_task(fetch_chunk(curr_part, offset))
                        pending_tasks.add(task)
                        curr_part += 1
                        offset += chunk_size
                    
                    if not pending_tasks:
                        break
                        
                    done_tasks, pending_tasks = await asyncio.wait(
                        pending_tasks, return_when=asyncio.FIRST_COMPLETED
                    )
                    
                    for t in done_tasks:
                        try:
                            result = await t
                            await queue.put(result)
                        except Exception as e:
                            LOGGER.error(f"DEBUG: Fetch error collected: {e}")
                            await queue.put((-1, str(e)))

            except Exception as e:
                LOGGER.error(f"DEBUG: Fetcher loop error: {e}")
            finally:
                # CRITICAL: Clean up all pending tasks to avoid "Task exception was never retrieved"
                for t in pending_tasks:
                    t.cancel()
                if pending_tasks:
                    await asyncio.gather(*pending_tasks, return_exceptions=True)
                
                await queue.put((None, None))
                fetcher_done.set()

        fetcher_task = asyncio.create_task(fetcher())
        buffer = {}
        expected_part = 1
        
        try:
            while expected_part <= part_count:
                # If the next expected part is already in buffer, yield it
                if expected_part in buffer:
                    chunk = buffer.pop(expected_part)
                else:
                    # Wait for next chunk(s) from queue
                    p_idx, chunk = await queue.get()
                    if p_idx is None: break # End of stream
                    if p_idx == -1: break # Error
                    
                    if p_idx == expected_part:
                        # Got what we wanted
                        pass
                    else:
                        # Got something else, buffer it and continue waiting
                        buffer[p_idx] = chunk
                        continue
                
                # Yield the correctly ordered chunk
                if part_count == 1:
                    yield chunk[first_part_cut:last_part_cut]
                elif expected_part == 1:
                    yield chunk[first_part_cut:]
                elif expected_part == part_count:
                    yield chunk[:last_part_cut]
                else:
                    yield chunk
                
                expected_part += 1

        except Exception as e:
            LOGGER.error(f"DEBUG: Consumer error in yield_file: {e}")
        finally:
            fetcher_task.cancel()
            work_loads[index] -= 1
            LOGGER.info(f"DEBUG: Finished yielding (Turbo+Fixed) file for client {index}. Parts: {expected_part-1}/{part_count}")

    async def generate_media_session(self, client: Client, file_id: FileId) -> Optional[Session]:
        dc_id = file_id.dc_id
        
        # Fast path check: using .is_set() because is_started is an asyncio.Event
        media_session = client.media_sessions.get(dc_id)
        if media_session and getattr(media_session, 'is_started', None) and media_session.is_started.is_set():
            return media_session

        lock = self.get_lock(dc_id)
        async with lock:
            # Check again inside lock
            media_session = client.media_sessions.get(dc_id)
            if media_session and getattr(media_session, 'is_started', None) and media_session.is_started.is_set():
                return media_session
            
            LOGGER.info(f"DEBUG: Establishing new media session for DC {dc_id}...")
            try:
                if dc_id != await client.storage.dc_id():
                    auth_key = await Auth(client, dc_id, await client.storage.test_mode()).create()
                    media_session = Session(
                        client,
                        dc_id,
                        auth_key,
                        await client.storage.test_mode(),
                        is_media=True,
                    )
                    await media_session.start()
                    
                    for i in range(5):
                        try:
                            exported_auth = await client.invoke(raw.functions.auth.ExportAuthorization(dc_id=dc_id))
                            await media_session.send(
                                raw.functions.auth.ImportAuthorization(id=exported_auth.id, bytes=exported_auth.bytes)
                            )
                            LOGGER.info(f"DEBUG: Auth imported for DC {dc_id} on attempt {i+1}")
                            break
                        except AuthBytesInvalid:
                            LOGGER.debug(f"DEBUG: Invalid auth bytes for DC {dc_id}, attempt {i+1}")
                        except FloodWait as e:
                            LOGGER.warning(f"DEBUG: FloodWait during ExportAuth for DC {dc_id}: {e.value}s")
                            await asyncio.sleep(e.value)
                        except Exception as e:
                            LOGGER.error(f"DEBUG: Error during ImportAuth for DC {dc_id}: {e}")
                            await asyncio.sleep(1)
                    else:
                        await media_session.stop()
                        LOGGER.error(f"DEBUG: Failed to establish media session for DC {dc_id} after retries")
                        return None
                else:
                    media_session = Session(
                        client,
                        dc_id,
                        await client.storage.auth_key(),
                        await client.storage.test_mode(),
                        is_media=True,
                    )
                    await media_session.start()
                
                client.media_sessions[dc_id] = media_session
                LOGGER.info(f"DEBUG: Media session established and started for DC {dc_id}")
                return media_session

            except Exception as e:
                LOGGER.error(f"DEBUG: Failed to create media session for DC {dc_id}: {e}")
                return None


    @staticmethod
    async def get_location(file_id: FileId) -> Union[raw.types.InputPhotoFileLocation, raw.types.InputDocumentFileLocation, raw.types.InputPeerPhotoFileLocation]:
        file_type = file_id.file_type
        if file_type == FileType.CHAT_PHOTO:
            if file_id.chat_id > 0:
                peer = raw.types.InputPeerUser(
                    user_id=file_id.chat_id, access_hash=file_id.chat_access_hash)
            else:
                if file_id.chat_access_hash == 0:
                    peer = raw.types.InputPeerChat(chat_id=-file_id.chat_id)
                else:
                    peer = raw.types.InputPeerChannel(channel_id=utils.get_channel_id(
                        file_id.chat_id), access_hash=file_id.chat_access_hash)
            location = raw.types.InputPeerPhotoFileLocation(peer=peer,
                                                            volume_id=file_id.volume_id,
                                                            local_id=file_id.local_id,
                                                            big=file_id.thumbnail_source == ThumbnailSource.CHAT_PHOTO_BIG)
        elif file_type == FileType.PHOTO:
            location = raw.types.InputPhotoFileLocation(id=file_id.media_id,
                                                        access_hash=file_id.access_hash,
                                                        file_reference=file_id.file_reference,
                                                        thumb_size=file_id.thumbnail_size)
        else:
            location = raw.types.InputDocumentFileLocation(id=file_id.media_id,
                                                           access_hash=file_id.access_hash,
                                                           file_reference=file_id.file_reference,
                                                           thumb_size=file_id.thumbnail_size)
        return location

    async def clean_cache(self) -> None:
        while True:
            await asyncio.sleep(self.clean_timer)
            self.__cached_file_ids.clear()
            self.__session_locks.clear()
            LOGGER.debug("Cleaned the cache")
