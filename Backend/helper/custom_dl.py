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
        LOGGER.debug(f"Starting to yielding file with client {index}. DC: {file_id.dc_id}")
        
        current_part = 1
        location = await self.get_location(file_id)
        
        try:
            media_session = await self.generate_media_session(client, file_id)
            if not media_session:
                LOGGER.error(f"Failed to generate media session for client {index}")
                return

            while True:
                try:
                    r = await media_session.send(
                        raw.functions.upload.GetFile(location=location, offset=offset, limit=chunk_size)
                    )
                    
                    if isinstance(r, raw.types.upload.File):
                        chunk = r.bytes
                        if not chunk:
                            break
                        
                        if part_count == 1:
                            yield chunk[first_part_cut:last_part_cut]
                        elif current_part == 1:
                            yield chunk[first_part_cut:]
                        elif current_part == part_count:
                            yield chunk[:last_part_cut]
                        else:
                            yield chunk

                        current_part += 1
                        offset += chunk_size

                        if current_part > part_count:
                            break
                    else:
                        LOGGER.error(f"Unexpected response type from GetFile: {type(r)}")
                        break

                except FileReferenceExpired:
                    LOGGER.info(f"File reference expired for msg_id {message_id}, refreshing...")
                    file_id = await self.get_file_properties(chat_id, message_id, refresh=True)
                    location = await self.get_location(file_id)
                    # Don't increment current_part, just retry the same part with new location
                    continue
                
                except FloodWait as e:
                    LOGGER.warning(f"FloodWait in yield_file: {e.value}s. Client: {index}")
                    await asyncio.sleep(e.value)
                    continue

                except RPCError as e:
                    LOGGER.error(f"RPC Error in yield_file for client {index}: {e}")
                    break
                
                except Exception as e:
                    LOGGER.error(f"Unexpected error in yield_file loop: {e}")
                    break

        except Exception as e:
            LOGGER.error(f"Critical error in yield_file for client {index}: {e}")
        finally:
            LOGGER.debug(f"Finished yielding file with {current_part - 1} parts.")
            work_loads[index] -= 1

    async def generate_media_session(self, client: Client, file_id: FileId) -> Optional[Session]:
        dc_id = file_id.dc_id
        
        # Check active sessions first (fast path)
        media_session = client.media_sessions.get(dc_id)
        if media_session and media_session.is_started:
            return media_session

        # Need to create/restart session (locked path to avoid thundering herd)
        lock = self.get_lock(dc_id)
        async with lock:
            # Check again inside lock
            media_session = client.media_sessions.get(dc_id)
            if media_session and media_session.is_started:
                return media_session
            
            LOGGER.info(f"Establishing new media session for DC {dc_id}...")
            try:
                if dc_id != await client.storage.dc_id():
                    media_session = Session(
                        client,
                        dc_id,
                        await Auth(client, dc_id, await client.storage.test_mode()).create(),
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
                            break
                        except AuthBytesInvalid:
                            LOGGER.debug(f"Invalid auth bytes for DC {dc_id}, attempt {i+1}")
                        except FloodWait as e:
                            LOGGER.warning(f"FloodWait during ExportAuth for DC {dc_id}: {e.value}s")
                            await asyncio.sleep(e.value)
                        except Exception as e:
                            LOGGER.error(f"Error during ImportAuth for DC {dc_id}: {e}")
                            await asyncio.sleep(1)
                    else:
                        await media_session.stop()
                        LOGGER.error(f"Failed to establish media session for DC {dc_id} after retries")
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
                LOGGER.info(f"Media session established for DC {dc_id}")
                return media_session

            except Exception as e:
                LOGGER.error(f"Failed to create media session for DC {dc_id}: {e}")
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
