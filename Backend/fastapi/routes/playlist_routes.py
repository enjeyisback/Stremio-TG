import re
from fastapi import APIRouter, Response, HTTPException
from Backend.helper.database import db
from Backend.config import Telegram, BASE_URL

router = APIRouter(tags=["Playlist"])

def get_part_number(filename: str) -> int:
    """Extract part number from filename (e.g., 'movie.part01.mkv' -> 1)."""
    # Look for 'part' followed by digits, likely near the end or separated by dots/spaces
    match = re.search(r"part\s*0*(\d+)", filename, re.IGNORECASE)
    if match:
        return int(match.group(1))
    return 999999 # Fallback for non-matching files to go to end

@router.get("/playlist/{media_id}/{quality}.m3u8")
async def get_playlist(media_id: str, quality: str):
    """Generate M3U8 playlist for multi-part files of a specific quality."""
    
    # 1. Resolve Media ID
    tmdb_id = None
    db_index = None
    season_num = None
    episode_num = None
    
    try:
        parts = media_id.split(":")
        base_id = parts[0]
        
        if base_id.startswith("tt") and Telegram.CINEMETA_SUPPORT:
            media = await db.get_media_by_imdb(base_id)
            if not media:
                raise HTTPException(status_code=404, detail="Media not found")
            tmdb_id = media.get("tmdb_id")
            db_index = media.get("db_index")
            if len(parts) > 1: season_num = int(parts[1])
            if len(parts) > 2: episode_num = int(parts[2])
        else:
            tmdb_id_str, db_index_str = base_id.split("-")
            tmdb_id, db_index = int(tmdb_id_str), int(db_index_str)
            if len(parts) > 1: season_num = int(parts[1])
            if len(parts) > 2: episode_num = int(parts[2])
            
    except Exception:
         raise HTTPException(status_code=400, detail="Invalid ID format")

    # 2. Fetch Media Details
    media_details = await db.get_media_details(
        tmdb_id=tmdb_id,
        db_index=db_index,
        season_number=season_num,
        episode_number=episode_num
    )
    
    if not media_details or "telegram" not in media_details:
        raise HTTPException(status_code=404, detail="No streams found")

    # 3. Filter Files by Quality and Sort
    # We allow loose matching for quality (e.g. "4K" matches "4K HDR")
    target_files = []
    
    for file_obj in media_details.get("telegram", []):
        file_quality = file_obj.get("quality", "HD")
        # specific check: if user asked for "4k", we match "4k". 
        # But we need to be careful if multiple qualities exist.
        # Ideally we match the exact quality string passed from stremio_routes
        if file_quality == quality:
            target_files.append(file_obj)
            
    if not target_files:
        raise HTTPException(status_code=404, detail="No files found for this quality")

    # Sort by part number
    target_files.sort(key=lambda x: get_part_number(x.get("name", "")))

    # 4. Generate M3U8
    m3u8_content = ["#EXTM3U", "#EXT-X-VERSION:3", "#EXT-X-TARGETDURATION:3600", "#EXT-X-PLAYLIST-TYPE:VOD"]
    
    for file_obj in target_files:
        if file_obj.get("id"):
            name = file_obj.get("name", "Video")
            # Clean name for EXTINF
            clean_name = re.sub(r'[^\w\s\-\.]', '', name)
            m3u8_content.append(f"#EXTINF:-1, {clean_name}")
            # Direct download link
            m3u8_content.append(f"{BASE_URL}/dl/{file_obj.get('id')}/video.mkv")
            
    m3u8_content.append("#EXT-X-ENDLIST")
    
    return Response(content="\n".join(m3u8_content), media_type="application/vnd.apple.mpegurl")
