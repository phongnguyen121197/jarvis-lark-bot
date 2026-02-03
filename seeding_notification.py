# seeding_notification.py
"""
Seeding Notification Module - Gửi thông báo seeding với thumbnail TikTok
Version 1.0.0
"""

import os
import re
import json
from typing import Optional, Callable
import httpx

# ============ CONFIG ============
LARK_API_BASE = "https://open.larksuite.com/open-apis"

# Chat ID của nhóm "Gấp 2H" - Lấy từ environment variable
GAP_2H_CHAT_ID = os.getenv("GAP_2H_CHAT_ID", "")


# ============ TIKTOK THUMBNAIL CRAWLER ============

async def get_tiktok_thumbnail(tiktok_url: str) -> Optional[str]:
    """
    Crawl thumbnail từ TikTok URL
    
    Args:
        tiktok_url: URL video TikTok
        
    Returns: 
        URL của thumbnail hoặc None nếu thất bại
    """
    if not tiktok_url:
        return None
        
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
        }
        
        async with httpx.AsyncClient(follow_redirects=True, timeout=15) as client:
            response = await client.get(tiktok_url, headers=headers)
            html = response.text
            
            # Các patterns để tìm thumbnail trong TikTok HTML
            patterns = [
                r'<meta property="og:image" content="([^"]+)"',
                r'"thumbnail":\s*\{\s*"url_list":\s*\[\s*"([^"]+)"',
                r'"cover":\s*"([^"]+)"',
                r'"originCover":\s*"([^"]+)"',
                r'"dynamicCover":\s*"([^"]+)"',
                r'"thumbnail_url":\s*"([^"]+)"',
            ]
            
            for pattern in patterns:
                match = re.search(pattern, html)
                if match:
                    thumbnail_url = match.group(1)
                    # Unescape URL characters
                    thumbnail_url = thumbnail_url.replace("\\u002F", "/")
                    thumbnail_url = thumbnail_url.replace("\\u0026", "&")
                    thumbnail_url = thumbnail_url.replace("\\/", "/")
                    print(f"✅ Found TikTok thumbnail: {thumbnail_url[:80]}...")
                    return thumbnail_url
            
            print(f"⚠️ No thumbnail found for: {tiktok_url}")
            return None
            
    except Exception as e:
        print(f"❌ Error crawling TikTok thumbnail: {e}")
        return None


# ============ LARK IMAGE UPLOAD ============

async def upload_image_to_lark(image_url: str, get_token_func: Callable) -> Optional[str]:
    """
    Download ảnh từ URL và upload lên Lark
    
    Args:
        image_url: URL của ảnh cần upload
        get_token_func: Async function để lấy tenant_access_token
        
    Returns: 
        image_key để dùng trong Message Card, hoặc None nếu thất bại
    """
    if not image_url:
        return None
        
    try:
        token = await get_token_func()
        
        # 1. Download ảnh từ URL
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        
        async with httpx.AsyncClient(timeout=20) as client:
            img_response = await client.get(image_url, headers=headers, follow_redirects=True)
            if img_response.status_code != 200:
                print(f"❌ Failed to download image: HTTP {img_response.status_code}")
                return None
            image_data = img_response.content
            
            if len(image_data) < 1000:
                print(f"❌ Image data too small, likely not a valid image")
                return None
            
            # Detect image type từ content-type header
            content_type = img_response.headers.get("content-type", "image/jpeg")
            if "png" in content_type:
                filename = "thumbnail.png"
                mime_type = "image/png"
            elif "gif" in content_type:
                filename = "thumbnail.gif"
                mime_type = "image/gif"
            elif "webp" in content_type:
                filename = "thumbnail.webp"
                mime_type = "image/webp"
            else:
                filename = "thumbnail.jpg"
                mime_type = "image/jpeg"
        
        # 2. Upload lên Lark
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                f"{LARK_API_BASE}/im/v1/images",
                headers={
                    "Authorization": f"Bearer {token}"
                },
                files={
                    "image": (filename, image_data, mime_type)
                },
                data={
                    "image_type": "message"
                }
            )
            
            result = response.json()
            if result.get("code") == 0:
                image_key = result.get("data", {}).get("image_key")
                print(f"✅ Uploaded image to Lark: {image_key}")
                return image_key
            else:
                print(f"❌ Lark upload failed: {result}")
                return None
                
    except Exception as e:
        print(f"❌ Error uploading image to Lark: {e}")
        return None


# ============ SEND MESSAGE CARD ============

async def send_seeding_card(
    chat_id: str,
    koc_name: str,
    channel_id: str,
    tiktok_url: str,
    product: str,
    get_token_func: Callable,
    image_key: Optional[str] = None,
    record_url: Optional[str] = None,
    title: str = "🔥 SOS VIDEO ĐÃ AIR SEEDING GẤP",
    header_color: str = "red"
) -> bool:
    """
    Gửi Message Card thông báo seeding với thumbnail
    
    Args:
        chat_id: ID của chat/nhóm Lark
        koc_name: Tên KOC
        channel_id: ID kênh TikTok
        tiktok_url: Link video TikTok
        product: Tên sản phẩm
        get_token_func: Async function để lấy tenant_access_token
        image_key: Image key từ Lark (đã upload)
        record_url: Link đến bản ghi trong Lark Base
        title: Tiêu đề card
        header_color: Màu header (red, orange, yellow, green, blue, purple)
        
    Returns: 
        True nếu gửi thành công
    """
    if not chat_id:
        print("❌ Missing chat_id")
        return False
        
    try:
        token = await get_token_func()
        
        # Tạo card elements
        elements = []
        
        # Thêm thumbnail nếu có image_key
        if image_key:
            elements.append({
                "tag": "img",
                "img_key": image_key,
                "alt": {
                    "tag": "plain_text",
                    "content": "Video thumbnail"
                },
                "mode": "fit_horizontal",
                "preview": True
            })
        
        # Thông tin chi tiết
        info_parts = []
        if koc_name:
            info_parts.append(f"• **Tên KOC:** {koc_name}")
        if channel_id:
            info_parts.append(f"• **ID kênh:** {channel_id}")
        if product:
            info_parts.append(f"• **Sản phẩm:** {product}")
        
        if info_parts:
            elements.append({
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": "\n".join(info_parts)
                }
            })
        
        # Note
        elements.append({
            "tag": "note",
            "elements": [
                {
                    "tag": "plain_text",
                    "content": "Check gấp triển khai công việc nha mọi người"
                }
            ]
        })
        
        # Divider
        elements.append({"tag": "hr"})
        
        # Buttons
        actions = []
        
        if tiktok_url:
            actions.append({
                "tag": "button",
                "text": {
                    "tag": "plain_text",
                    "content": "🎬 XEM VIDEO"
                },
                "type": "primary",
                "url": tiktok_url
            })
        
        if record_url:
            actions.append({
                "tag": "button",
                "text": {
                    "tag": "plain_text",
                    "content": "📋 LINK BẢN GHI"
                },
                "type": "default",
                "url": record_url
            })
        
        if actions:
            elements.append({
                "tag": "action",
                "actions": actions
            })
        
        # Card JSON
        card = {
            "config": {
                "wide_screen_mode": True
            },
            "header": {
                "title": {
                    "tag": "plain_text",
                    "content": title
                },
                "template": header_color
            },
            "elements": elements
        }
        
        # Gửi message
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                f"{LARK_API_BASE}/im/v1/messages?receive_id_type=chat_id",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json"
                },
                json={
                    "receive_id": chat_id,
                    "msg_type": "interactive",
                    "content": json.dumps(card)
                }
            )
            
            result = response.json()
            if result.get("code") == 0:
                print(f"✅ Sent seeding card to chat {chat_id}")
                return True
            else:
                print(f"❌ Failed to send seeding card: {result}")
                return False
                
    except Exception as e:
        print(f"❌ Error sending seeding card: {e}")
        return False


# ============ MAIN FUNCTION ============

async def send_seeding_notification(
    chat_id: str,
    koc_name: str,
    channel_id: str,
    tiktok_url: str,
    product: str,
    get_token_func: Callable,
    record_url: Optional[str] = None,
    with_thumbnail: bool = True,
    title: str = "🔥 SOS VIDEO ĐÃ AIR SEEDING GẤP"
) -> dict:
    """
    Function chính: Crawl thumbnail + Upload + Gửi card
    
    Args:
        chat_id: ID của chat/nhóm Lark
        koc_name: Tên KOC
        channel_id: ID kênh TikTok  
        tiktok_url: Link video TikTok
        product: Tên sản phẩm
        get_token_func: Async function để lấy tenant_access_token
        record_url: Link đến bản ghi trong Lark Base (optional)
        with_thumbnail: Có crawl và hiển thị thumbnail không
        title: Tiêu đề card (optional)
        
    Returns: 
        Dict với kết quả chi tiết
    """
    result = {
        "success": False,
        "thumbnail_crawled": False,
        "thumbnail_uploaded": False,
        "card_sent": False,
        "error": None,
        "chat_id": chat_id
    }
    
    if not chat_id:
        result["error"] = "Missing chat_id. Please set GAP_2H_CHAT_ID environment variable."
        return result
    
    image_key = None
    
    # Step 1: Crawl thumbnail từ TikTok
    if with_thumbnail and tiktok_url:
        print(f"🔍 Crawling thumbnail from: {tiktok_url}")
        thumbnail_url = await get_tiktok_thumbnail(tiktok_url)
        if thumbnail_url:
            result["thumbnail_crawled"] = True
            
            # Step 2: Upload thumbnail lên Lark
            print(f"📤 Uploading thumbnail to Lark...")
            image_key = await upload_image_to_lark(thumbnail_url, get_token_func)
            if image_key:
                result["thumbnail_uploaded"] = True
    
    # Step 3: Gửi Message Card
    try:
        print(f"📨 Sending seeding card to chat: {chat_id}")
        card_sent = await send_seeding_card(
            chat_id=chat_id,
            koc_name=koc_name,
            channel_id=channel_id,
            tiktok_url=tiktok_url,
            product=product,
            get_token_func=get_token_func,
            image_key=image_key,
            record_url=record_url,
            title=title
        )
        
        result["card_sent"] = card_sent
        result["success"] = card_sent
        
    except Exception as e:
        result["error"] = str(e)
        print(f"❌ Error in send_seeding_notification: {e}")
    
    return result
