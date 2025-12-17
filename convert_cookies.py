#!/usr/bin/env python3
"""
Helper script để convert cookies từ browser sang JSON format
"""
import json
import sys

def parse_netscape_cookies(cookie_text):
    """Parse Netscape cookie format (từ EditThisCookie)"""
    cookies = []
    for line in cookie_text.strip().split('\n'):
        if line.startswith('#') or not line.strip():
            continue
        parts = line.split('\t')
        if len(parts) >= 7:
            cookies.append({
                "name": parts[5],
                "value": parts[6],
                "domain": parts[0],
                "path": parts[2],
                "expires": int(parts[4]) if parts[4] != "0" else -1,
                "httpOnly": parts[1] == "TRUE",
                "secure": parts[3] == "TRUE"
            })
    return cookies

def parse_document_cookie(cookie_text):
    """Parse document.cookie format"""
    cookies = []
    for pair in cookie_text.split(';'):
        pair = pair.strip()
        if '=' in pair:
            name, value = pair.split('=', 1)
            cookies.append({
                "name": name.strip(),
                "value": value.strip(),
                "domain": ".tiktok.com",
                "path": "/"
            })
    return cookies

def main():
    print("🍪 TikTok Ads Cookie Converter")
    print("-" * 50)
    print()
    print("Paste cookies (Ctrl+D khi xong):")
    print()
    
    cookie_text = sys.stdin.read().strip()
    
    # Try different formats
    if '\t' in cookie_text:
        cookies = parse_netscape_cookies(cookie_text)
        print(f"✅ Parsed {len(cookies)} cookies (Netscape format)")
    else:
        cookies = parse_document_cookie(cookie_text)
        print(f"✅ Parsed {len(cookies)} cookies (document.cookie format)")
    
    if not cookies:
        print("❌ No cookies found!")
        return
    
    # Save to file
    output_file = "/tmp/tiktok_ads_cookies.json"
    with open(output_file, 'w') as f:
        json.dump(cookies, f, indent=2)
    
    print(f"💾 Saved to: {output_file}")
    print()
    print("Upload file này lên Railway:")
    print("1. Railway Dashboard → Data → Volumes")
    print("2. Upload tiktok_ads_cookies.json")

if __name__ == "__main__":
    main()
