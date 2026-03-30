import urllib.request
import json

CATEGORIES_API = "https://rms.api.bbc.co.uk/v2/categories"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; RadioCacheBot/1.0; +https://github.com/bumface11/radiocache)",
    "Accept": "application/json",
}

def fetch_categories():
    req = urllib.request.Request(CATEGORIES_API, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=20) as resp:
        data = json.loads(resp.read())
    return data

if __name__ == "__main__":
    data = fetch_categories()
    # The structure may vary; look for 'categories' or similar key
    categories = data.get("categories") or data.get("data") or []
    for cat in categories:
        slug = cat.get("id") or cat.get("slug")
        title = cat.get("title") or cat.get("name")
        print(f"{slug}: {title}")