import json, urllib.request
from radio_cache.bbc_feed_parser import fetch_programme_detail

pid = "p0hwjs59"

print("=== fetch_programme_detail() ===")
prog = fetch_programme_detail(pid)
print(repr(prog))
print("categories:", repr(prog.categories if prog else None))

for url in (
    f"https://ibl.api.bbc.co.uk/ibl/v1/episodes/{pid}",
    f"https://www.bbc.co.uk/programmes/{pid}.json",
):
    print("\n=== RAW URL ===", url)
    try:
        req = urllib.request.Request(url, headers={"User-Agent":"RadioCacheBot/1.0","Accept":"application/json"})
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read())
            print(json.dumps(data, indent=2)[:4000])
    except Exception as e:
        print("ERROR:", e)