"""Turn Wikipedia's live edit stream into a folder of JSON-lines files.

This is your "source system" for the Chapter 7 capstone. Wikimedia publishes
every edit, page creation and log action (deletes, moves, ...) on all its wikis
as a public Server-Sent-Events stream:

    https://stream.wikimedia.org/v2/stream/recentchange

The collector keeps the events of one wiki (default: English Wikipedia,
articles only), and every --seconds it writes what it collected as a new file
into --out — exactly the "files keep arriving in a folder" situation Auto
Loader is built for.

Examples
--------
    # 10 minutes of English Wikipedia, a new file every 20 s
    python tools/wiki_collector.py --out data/wiki/landing --minutes 10

    # simulate a source-system upgrade: after 6 files every event gets a new field
    python tools/wiki_collector.py --out data/wiki/landing --minutes 10 --drift-after-files 6

    # no internet / corporate proxy blocks the stream: synthetic events, same shape
    python tools/wiki_collector.py --out data/wiki/landing --minutes 5 --fake
"""
import argparse
import json
import os
import random
import string
import sys
import time
from datetime import datetime, timezone

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
# Wikimedia asks every client to identify itself: https://meta.wikimedia.org/wiki/User-Agent_policy
USER_AGENT = "spark-book-training/1.0 (educational Spark streaming exercise)"


def write_file(out_dir, events, seq):
    """Write atomically: hidden temp name first, then rename (readers ignore '.' files)."""
    name = f"rc_{datetime.now(timezone.utc):%Y%m%dT%H%M%S}_{seq:04d}.json"
    tmp = os.path.join(out_dir, "." + name + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        for e in events:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")
    os.replace(tmp, os.path.join(out_dir, name))
    kinds = {}
    for e in events:
        kinds[e.get("type")] = kinds.get(e.get("type"), 0) + 1
    print(f"wrote {name}: {len(events)} events {kinds}", flush=True)


def keep(event, args):
    if event.get("wiki") != args.wiki:
        return False
    if args.articles_only and event.get("namespace") != 0:
        return False
    if event.get("type") == "canary" or (event.get("meta") or {}).get("domain") == "canary":
        return False
    return True


def live_events(args):
    """Yield parsed events from the SSE stream, reconnecting with ?since= on drops."""
    import requests  # imported here so --fake works without requests installed

    since = None
    while True:
        params = {"since": since} if since else None
        try:
            with requests.get(
                STREAM_URL,
                params=params,
                headers={"User-Agent": USER_AGENT, "Accept": "text/event-stream"},
                stream=True,
                timeout=(10, 60),
            ) as resp:
                resp.raise_for_status()
                for line in resp.iter_lines(decode_unicode=True):
                    if not line or not line.startswith("data:"):
                        yield None        # heartbeat / other SSE field: lets the caller flush on time
                        continue
                    try:
                        event = json.loads(line[5:].strip())
                    except ValueError:
                        continue
                    since = (event.get("meta") or {}).get("dt") or since
                    yield event
        except KeyboardInterrupt:
            raise
        except Exception as exc:  # network hiccup: wait and resume where we were
            print(f"stream error ({exc.__class__.__name__}: {exc}); reconnecting in 3 s", file=sys.stderr)
            time.sleep(3)


TITLES = [
    "Apache Spark", "Delta Lake", "Change data capture", "Slowly changing dimension",
    "Data lake", "Kyiv", "Lviv", "Odesa", "Python (programming language)", "SQL",
    "Amsterdam", "Rotterdam", "Utrecht", "Netherlands", "Data warehouse",
    "Extract, transform, load", "Apache Kafka", "Stream processing", "Database", "Big data",
]


def fake_events(args):
    """Synthetic events with the same shape as the real stream (incl. deletes and late events)."""
    rev = 1_250_000_000
    created = set()
    while True:
        time.sleep(random.uniform(0.02, 0.15))
        title = random.choice(TITLES + [f"Draft topic {random.randint(1, 400)}"])
        now = int(time.time()) - (random.randint(30, 600) if random.random() < 0.03 else 0)  # 3% late
        user = random.choice(["AliceBot", "Bob", "Carol", "DanBot", "Eve", "Frank"]) + (
            "" if random.random() < 0.7 else str(random.randint(1, 99)))
        base = {
            "$schema": "/mediawiki/recentchange/1.0.0",
            "meta": {"domain": "en.wikipedia.org", "dt": datetime.fromtimestamp(now, timezone.utc).isoformat(),
                     "id": "".join(random.choices(string.hexdigits.lower(), k=32)), "stream": "mediawiki.recentchange"},
            "id": random.randint(1_800_000_000, 1_900_000_000),
            "namespace": 0, "title": title, "comment": random.choice(["typo", "ref added", "/* History */", "rv"]),
            "timestamp": now, "user": user, "bot": user.endswith("Bot"),
            "server_name": "en.wikipedia.org", "wiki": args.wiki,
        }
        r = random.random()
        if title not in created or r < 0.05:
            rev += 1
            created.add(title)
            base.update({"type": "new", "minor": False, "length": {"new": random.randint(200, 9000)},
                         "revision": {"new": rev}})
        elif r < 0.08:
            created.discard(title)
            base.update({"type": "log", "log_id": random.randint(1, 10**9), "log_type": "delete",
                         "log_action": "delete", "log_params": [], "log_action_comment": "deleted page"})
        elif r < 0.10:
            base.update({"type": "log", "log_id": random.randint(1, 10**9), "log_type": "move",
                         "log_action": "move", "log_params": {"target": title + " (moved)", "noredir": "0"}})
        else:
            rev += 1
            old = random.randint(200, 9000)
            base.update({"type": "edit", "minor": random.random() < 0.3,
                         "length": {"old": old, "new": max(0, old + random.randint(-400, 600))},
                         "revision": {"old": rev - random.randint(1, 50), "new": rev}})
        yield base


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    default_out = os.path.join(os.environ.get("BOOK_DATA", os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")),
                               "wiki", "landing")
    ap.add_argument("--out", default=default_out, help="landing folder (default: <BOOK_DATA>/wiki/landing)")
    ap.add_argument("--wiki", default="enwiki", help="wiki to keep (enwiki, nlwiki, dewiki, ukwiki, ...)")
    ap.add_argument("--seconds", type=float, default=20, help="write a new file every N seconds")
    ap.add_argument("--minutes", type=float, default=10, help="stop after N minutes")
    ap.add_argument("--all-namespaces", dest="articles_only", action="store_false",
                    help="also keep talk pages, user pages, ... (default: articles only)")
    ap.add_argument("--drift-after-files", type=int, default=0,
                    help="after N files, add a new nested field to every event (schema-drift exercise)")
    ap.add_argument("--fake", action="store_true", help="generate synthetic events instead of connecting")
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)
    source = fake_events(args) if args.fake else live_events(args)
    print(f"collecting {'FAKE ' if args.fake else ''}{args.wiki} events into {args.out} "
          f"for {args.minutes} min (Ctrl-C to stop early)", flush=True)

    buf, seq = [], 0
    started = last_flush = time.time()
    try:
        for event in source:
            if event is not None and keep(event, args):
                if args.drift_after_files and seq >= args.drift_after_files:
                    event["collector"] = {"version": 2, "received_at": datetime.now(timezone.utc).isoformat()}
                buf.append(event)
            now = time.time()
            if now - last_flush >= args.seconds:
                if buf:
                    write_file(args.out, buf, seq)
                    seq += 1
                    buf = []
                last_flush = now
            if now - started >= args.minutes * 60:
                break
    except KeyboardInterrupt:
        print("stopping…")
    if buf:
        write_file(args.out, buf, seq)
    print("done.")


if __name__ == "__main__":
    main()
