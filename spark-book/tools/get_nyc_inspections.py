"""Download NYC restaurant inspection results for the Chapter 8 capstone.

NYC's Department of Health inspects every restaurant roughly once or twice a
year and gives it a letter grade (A / B / C) that the restaurant must display
in its window. Grades, names (DBA = "doing business as"), cuisine and zip code
change slowly over time: a real slowly changing dimension.

Source (NYC Open Data, public, no key needed):
    https://data.cityofnewyork.us/Health/DOHMH-New-York-City-Restaurant-Inspection-Results/43nn-pn8j
API used: https://data.cityofnewyork.us/resource/43nn-pn8j.csv (Socrata SODA, SoQL query parameters)

Examples
--------
    python tools/get_nyc_inspections.py                              # Manhattan, graded since 2022
    python tools/get_nyc_inspections.py --boro Brooklyn --since 2023-01-01
    python tools/get_nyc_inspections.py --boro all
    python tools/get_nyc_inspections.py --fake                       # offline: synthetic, same columns
"""
import argparse
import csv
import io
import os
import random
import sys
from datetime import date, timedelta

API = "https://data.cityofnewyork.us/resource/43nn-pn8j.csv"
COLUMNS = [
    "camis", "dba", "boro", "zipcode", "cuisine_description", "inspection_date",
    "inspection_type", "violation_code", "score", "grade", "grade_date",
]
PAGE = 50_000


def download(args):
    import requests

    where = [f"grade_date >= '{args.since}T00:00:00'", "grade IN ('A', 'B', 'C')"]
    if args.boro.lower() != "all":
        where.append(f"boro = '{args.boro}'")
    params = {
        "$select": ",".join(COLUMNS),
        "$where": " AND ".join(where),
        "$order": "camis, grade_date, violation_code, :id",   # :id makes paging stable
        "$limit": PAGE,
    }
    headers = {"Accept": "text/csv"}
    if os.environ.get("SOCRATA_APP_TOKEN"):           # optional: higher rate limits
        headers["X-App-Token"] = os.environ["SOCRATA_APP_TOKEN"]

    total, offset = 0, 0
    with open(args.out, "w", newline="", encoding="utf-8") as out:
        writer = csv.writer(out)
        writer.writerow(COLUMNS)
        while True:
            params["$offset"] = offset
            resp = requests.get(API, params=params, headers=headers, timeout=120)
            resp.raise_for_status()
            rows = list(csv.reader(io.StringIO(resp.text)))
            body = rows[1:]                              # every page repeats the header
            if not body:
                break
            writer.writerows(body)
            total += len(body)
            offset += PAGE
            print(f"  {total:,} rows…", flush=True)
            if len(body) < PAGE:
                break
    print(f"wrote {total:,} rows to {args.out}")


CUISINES = ["Pizza", "Chinese", "Italian", "American", "Japanese", "Mexican", "Coffee/Tea",
            "Bakery Products/Desserts", "Indian", "Thai", "Korean", "Mediterranean"]
NAMES = ["Golden", "Little", "Joe's", "Lucky", "Corner", "Village", "Empire", "Harbor", "Sunny", "Broadway"]
KINDS = ["Pizza", "Kitchen", "Cafe", "Deli", "Noodle Bar", "Grill", "Bistro", "Bagels", "Taqueria", "Sushi"]


def fake(args):
    """Synthetic inspections with the real file's columns and quirks.

    Quirks reproduced: violation-level rows (several per inspection), grades that
    mostly stay but sometimes drop and recover, occasional renames (DBA changes),
    and a couple of restaurants that move zip code.
    """
    random.seed(42)
    start = date.fromisoformat(args.since)
    end = date.today()
    rows = 0
    with open(args.out, "w", newline="", encoding="utf-8") as out:
        w = csv.writer(out)
        w.writerow(COLUMNS)
        for i in range(args.fake_restaurants):
            camis = str(50_000_000 + i)
            dba = f"{random.choice(NAMES)} {random.choice(KINDS)}".upper()
            cuisine = random.choice(CUISINES)
            zipcode = str(random.choice([10001, 10002, 10003, 10011, 10012, 10013, 10014, 10016, 10019, 10036]))
            grade = random.choices("ABC", [0.8, 0.14, 0.06])[0]
            d = start + timedelta(days=random.randint(0, 180))
            while d <= end:
                r = random.random()
                if r < 0.12:
                    grade = random.choices("ABC", [0.5, 0.3, 0.2])[0]
                elif r < 0.30 and grade != "A":
                    grade = "A"
                if random.random() < 0.03:
                    dba = dba + " NYC" if not dba.endswith(" NYC") else dba[:-4]
                if random.random() < 0.01:
                    zipcode = str(int(zipcode) + 1)
                score = {"A": random.randint(0, 13), "B": random.randint(14, 27), "C": random.randint(28, 60)}[grade]
                ts = d.isoformat() + "T00:00:00.000"
                for v in random.sample(["02B", "04L", "06C", "08A", "10F", "04N", "06D"], random.randint(1, 4)):
                    w.writerow([camis, dba, args.boro if args.boro != "all" else "Manhattan", zipcode, cuisine,
                                ts, "Cycle Inspection / Initial Inspection", v, score, grade, ts])
                    rows += 1
                d += timedelta(days=random.randint(90, 330))
    print(f"wrote {rows:,} synthetic rows ({args.fake_restaurants} restaurants) to {args.out}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    data_dir = os.environ.get("BOOK_DATA", os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data"))
    ap.add_argument("--out", default=os.path.join(data_dir, "nyc", "inspections.csv"))
    ap.add_argument("--boro", default="Manhattan", help="Manhattan | Brooklyn | Queens | Bronx | Staten Island | all")
    ap.add_argument("--since", default="2022-01-01", help="only grades given on/after this date")
    ap.add_argument("--fake", action="store_true", help="generate synthetic data instead of downloading")
    ap.add_argument("--fake-restaurants", type=int, default=3000)
    args = ap.parse_args()
    args.out = os.path.abspath(args.out)
    os.makedirs(os.path.dirname(args.out), exist_ok=True)

    if args.fake:
        fake(args)
        return
    try:
        download(args)
    except Exception as exc:
        print(f"download failed ({exc.__class__.__name__}: {exc})\n"
              f"→ no internet / blocked? run again with --fake", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
