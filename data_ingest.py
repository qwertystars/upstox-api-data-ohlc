#!/usr/bin/env python3
"""
Upstox Historical Candle V3 bulk harvester
Resumable + daily top-up to today + one JSON per company per segment (no JSONL)

Changes requested:
- Fix Windows PermissionError on atomic metadata writes: implemented robust atomic write with retries.
- Store data as normal JSON (one file per company per segment), not JSONL.
- Still preemptible: state is embedded in the same JSON file per instrument. If stopped, it resumes.
- Also tops up each file to include today's data if missing.

Output
- data_upstox_json/{segment}/{TRADING_SYMBOL}.json
  Structure:
  {
    "instrument": { instrument_key, segment, exchange, isin, trading_symbol, name },
    "timeframes": {
      "days|1": {
        "min_seen_date": "YYYY-MM-DD",
        "max_seen_date": "YYYY-MM-DD",
        "next_backfill_to_date": "YYYY-MM-DD",
        "done_backfill": false,
        "candles": [
          ["2025-01-01T00:00:00+05:30", open, high, low, close, volume, open_interest],
          ...
        ]  // sorted by timestamp asc, deduplicated by timestamp
      },
      ...
    },
    "last_updated_utc": "....Z",
    "schema_version": 2
  }

Notes
- JSON files can grow large. This meets your "normal JSON" and "1 file per company per segment" requirements.
- Atomic writes with retry mitigate Windows file locking (AV/scanner) issues.
"""

import asyncio
import aiohttp
import gzip
import json
import os
import random
import string
from pathlib import Path
from datetime import date, timedelta, datetime
from dateutil.relativedelta import relativedelta
import sys
import logging
import time
import typing
from urllib.parse import quote

# ----------------------------
# CONFIGURATION
# ----------------------------
INSTRUMENTS_URL = "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"
API_BASE = "https://api.upstox.com/v3/historical-candle"
OUTPUT_DIR = Path("data_upstox_json")
MAX_CONCURRENCY = 6
REQUESTS_PER_SECOND = 4
RETRY_COUNT = 3
RETRY_BACKOFF = 2.0
USER_AGENT = "upstox-bulk-harvester/1.3-json"

TIMEFRAMES = [
    ("days", "1"),
    ("hours", "4"),
    ("hours", "1"),
    ("minutes", "15"),
    ("minutes", "3"),
    ("minutes", "1"),
]

UNIT_AVAILABILITY = {
    "minutes": date(2022, 1, 1),
    "hours":   date(2022, 1, 1),
    "days":    date(2000, 1, 1),
    "weeks":   date(2000, 1, 1),
    "months":  date(2000, 1, 1),
}

CHUNK_RULES = {
    "days":    lambda interval: relativedelta(years=10),
    "hours":   lambda interval: relativedelta(months=3),
    "minutes": lambda interval: relativedelta(months=1) if int(interval) <= 15 else relativedelta(months=3),
    "weeks":   lambda interval: relativedelta(years=10),
    "months":  lambda interval: relativedelta(years=10),
}

HEADERS = {
    "Accept": "application/json",
    "User-Agent": USER_AGENT,
}
API_TOKEN = os.environ.get("UPSTOX_API_TOKEN") or os.environ.get("API_TOKEN")
if API_TOKEN:
    HEADERS["Authorization"] = API_TOKEN

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

# ----------------------------
# Helpers
# ----------------------------

def today_date() -> date:
    return date.today()

def safe_path_segment(s: str) -> str:
    if s is None:
        s = ""
    seg = quote(str(s), safe='-_.=')  # keep common safe chars
    reserved = {
        "CON","PRN","AUX","NUL","COM1","COM2","COM3","COM4","COM5","COM6","COM7","COM8","COM9",
        "LPT1","LPT2","LPT3","LPT4","LPT5","LPT6","LPT7","LPT8","LPT9"
    }
    if not seg:
        seg = "_"
    if seg.upper() in reserved:
        seg = f"_{seg}_"
    seg = seg.rstrip(" .")
    return seg

def instrument_outfile(segment: str, trading_symbol: str, instrument_key: str) -> Path:
    seg_dir = safe_path_segment(segment or "UNKNOWN")
    symbol = trading_symbol or instrument_key
    fname_base = safe_path_segment(symbol)
    base_dir = OUTPUT_DIR / seg_dir
    base_dir.mkdir(parents=True, exist_ok=True)
    return base_dir / f"{fname_base}.json"

def tf_key(unit: str, interval: str) -> str:
    return f"{unit}|{interval}"

def get_chunk_delta(unit: str, interval: str) -> relativedelta:
    return CHUNK_RULES.get(unit, lambda i: relativedelta(months=1))(interval)

def fmt_date(d: date) -> str:
    return d.isoformat()

def parse_date_from_timestamp(ts: str) -> date:
    # "YYYY-MM-DD..." -> first 10 chars
    return date.fromisoformat(ts[:10])

def iso_utc_now() -> str:
    return datetime.utcnow().isoformat() + "Z"

def rand_suffix(n: int = 6) -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=n))

def atomic_write_json(path: Path, obj: dict, max_retries: int = 10) -> None:
    """
    Robust atomic write with retries for Windows.
    - Write to a temp file in the same directory.
    - Flush+fsync.
    - os.replace() onto the destination with exponential backoff if locked.
    - As last resort, write directly (non-atomic).
    """
    directory = path.parent
    directory.mkdir(parents=True, exist_ok=True)
    tmp = directory / (path.name + f".{int(time.time()*1000)}.{rand_suffix()}.tmp")

    data = json.dumps(obj, separators=(",", ":"), ensure_ascii=False)
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(data)
        f.flush()
        try:
            os.fsync(f.fileno())
        except Exception:
            pass

    delay = 0.05
    last_err = None
    for attempt in range(max_retries):
        try:
            os.replace(tmp, path)
            return
        except PermissionError as e:
            last_err = e
            time.sleep(delay)
            delay = min(delay * 2, 1.0)
        except Exception as e:
            last_err = e
            time.sleep(delay)
            delay = min(delay * 2, 1.0)
    # Fallback: try direct write (non-atomic)
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(data)
            f.flush()
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass
        logging.warning("Atomic replace failed for %s; wrote non-atomically. Last error: %s", path, last_err)
    except Exception as e:
        # leave tmp behind for inspection
        logging.error("Failed to write %s (even fallback). Temp at %s. Error: %s", path, tmp, e)
        raise

def load_doc(path: Path) -> typing.Optional[dict]:
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logging.warning("Failed to read JSON %s: %s", path, e)
        return None

def init_doc(instrument: dict, outfile: Path) -> dict:
    return {
        "instrument": {
            "instrument_key": instrument.get("instrument_key"),
            "segment": instrument.get("segment"),
            "exchange": instrument.get("exchange"),
            "isin": instrument.get("isin"),
            "trading_symbol": instrument.get("trading_symbol"),
            "name": instrument.get("name"),
        },
        "timeframes": {},
        "last_updated_utc": iso_utc_now(),
        "schema_version": 2,
    }

def ensure_tf(doc: dict, unit: str, interval: str) -> dict:
    key = tf_key(unit, interval)
    tf = doc["timeframes"].get(key)
    if not tf:
        tf = {
            "min_seen_date": None,
            "max_seen_date": None,
            "next_backfill_to_date": None,
            "done_backfill": False,
            "candles": []
        }
        doc["timeframes"][key] = tf
    return tf

def dedup_and_merge(existing: list, new_list: list, mode: str) -> list:
    """
    Merge candles arrays with dedup by timestamp.
    - existing/new_list items are arrays [ts, o, h, l, c, v, oi]
    - mode: "forward" (append at end) or "backward" (prepend at start)
    Result must be sorted by timestamp ascending.
    """
    if not existing:
        # Ensure new_list sorted by ts
        new_sorted = sorted(new_list, key=lambda x: x[0])
        return new_sorted

    existing_ts = set(c[0] for c in existing)
    filtered = [c for c in new_list if c[0] not in existing_ts]
    if not filtered:
        return existing

    if mode == "forward":
        # Assume filtered are >= existing last; still sort to be safe
        filtered.sort(key=lambda x: x[0])
        return existing + filtered
    else:
        # backward: filtered should be <= existing first; sort and prepend
        filtered.sort(key=lambda x: x[0])
        return filtered + existing

def update_tf_bounds(tf: dict):
    if not tf["candles"]:
        tf["min_seen_date"] = None
        tf["max_seen_date"] = None
        return
    first_d = parse_date_from_timestamp(tf["candles"][0][0])
    last_d = parse_date_from_timestamp(tf["candles"][-1][0])
    tf["min_seen_date"] = first_d.isoformat()
    tf["max_seen_date"] = last_d.isoformat()

# ----------------------------
# Networking
# ----------------------------

async def download_instruments(session: aiohttp.ClientSession) -> list:
    logging.info("Downloading instruments list from %s", INSTRUMENTS_URL)
    async with session.get(INSTRUMENTS_URL, headers={"User-Agent": USER_AGENT}) as resp:
        resp.raise_for_status()
        content = await resp.read()
    buf = gzip.decompress(content)
    instruments = json.loads(buf.decode("utf-8"))
    eqs = [ins for ins in instruments if ins.get("instrument_type") == "EQ"]
    logging.info("Parsed %d instruments, EQ only: %d", len(instruments), len(eqs))
    return eqs

async def fetch_candles_chunk(session: aiohttp.ClientSession, instrument_key: str, unit: str, interval: str, to_date_d: date, from_date_d: date) -> typing.Optional[list]:
    instrument_key_enc = quote(instrument_key, safe='')
    unit_enc = quote(unit, safe='')
    interval_enc = quote(interval, safe='')
    to_enc = quote(fmt_date(to_date_d), safe='')
    from_enc = quote(fmt_date(from_date_d), safe='')
    url = f"{API_BASE}/{instrument_key_enc}/{unit_enc}/{interval_enc}/{to_enc}/{from_enc}"

    for attempt in range(1, RETRY_COUNT + 1):
        try:
            async with session.get(url, headers=HEADERS) as resp:
                text = await resp.text()
                if resp.status == 200:
                    try:
                        payload = json.loads(text)
                    except Exception as e:
                        logging.warning("JSON parse error for %s: %s", url, e)
                        return None
                    return payload.get("data", {}).get("candles", [])
                elif 400 <= resp.status < 500:
                    logging.warning("Client %s for %s: %s", resp.status, url, text[:200])
                    return []
                else:
                    logging.warning("Server %s for %s (attempt %d): %s", resp.status, url, attempt, text[:200])
        except Exception as e:
            logging.warning("Network error on %s (attempt %d): %s", url, attempt, e)
        await asyncio.sleep(RETRY_BACKOFF ** (attempt - 1))
    logging.error("Exceeded retries for %s", url)
    return None

# ----------------------------
# Core processing
# ----------------------------

async def backfill_timeframe(session: aiohttp.ClientSession, instrument: dict, doc: dict, outfile: Path, unit: str, interval: str):
    tf = ensure_tf(doc, unit, interval)
    if tf.get("done_backfill"):
        return

    unit_start = UNIT_AVAILABILITY.get(unit, date(2000, 1, 1))
    to_date_d = min(today_date(), date.fromisoformat(tf["next_backfill_to_date"])) if tf.get("next_backfill_to_date") else today_date()
    chunk_delta = get_chunk_delta(unit, interval)

    while True:
        tentative_from = to_date_d - chunk_delta + timedelta(days=1)
        from_date_d = max(tentative_from, unit_start)
        if from_date_d > to_date_d:
            tf["done_backfill"] = True
            doc["last_updated_utc"] = iso_utc_now()
            atomic_write_json(outfile, doc)
            break

        candles = await fetch_candles_chunk(session, instrument["instrument_key"], unit, interval, to_date_d, from_date_d)
        if candles is None:
            break
        if not candles:
            tf["done_backfill"] = True
            doc["last_updated_utc"] = iso_utc_now()
            atomic_write_json(outfile, doc)
            break

        # Merge (prepend because we're going backwards)
        tf["candles"] = dedup_and_merge(tf["candles"], candles, mode="backward")
        update_tf_bounds(tf)

        # Move pointer back one day before this chunk
        to_date_d = from_date_d - timedelta(days=1)
        tf["next_backfill_to_date"] = to_date_d.isoformat()

        # Persist after each chunk (resumable)
        doc["last_updated_utc"] = iso_utc_now()
        atomic_write_json(outfile, doc)

        await asyncio.sleep(1.0 / max(REQUESTS_PER_SECOND, 1))

    logging.info("Backfill %s %s for %s -> min=%s max=%s done=%s",
                 unit, interval, instrument.get("trading_symbol") or instrument["instrument_key"],
                 tf.get("min_seen_date"), tf.get("max_seen_date"), tf.get("done_backfill"))

async def forward_fill_to_today(session: aiohttp.ClientSession, instrument: dict, doc: dict, outfile: Path, unit: str, interval: str):
    tf = ensure_tf(doc, unit, interval)
    if not tf.get("max_seen_date"):
        return

    cur_from = date.fromisoformat(tf["max_seen_date"]) + timedelta(days=1)
    end_target = today_date()
    if cur_from > end_target:
        return

    chunk_delta = get_chunk_delta(unit, interval)
    while cur_from <= end_target:
        cur_to = min(end_target, (cur_from + (chunk_delta - timedelta(days=1))))
        candles = await fetch_candles_chunk(session, instrument["instrument_key"], unit, interval, cur_to, cur_from)
        if candles is None:
            break
        if not candles:
            break

        # Merge at the end (forward)
        tf["candles"] = dedup_and_merge(tf["candles"], candles, mode="forward")
        update_tf_bounds(tf)

        doc["last_updated_utc"] = iso_utc_now()
        atomic_write_json(outfile, doc)

        cur_from = cur_to + timedelta(days=1)
        await asyncio.sleep(1.0 / max(REQUESTS_PER_SECOND, 1))

    logging.info("Forward top-up %s %s for %s -> now max=%s",
                 unit, interval, instrument.get("trading_symbol") or instrument["instrument_key"],
                 tf.get("max_seen_date"))

async def fetch_for_instrument(session: aiohttp.ClientSession, sem: asyncio.Semaphore, instrument: dict):
    async with sem:
        instrument_key = instrument.get("instrument_key")
        if not instrument_key:
            logging.warning("Skipping instrument with no instrument_key: %s", instrument.get("trading_symbol"))
            return

        outfile = instrument_outfile(instrument.get("segment"), instrument.get("trading_symbol"), instrument_key)
        doc = load_doc(outfile)
        if not doc:
            doc = init_doc(instrument, outfile)
            atomic_write_json(outfile, doc)

        # Backfill historically (resumable)
        for unit, interval in TIMEFRAMES:
            await backfill_timeframe(session, instrument, doc, outfile, unit, interval)

        # Then ensure up to today
        for unit, interval in TIMEFRAMES:
            await forward_fill_to_today(session, instrument, doc, outfile, unit, interval)

        logging.info("Finished %s (%s) -> %s", instrument.get("trading_symbol") or instrument_key, instrument_key, outfile)

async def topup_existing_files_to_today(session: aiohttp.ClientSession, sem: asyncio.Semaphore):
    for jf in OUTPUT_DIR.rglob("*.json"):
        doc = load_doc(jf)
        if not doc:
            continue
        inst = doc.get("instrument") or {}
        instrument = {
            "instrument_key": inst.get("instrument_key"),
            "segment": inst.get("segment"),
            "exchange": inst.get("exchange"),
            "isin": inst.get("isin"),
            "trading_symbol": inst.get("trading_symbol"),
            "name": inst.get("name"),
        }
        if not instrument["instrument_key"]:
            continue
        for key in list((doc.get("timeframes") or {}).keys()):
            try:
                unit, interval = key.split("|", 1)
            except Exception:
                continue
            await forward_fill_to_today(session, instrument, doc, jf, unit, interval)

# ----------------------------
# Main
# ----------------------------

async def main():
    conn = aiohttp.TCPConnector(limit=MAX_CONCURRENCY * 2)
    timeout = aiohttp.ClientTimeout(total=600)
    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    async with aiohttp.ClientSession(connector=conn, timeout=timeout) as session:
        instruments = await download_instruments(session)
        logging.info("Preparing to fetch %d instruments", len(instruments))

        tasks = [fetch_for_instrument(session, sem, ins) for ins in instruments]
        futures = [asyncio.create_task(t) for t in tasks]
        completed = 0
        total = len(futures)
        for fut in asyncio.as_completed(futures):
            try:
                await fut
            except Exception as e:
                logging.exception("Task failed: %s", e)
            completed += 1
            logging.info("Progress: %d/%d instruments completed", completed, total)

        # Optional: top up all existing JSON files (covers delisted symbols etc.)
        await topup_existing_files_to_today(session, sem)

        logging.info("All done.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Interrupted by user, shutting down.")