# Upstox Historical Candle V3 - Resumable single-file JSON per company

What changed
- Fixed Windows PermissionError by using an atomic write with retries and a fallback.
- Data is now stored as normal JSON (not JSONL): one file per company per segment:
  - data_upstox_json/NSE_EQ/TRADING_SYMBOL.json
  - data_upstox_json/BSE_EQ/TRADING_SYMBOL.json

File structure
{
  "instrument": { instrument_key, segment, exchange, isin, trading_symbol, name },
  "timeframes": {
    "days|1": {
      "min_seen_date": "YYYY-MM-DD",
      "max_seen_date": "YYYY-MM-DD",
      "next_backfill_to_date": "YYYY-MM-DD",
      "done_backfill": false,
      "candles": [
        ["2025-01-01T00:00:00+05:30", 53.1, 53.95, 51.6, 52.05, 235519861, 0],
        ...
      ]
    },
    ...
  },
  "last_updated_utc": "....Z",
  "schema_version": 2
}

Behavior
- Preemptible/resumable: After each successful chunk, the file is updated with pointers so a crash can resume.
- Daily top-up: After backfill, the script fills forward so each timeframe reaches today's date if data exists.
- Dedup on merge: Candles are deduplicated by timestamp and kept sorted ascending.

Run
1) pip install python-dateutil aiohttp
2) Optionally set:
   - set UPSTOX_API_TOKEN=Bearer <token>   (Windows)
   - export UPSTOX_API_TOKEN="Bearer <token>" (Linux/macOS)
3) python data_ingest.py

Notes
- Normal JSON can grow large; this matches your request. If size becomes an issue, consider compressing at rest (.json.gz) or switching back to Parquet.