# Upstox API OHLC Data

Fetch, normalize, and save OHLCV market data from the Upstox API. This repository provides utilities to retrieve historical candles for instruments and export them to common formats for research, backtesting, or analytics.

If you’re just getting started with the Upstox API or want a ready-made workflow to collect data, this project is for you.

## Features

- Historical OHLCV candles for supported instruments
- Multiple intervals (e.g., 1m, 5m, 15m, 1h, 1d) when supported by the API
- Date-range batching with retry/backoff to respect API limits
- Output to CSV and/or Parquet
- Simple configuration via environment variables
- Reproducible runs for cron/schedulers

> Note: Names, script entrypoints, and module paths can differ based on your local setup. Adjust the commands/examples below to match your code layout if needed.

## Prerequisites

- Python 3.10+ recommended
- Upstox Developer Account and API credentials
  - API Key
  - API Secret
  - Redirect URI (for OAuth)
- A valid access token or refresh token to call the API

Helpful links:
- Upstox Developer portal and docs: https://upstox.com/developer/api-documentation/
- Upstox OAuth and token generation docs: https://upstox.com/developer/api-documentation/oauth

## Installation

- Clone the repository:
  ```bash
  git clone https://github.com/qwertystars/upstox-api-data-ohlc.git
  cd upstox-api-data-ohlc
  ```

- (Recommended) Create and activate a virtual environment:
  ```bash
  python -m venv .venv
  # Windows
  .venv\Scripts\activate
  # macOS/Linux
  source .venv/bin/activate
  ```

- Install dependencies:
  ```bash
  pip install -r requirements.txt
  ```
  If the project is packaged (pyproject.toml/setup.cfg), you can also do:
  ```bash
  pip install -e .
  ```

## Configuration

Set the following environment variables (use a .env file or your shell profile). Depending on your auth flow, you may use access tokens or refresh tokens.

```bash
# Required for OAuth/client setup
UPSTOX_API_KEY=your_api_key
UPSTOX_API_SECRET=your_api_secret
UPSTOX_REDIRECT_URI=https://your-redirect-uri.example.com/callback

# One of the following for authenticated calls
UPSTOX_ACCESS_TOKEN=your_access_token
# or
UPSTOX_REFRESH_TOKEN=your_refresh_token

# Optional
OUTPUT_DIR=./data
DEFAULT_EXCHANGE=NSE_EQ
DEFAULT_INTERVAL=15m
```

You can keep these in a `.env` file during development:
```
UPSTOX_API_KEY=...
UPSTOX_API_SECRET=...
UPSTOX_REDIRECT_URI=...
UPSTOX_ACCESS_TOKEN=...
OUTPUT_DIR=./data
DEFAULT_EXCHANGE=NSE_EQ
DEFAULT_INTERVAL=15m
```

If this repo includes a token generation helper (e.g., a script to complete the OAuth flow), run it first to obtain your tokens. Otherwise, follow the Upstox OAuth documentation to generate them and populate the variables above.

## Usage

Below are example patterns. Replace script/module names with those present in this repo.

- Fetch historical OHLCV for a single instrument and save to CSV:
  ```bash
  python scripts/fetch_ohlc.py \
    --exchange NSE_EQ \
    --symbol RELIANCE \
    --interval 15m \
    --start 2024-01-01 \
    --end 2024-03-31 \
    --out data/reliance_15m_20240101_20240331.csv
  ```

- Fetch and save Parquet:
  ```bash
  python scripts/fetch_ohlc.py \
    --exchange NSE_EQ \
    --symbol INFY \
    --interval 1d \
    --start 2020-01-01 \
    --end 2024-12-31 \
    --format parquet \
    --out data/infy_daily.parquet
  ```

- Batch fetch a list of symbols from a file:
  ```bash
  python scripts/fetch_ohlc_batch.py \
    --exchange NSE_EQ \
    --symbols-file symbols.txt \
    --interval 5m \
    --start 2024-07-01 \
    --end 2024-07-31 \
    --out-dir data/jul-5m
  ```

- Example Python usage:
  ```python
  from pathlib import Path
  from your_module.upstox_client import make_client
  from your_module.ohlc import fetch_candles_to_df, save_df

  client = make_client()  # reads env vars for credentials/tokens
  df = fetch_candles_to_df(
      client=client,
      exchange="NSE_EQ",
      symbol="RELIANCE",
      interval="15m",
      start="2024-01-01",
      end="2024-03-31",
  )
  Path("data").mkdir(exist_ok=True, parents=True)
  save_df(df, path="data/reliance_15m_20240101_20240331.csv", fmt="csv")
  ```

## Data Schema

Typical OHLCV fields (subject to what the API returns):
- timestamp (UTC)
- open
- high
- low
- close
- volume
- oi (Open Interest, if available)
- symbol / instrument_key
- interval

All timestamps are recommended to be stored in UTC. Confirm with your own code/API adapters.

## Rate Limits and Reliability

- The Upstox API enforces rate limits. This project uses batching and retries where possible.
- If you hit 429 or transient errors:
  - Increase backoff settings.
  - Narrow date ranges.
  - Cache intermediate results where applicable.

## Scheduling

For recurring collection (e.g., EOD):
- Create a cron job or use a scheduler like systemd timers, GitHub Actions, or Airflow.
- Example cron (runs daily at 18:30 IST / 13:00 UTC adjust accordingly):
  ```
  0 13 * * 1-5 /usr/bin/bash -lc 'cd /path/to/upstox-api-data-ohlc && . .venv/bin/activate && python scripts/fetch_eod.py >> logs/eod.log 2>&1'
  ```

## Project Structure

A typical layout might look like:
```
upstox-api-data-ohlc/
├─ scripts/
│  ├─ fetch_ohlc.py
│  ├─ fetch_ohlc_batch.py
│  └─ fetch_eod.py
├─ your_module/
│  ├─ __init__.py
│  ├─ upstox_client.py
│  ├─ ohlc.py
│  └─ io.py
├─ requirements.txt
├─ .env.example
└─ README.md
```

Adjust paths to match the actual files in this repository.

## Development

- Lint/format:
  ```bash
  ruff check .
  ruff format .
  ```
  or
  ```bash
  black .
  isort .
  flake8
  ```

- Tests:
  ```bash
  pytest -q
  ```

- Type-check:
  ```bash
  mypy your_module
  ```

## Troubleshooting

- Auth errors: Regenerate tokens and ensure env vars are set correctly.
- Empty data frames:
  - Verify symbol/exchange are valid and tradable for the requested date range.
  - Ensure interval is supported by the API for that instrument.
- Rate limit or timeout:
  - Add sleep/backoff and reduce request concurrency.
  - Shorten the date range per request.

## Roadmap

- [ ] CLI to discover/search instruments
- [ ] Parallelized batch downloads with safe rate-limiting
- [ ] Caching layer (e.g., sqlite/parquet partitioning)
- [ ] Example notebooks for basic analysis
- [ ] Optional Docker image for consistent runs

## Contributing

Issues and pull requests are welcome! Please:
- Open an issue describing the change or bug.
- Keep PRs focused and well-tested.

