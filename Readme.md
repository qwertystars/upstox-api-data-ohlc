# Upstox API OHLC Data

Fetch, normalize, and save OHLCV market data from the Upstox API. This repository provides utilities to retrieve historical candles for instruments and export them to common formats for research, backtesting, or analytics.

If you’re just getting started with the Upstox API or want a ready-made workflow to collect data, this project is for you.

## Features

- Historical OHLCV candles for supported instruments
- Multiple intervals (e.g., 1m, 5m, 15m, 1h, 1d)
- Output to json
TODO
- Date-range batching with retry/backoff to respect API limits
- Output to CSV and/or Parquet
- Simple configuration via environment variables
- Reproducible runs for cron/schedulers

> Note: Names, script entrypoints, and module paths can differ based on your local setup. Adjust the commands/examples below to match your code layout if needed.

## Prerequisites

- Python 3.10+ recommended


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
- Run
  ```bash
  python data-ingest.py
  ```

## Project Structure

A typical layout might look like:
```
upstox-api-data-ohlc/
├─ requirements.txt
├─ data-ingest.py
└─ README.md
```


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


