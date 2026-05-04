"""
Fetches OHLCV + company fundamentals for the SP50 universe for a single date,
writes a temp CSV, uploads to GCS, then deletes the local file.

    python extract_from_yahoo.py --date 2026-04-20
"""

import argparse
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import yfinance as yf
from google.cloud import storage


PROJECT_ID  = "airflow-portfolio-494006"
BUCKET_NAME = "financial-pipeline-raw"
TMP_DIR     = Path("/tmp")

# BRK-B uses a hyphen on Yahoo, not a dot
TICKERS = [
    "MSFT", "NVDA", "AAPL", "AMZN", "META", "AVGO", "GOOGL", "TSLA", "BRK-B", "GOOG",
    "JPM",  "V",    "LLY",  "NFLX", "MA",   "COST", "XOM",   "WMT",  "PG",    "JNJ",
    "HD",   "ABBV", "BAC",  "UNH",  "KO",   "PM",   "CRM",   "ORCL", "CSCO",  "GE",
    "PLTR", "IBM",  "WFC",  "ABT",  "MCD",  "CVX",  "LIN",   "NOW",  "DIS",   "ACN",
    "T",    "ISRG", "MRK",  "UBER", "GS",   "INTU", "VZ",    "AMD",  "ADBE",  "RTX",
]

SLEEP_BETWEEN_TICKERS = 0.3  # avoid Yahoo rate-limiting


def fetch_ohlcv_batch(tickers, start, end):
    print(f"Downloading OHLCV for {len(tickers)} tickers...")
    raw = yf.download(
        tickers=tickers,
        start=start,
        end=end,
        interval="1d",
        auto_adjust=True,
        progress=False,
        group_by="ticker",
        threads=True,
    )

    if raw.empty:
        print("No OHLCV data returned.")
        return pd.DataFrame()

    rows = []
    for ticker in tickers:
        try:
            sub = raw[ticker].reset_index()
            sub["ticker"] = ticker
            rows.append(sub)
        except KeyError:
            print(f"  [warn] {ticker}: no OHLCV in batch result")
            continue

    if not rows:
        return pd.DataFrame()

    df = pd.concat(rows, ignore_index=True)
    df = df.rename(columns={
        "Date":   "date",
        "Open":   "open",
        "High":   "high",
        "Low":    "low",
        "Close":  "close",
        "Volume": "volume",
    })

    df["date"] = df["date"].astype(str)
    df["daily_pct_change"]    = ((df["close"] - df["open"]) / df["open"] * 100).round(4)
    df["intraday_volatility"] = (df["high"] - df["low"]).round(4)

    return df


def fetch_fundamentals(ticker):
    try:
        tk = yf.Ticker(ticker)
        info = tk.info or {}
        financials = tk.financials

        def get_financial(label):
            try:
                return financials.loc[label].iloc[0]
            except Exception:
                return None

        return {
            "company_name": info.get("shortName"),
            "sector":       info.get("sector"),
            "industry":     info.get("industry"),

            # Valuation
            "market_cap":    info.get("marketCap"),
            "pe_ratio":      info.get("trailingPE"),
            "eps":           info.get("trailingEps"),
            "book_value":    info.get("bookValue"),
            "price_to_book": info.get("priceToBook"),

            # Dividends
            "dividend":       info.get("dividendRate"),
            "dividend_yield": info.get("dividendYield"),

            # Risk
            "beta": info.get("beta"),

            # 52-week range
            "week_52_high": info.get("fiftyTwoWeekHigh"),
            "week_52_low":  info.get("fiftyTwoWeekLow"),

            # Income statement
            "total_revenue":    get_financial("Total Revenue"),
            "cost_of_revenue":  get_financial("Cost Of Revenue"),
            "gross_profit":     get_financial("Gross Profit"),
            "operating_income": get_financial("Operating Income"),
            "net_income":       get_financial("Net Income"),
            "ebitda":           get_financial("EBITDA"),
        }
    except Exception as e:
        print(f"  [error] {ticker}: fundamentals fetch failed — {e}")
        return None


def gcs_blob_exists(data_date: str) -> bool:
    client = storage.Client(project=PROJECT_ID)
    blob   = client.bucket(BUCKET_NAME).blob(f"raw/SP50_{data_date}.csv")
    return blob.exists(client)


def upload_to_gcs(local_path: Path, data_date: str):
    client = storage.Client(project=PROJECT_ID)
    blob   = client.bucket(BUCKET_NAME).blob(f"raw/SP50_{data_date}.csv")

    print(f"  Uploading to gs://{BUCKET_NAME}/raw/SP50_{data_date}.csv")
    blob.upload_from_filename(str(local_path), content_type="text/csv")
    blob.reload()
    print(f"  Upload done. Size: {blob.size:,} bytes  MD5: {blob.md5_hash}")


def fetch_and_save(data_date: str):
    """yfinance's `end` is exclusive, so the fetch window is [data_date, data_date+1)."""
    print(f"\nIngest run — {data_date}")
    print("-" * 60)

    if gcs_blob_exists(data_date):
        print(f"gs://{BUCKET_NAME}/raw/SP50_{data_date}.csv already exists. Skipping.")
        return

    start = datetime.strptime(data_date, "%Y-%m-%d")
    end   = start + timedelta(days=1)

    print(f"Fetching {len(TICKERS)} tickers for {data_date}\n")
    price_df = fetch_ohlcv_batch(TICKERS, start, end)

    if price_df.empty:
        print("No price data. Aborting.", file=sys.stderr)
        sys.exit(1)

    print(f"  OHLCV fetched for {price_df['ticker'].nunique()} tickers\n")

    print("Fetching fundamentals (one call per ticker)...")
    fundamentals_rows = []
    failed = []

    for i, ticker in enumerate(TICKERS, start=1):
        print(f"  [{i:>2}/{len(TICKERS)}] {ticker}", end=" ")
        fund = fetch_fundamentals(ticker)
        if fund is None:
            failed.append(ticker)
            print("x")
        else:
            fund["ticker"] = ticker
            fundamentals_rows.append(fund)
            print("ok")
        time.sleep(SLEEP_BETWEEN_TICKERS)

    if not fundamentals_rows:
        print("\nNo fundamentals fetched. Aborting.", file=sys.stderr)
        sys.exit(1)

    fund_df = pd.DataFrame(fundamentals_rows)

    df = price_df.merge(fund_df, on="ticker", how="inner")
    df["fetched_at"] = datetime.now(timezone.utc).isoformat()

    columns = [
        "date", "ticker", "company_name", "sector", "industry",
        "open", "high", "low", "close", "volume",
        "daily_pct_change", "intraday_volatility",
        "market_cap", "pe_ratio", "price_to_book", "eps", "book_value",
        "dividend", "dividend_yield",
        "beta",
        "week_52_high", "week_52_low",
        "total_revenue", "cost_of_revenue", "gross_profit",
        "operating_income", "net_income", "ebitda",
        "fetched_at",
    ]
    df = df[columns]

    tmp_path = TMP_DIR / f"SP50_{data_date}.csv"
    df.to_csv(tmp_path, index=False)
    print(f"\n  Wrote temp CSV: {tmp_path} ({tmp_path.stat().st_size:,} bytes)")

    try:
        upload_to_gcs(tmp_path, data_date)
    finally:
        if tmp_path.exists():
            os.remove(tmp_path)
            print(f"  Deleted temp file: {tmp_path}")

    print("\nSummary")
    print(f"  Date             : {data_date}")
    print(f"  Tickers attempted: {len(TICKERS)}")
    print(f"  Tickers saved    : {df['ticker'].nunique()}")
    print(f"  Tickers failed   : {len(failed)}  {failed if failed else ''}")
    print(f"  Rows             : {len(df)}")
    print(f"  Columns          : {len(df.columns)}")
    print(f"  GCS path         : gs://{BUCKET_NAME}/raw/SP50_{data_date}.csv")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Run date (YYYY-MM-DD)")
    args = parser.parse_args()
    fetch_and_save(args.date)