"""
Reads the SP50 CSV from GCS, runs validation checks, checks for per-ticker
duplicates already in BigQuery, logs a summary row to fin_raw.dq_results,
and loads the non-duplicate tickers into fin_raw.stock_prices_raw.

    python3 load_to_bigquery.py --date 2026-04-20
"""

import io
import uuid
import argparse
from datetime import datetime, timezone

import pandas as pd
from google.cloud import bigquery, storage


PROJECT_ID   = "airflow-portfolio-494006"
DATASET_ID   = "fin_raw"
TABLE_ID     = "stock_prices_raw"
DQ_TABLE_ID  = "dq_results"
TABLE_REF    = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
DQ_TABLE_REF = f"{PROJECT_ID}.{DATASET_ID}.{DQ_TABLE_ID}"
BUCKET_NAME  = "financial-pipeline-raw"

EXPECTED_COLUMNS = [
    "date", "ticker", "company_name", "sector", "industry",
    "open", "high", "low", "close", "volume",
    "daily_pct_change", "intraday_volatility",
    "market_cap", "pe_ratio", "price_to_book", "eps", "book_value",
    "dividend", "dividend_yield", "beta",
    "week_52_high", "week_52_low",
    "total_revenue", "cost_of_revenue", "gross_profit",
    "operating_income", "net_income", "ebitda",
    "fetched_at",
]

CRITICAL_COLUMNS = ["date", "ticker", "open", "high", "low", "close", "volume"]

EXPECTED_TICKERS = {
    "MSFT", "NVDA", "AAPL", "AMZN", "META", "AVGO", "GOOGL", "TSLA", "BRK-B", "GOOG",
    "JPM",  "V",    "LLY",  "NFLX", "MA",   "COST", "XOM",   "WMT",  "PG",    "JNJ",
    "HD",   "ABBV", "BAC",  "UNH",  "KO",   "PM",   "CRM",   "ORCL", "CSCO",  "GE",
    "PLTR", "IBM",  "WFC",  "ABT",  "MCD",  "CVX",  "LIN",   "NOW",  "DIS",   "ACN",
    "T",    "ISRG", "MRK",  "UBER", "GS",   "INTU", "VZ",    "AMD",  "ADBE",  "RTX",
}


def read_csv_from_gcs(data_date: str) -> pd.DataFrame | None:
    blob_name  = f"raw/SP50_{data_date}.csv"
    gcs_client = storage.Client(project=PROJECT_ID)
    blob       = gcs_client.bucket(BUCKET_NAME).blob(blob_name)

    if not blob.exists(gcs_client):
        print(f"  File not found in GCS: gs://{BUCKET_NAME}/{blob_name}")
        return None

    csv_bytes = blob.download_as_bytes()
    if len(csv_bytes) == 0:
        print(f"  File is empty: gs://{BUCKET_NAME}/{blob_name}")
        return None

    return pd.read_csv(io.BytesIO(csv_bytes))


def run_validations(df: pd.DataFrame, date: str) -> dict:
    failed = []
    passed = 0

    def check(name: str, condition: bool):
        nonlocal passed
        if condition:
            passed += 1
            print(f"  PASS  {name}")
        else:
            failed.append(name)
            print(f"  FAIL  {name}")

    print("\nRunning validations")

    check("file has rows", len(df) > 0)
    check(f"row count between 1 and {len(EXPECTED_TICKERS)}",
          1 <= len(df) <= len(EXPECTED_TICKERS))

    missing_cols = [c for c in EXPECTED_COLUMNS if c not in df.columns]
    check("all expected columns present", len(missing_cols) == 0)
    if missing_cols:
        print(f"         missing: {missing_cols}")

    check("no unexpected extra columns", len(df.columns) <= len(EXPECTED_COLUMNS) + 2)

    for col in CRITICAL_COLUMNS:
        if col in df.columns:
            check(f"no null in {col}", df[col].notna().all())

    if all(c in df.columns for c in ["open", "high", "low", "close"]):
        check("open > 0",  (df["open"]  > 0).all())
        check("high > 0",  (df["high"]  > 0).all())
        check("low > 0",   (df["low"]   > 0).all())
        check("close > 0", (df["close"] > 0).all())

        check("high >= low",   (df["high"] >= df["low"]).all())
        check("high >= open",  (df["high"] >= df["open"]).all())
        check("high >= close", (df["high"] >= df["close"]).all())

        check("low <= open",   (df["low"] <= df["open"]).all())
        check("low <= close",  (df["low"] <= df["close"]).all())

    if "volume" in df.columns:
        check("volume > 0", (df["volume"] > 0).all())

    if "daily_pct_change" in df.columns:
        check("daily_pct_change within -50% to +50%",
              ((df["daily_pct_change"] >= -50) & (df["daily_pct_change"] <= 50)).all())

    if "ticker" in df.columns:
        tickers_in_file = set(df["ticker"].unique())
        unknown_tickers = tickers_in_file - EXPECTED_TICKERS
        check("all tickers from expected universe", len(unknown_tickers) == 0)
        if unknown_tickers:
            print(f"         unknown: {unknown_tickers}")

    if "date" in df.columns:
        check(f"date matches {date}",
              (df["date"].astype(str).str.startswith(date)).all())

    if "market_cap" in df.columns:
        non_null = df["market_cap"].dropna()
        if len(non_null) > 0:
            check("market_cap > 0 where present", (non_null > 0).all())

    if "pe_ratio" in df.columns:
        non_null = df["pe_ratio"].dropna()
        if len(non_null) > 0:
            check("pe_ratio > 0 where present", (non_null > 0).all())

    check("no duplicate rows in file", df.duplicated().sum() == 0)
    check("no ticker appears more than once", df["ticker"].duplicated().sum() == 0)

    return {
        "passed":        len(failed) == 0,
        "checks_passed": passed,
        "checks_failed": len(failed),
        "failed_checks": failed,
    }


def check_duplicates_per_ticker(client, tickers, date):
    tickers_to_load = []
    tickers_to_skip = []

    for i, ticker in enumerate(tickers, start=1):
        query = f"""
            SELECT COUNT(*) AS cnt
            FROM `{TABLE_REF}`
            WHERE ticker = '{ticker}'
              AND DATE(date) = '{date}'
        """
        try:
            result = client.query(query).result()
            cnt = next(iter(result)).cnt
            if cnt > 0:
                tickers_to_skip.append(ticker)
                print(f"  [{i:>2}/{len(tickers)}] {ticker:<6} SKIP (already in BQ)")
            else:
                tickers_to_load.append(ticker)
                print(f"  [{i:>2}/{len(tickers)}] {ticker:<6} OK   (not in BQ yet)")
        except Exception:
            tickers_to_load.append(ticker)
            print(f"  [{i:>2}/{len(tickers)}] {ticker:<6} OK   (table empty or new)")

    return tickers_to_load, tickers_to_skip


def log_dq_result(client, run_id, date, validation_result, rows_checked,
                  tickers_loaded, tickers_skipped):
    failed_str_parts = list(validation_result["failed_checks"])
    if tickers_skipped:
        failed_str_parts.append(f"skipped (duplicate in BQ): {','.join(tickers_skipped)}")
    failed_str = "; ".join(failed_str_parts) or None

    row = {
        "run_id":        run_id,
        "ticker":        "SP50_BATCH",
        "date":          date,
        "status":        "passed" if validation_result["passed"] else "failed",
        "checks_passed": validation_result["checks_passed"],
        "checks_failed": validation_result["checks_failed"],
        "failed_checks": failed_str,
        "rows_checked":  rows_checked,
        "validated_at":  datetime.now(timezone.utc).isoformat(),
    }

    errors = client.insert_rows_json(DQ_TABLE_REF, [row])
    if errors:
        print(f"  Warning: dq_results log error: {errors}")
    else:
        print(f"  Logged batch row to fin_raw.dq_results — status: {row['status']}")
        print(f"    tickers loaded : {len(tickers_loaded)}")
        print(f"    tickers skipped: {len(tickers_skipped)}")


def load_df_to_bq(client, df):
    """
    FIX: pyarrow can't convert string 'date' column to BigQuery DATE type.
    We explicitly convert string -> Python date BEFORE the load. The BQ table
    has 'date' as DATE and 'fetched_at' as TIMESTAMP.
    """
    df = df.copy()
    df["date"]       = pd.to_datetime(df["date"], errors="coerce").dt.date
    df["fetched_at"] = pd.to_datetime(df["fetched_at"], errors="coerce", utc=True)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    load_job = client.load_table_from_dataframe(df, TABLE_REF, job_config=job_config)

    print(f"  Job started: {load_job.job_id}")
    print(f"  Rows queued for load: {len(df)}")
    print("  Waiting for job to complete...")
    load_job.result()

    table = client.get_table(TABLE_REF)
    print(f"  Total rows in table now: {table.num_rows}")


def run(date: str):
    run_id  = str(uuid.uuid4())
    gcs_uri = f"gs://{BUCKET_NAME}/raw/SP50_{date}.csv"

    print(f"\nSP50 batch pipeline run started")
    print(f"  run_id : {run_id}")
    print(f"  date   : {date}")
    print(f"  source : {gcs_uri}")
    print("-" * 60)

    print("\nReading CSV from GCS")
    df = read_csv_from_gcs(date)
    if df is None:
        print("  Aborting — file could not be read.")
        return
    print(f"  Rows: {len(df)}  Columns: {len(df.columns)}")

    print("\nValidating data")
    result = run_validations(df, date)

    print("\nConnecting to BigQuery")
    client = bigquery.Client(project=PROJECT_ID)
    print(f"  Connected to {PROJECT_ID}")

    print("\nPer-ticker duplicate check against BigQuery")
    tickers_in_file = df["ticker"].tolist()
    tickers_to_load, tickers_to_skip = check_duplicates_per_ticker(client, tickers_in_file, date)

    if not tickers_to_skip:
        result["checks_passed"] += 1
        print(f"  PASS  no tickers already in BQ — all {len(tickers_to_load)} will load")
    else:
        result["checks_failed"] += 1
        result["failed_checks"].append("some tickers already in BQ (partial load)")
        print(f"  WARN  {len(tickers_to_skip)} tickers already in BQ, will skip them")

    print("\nLogging to dq_results")
    log_dq_result(client, run_id, date, result, len(df), tickers_to_load, tickers_to_skip)

    print("\nLoading to BigQuery")
    if not result["passed"]:
        print(f"  Validation FAILED — data NOT loaded to BigQuery")
        print(f"  Failed checks: {result['failed_checks']}")
    elif not tickers_to_load:
        print(f"  All {len(tickers_in_file)} tickers already in BQ — nothing to load")
    else:
        df_to_load = df[df["ticker"].isin(tickers_to_load)].copy()
        load_df_to_bq(client, df_to_load)
        print(f"\n  Loaded {len(df_to_load)} rows to {TABLE_REF}")

    print("\nRun complete")
    print(f"  Status           : {'PASSED' if result['passed'] else 'FAILED'}")
    print(f"  Checks passed    : {result['checks_passed']}")
    print(f"  Checks failed    : {result['checks_failed']}")
    print(f"  Tickers in file  : {len(tickers_in_file)}")
    print(f"  Tickers loaded   : {len(tickers_to_load)}")
    print(f"  Tickers skipped  : {len(tickers_to_skip)}")
    if tickers_to_skip:
        print(f"  Skipped tickers  : {', '.join(tickers_to_skip)}")
    if result["failed_checks"]:
        print(f"  Failed checks    : {'; '.join(result['failed_checks'])}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Run date (YYYY-MM-DD)")
    args = parser.parse_args()
    run(args.date)