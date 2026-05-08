"""
Reads from fin_raw.stock_prices_raw and builds the curated layer:
    dim_company         (per-date snapshot, one row per ticker)
    fact_daily_prices   (per-date facts, one row per ticker)
    fact_weekly_summary (ISO-week rollup containing run_date)

    python3 transform.py --date 2026-04-20

Notes:
  - fin_raw is read with full history when computing facts. Window/shift ops
    need the history; output is filtered down to run_date before loading.
  - Per-ticker window ops use groupby("ticker") so values don't bleed between
    tickers.
  - dim_company and fact_daily_prices are APPEND-only. Existing (ticker, date)
    pairs are skipped to make re-runs idempotent.
  - fact_weekly_summary OVERWRITES the current ISO week (delete + insert).
    The week containing run_date may be in-progress, so we rebuild its row
    on every run within it.
"""

import argparse
from datetime import datetime, timezone
from google.cloud import bigquery
import pandas as pd
import numpy as np


PROJECT_ID = "airflow-portfolio-494006"
RAW_TABLE  = f"{PROJECT_ID}.fin_raw.stock_prices_raw"
DIM_CO     = f"{PROJECT_ID}.fin_curated.dim_company"
FACT_DAILY = f"{PROJECT_ID}.fin_curated.fact_daily_prices"
FACT_WEEK  = f"{PROJECT_ID}.fin_curated.fact_weekly_summary"


def now_utc():
    return datetime.now(timezone.utc)


def safe(val):
    try:
        if pd.isna(val):
            return None
        return val
    except Exception:
        return None


def safe_div(a, b) -> float | None:
    """Returns a/b * 100 as a percent, or None for null/zero denominator."""
    try:
        a, b = float(a), float(b)
        if b == 0 or pd.isna(a) or pd.isna(b):
            return None
        return round(a / b * 100, 4)
    except Exception:
        return None


def iso_week_start(d: pd.Timestamp) -> pd.Timestamp:
    return d - pd.Timedelta(days=d.weekday())


def read_raw_for_date(client, run_date):
    query = f"""
        SELECT *
        FROM `{RAW_TABLE}`
        WHERE date = DATE('{run_date}')
        ORDER BY ticker ASC
    """
    df = client.query(query).to_dataframe()
    print(f"  Read {len(df)} rows from fin_raw for date {run_date}")
    return df


def read_raw_history(client, run_date):
    """Used by fact builders so window/shift ops have enough history."""
    query = f"""
        SELECT *
        FROM `{RAW_TABLE}`
        WHERE date <= DATE('{run_date}')
        ORDER BY ticker ASC, date ASC
    """
    df = client.query(query).to_dataframe()
    print(f"  Read {len(df)} rows from fin_raw history up to {run_date}")
    return df


def build_dim_company_row(row, run_date, created_at):
    market_cap     = safe(row["market_cap"])
    total_revenue  = safe(row["total_revenue"])
    gross_profit   = safe(row["gross_profit"])
    net_income     = safe(row["net_income"])
    dividend_yield = safe(row["dividend_yield"])
    pe_ratio       = safe(row["pe_ratio"])

    return {
        # Snapshot key
        "date":                run_date,

        # Identity
        "ticker":              safe(row["ticker"]),
        "company_name":        safe(row["company_name"]),
        "sector":              safe(row["sector"]),
        "industry":            safe(row["industry"]),
        "exchange":            str(safe(row["exchange"])) if ("exchange" in row.index and safe(row["exchange"]) is not None) else "",
        "country":             "",

        # Valuation
        "market_cap":          market_cap,
        "market_cap_billions": round(float(market_cap) / 1_000_000_000, 2) if market_cap else None,
        "pe_ratio":            pe_ratio,
        "eps":                 safe(row["eps"]),
        "book_value":          safe(row["book_value"]),
        "price_to_book":       safe(row["price_to_book"]),
        "beta":                safe(row["beta"]),

        # Dividends
        "dividend":            safe(row["dividend"]),
        "dividend_yield_pct":  round(float(dividend_yield) * 100, 4) if dividend_yield else None,

        # 52-week range
        "week_52_high":        safe(row["week_52_high"]),
        "week_52_low":         safe(row["week_52_low"]),

        # Income statement
        "total_revenue":       total_revenue,
        "cost_of_revenue":     safe(row["cost_of_revenue"]),
        "gross_profit":        gross_profit,
        "gross_margin_pct":    safe_div(gross_profit, total_revenue),
        "operating_income":    safe(row["operating_income"]),
        "net_income":          net_income,
        "net_margin_pct":      safe_div(net_income, total_revenue),
        "ebitda":              safe(row["ebitda"]),

        # Flags
        "is_profitable":       bool(float(net_income) > 0) if net_income else None,
        "is_dividend_paying":  bool(dividend_yield and float(dividend_yield) > 0),

        # Audit
        "created_at":          created_at,
        "updated_at":          created_at,
    }


def build_dim_company(df, run_date):
    created_at = now_utc()
    rows = [build_dim_company_row(row, run_date, created_at) for _, row in df.iterrows()]
    out = pd.DataFrame(rows)
    out["date"] = pd.to_datetime(out["date"]).dt.date
    print(f"  Built dim_company: {len(out)} rows")
    return out


def build_fact_daily_prices(df_history, run_date):
    """
    Use full history as scratch:
      1. clean types
      2. sort by (ticker, date)
      3. compute windows GROUPED BY TICKER so tickers don't bleed
      4. filter output to run_date before returning
    """
    created_at = now_utc()

    if df_history.empty:
        print("  No history available — cannot build fact_daily_prices")
        return pd.DataFrame()

    df = df_history.copy()
    df["date"]   = pd.to_datetime(df["date"])
    df["ticker"] = df["ticker"].astype(str)
    df["open"]   = df["open"].astype(float).round(4)
    df["high"]   = df["high"].astype(float).round(4)
    df["low"]    = df["low"].astype(float).round(4)
    df["close"]  = df["close"].astype(float).round(4)
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").astype(float)
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)

    fact = pd.DataFrame()
    fact["date"]   = df["date"].dt.date
    fact["ticker"] = df["ticker"].values
    fact["open"]   = df["open"].values
    fact["high"]   = df["high"].values
    fact["low"]    = df["low"].values
    fact["close"]  = df["close"].values
    fact["volume"] = df["volume"].astype("Int64").values

    # Intraday metrics (no cross-row dependency)
    fact["daily_pct_change"]        = ((df["close"] - df["open"]) / df["open"] * 100).round(4).values
    fact["intraday_volatility"]     = (df["high"] - df["low"]).round(4).values
    fact["intraday_volatility_pct"] = ((df["high"] - df["low"]) / df["open"] * 100).round(4).values

    # Per-ticker shift for prev close + overnight gap
    prev_close = df.groupby("ticker")["close"].shift(1)
    fact["prev_close"]        = prev_close.round(4).values
    fact["overnight_gap"]     = (df["open"] - prev_close).round(4).values
    fact["overnight_gap_pct"] = ((df["open"] - prev_close) / prev_close * 100).round(4).values

    # Volume vs rolling 30-day avg, per ticker
    # groupby+rolling returns a MultiIndex; reset to align with df row order.
    rolling_vol = (
        df.groupby("ticker")["volume"]
          .rolling(window=30, min_periods=1)
          .mean()
          .reset_index(level=0, drop=True)
    )
    vol_vs_avg = ((df["volume"] - rolling_vol) / rolling_vol * 100).round(4)
    fact["volume_vs_avg_pct"] = vol_vs_avg.values

    # Flags
    fact["is_up_day"]          = (df["close"] > df["open"]).values
    fact["is_high_volatility"] = (fact["intraday_volatility_pct"] > 3.0).values
    fact["is_volume_spike"]    = fact["volume_vs_avg_pct"].apply(
        lambda x: bool(x > 50.0) if pd.notna(x) else None
    )

    fact["created_at"] = created_at

    run_date_obj = pd.to_datetime(run_date).date()
    fact = fact[fact["date"] == run_date_obj].reset_index(drop=True)

    print(f"  Built fact_daily_prices: {len(fact)} rows for {run_date}")
    return fact


def build_fact_weekly_summary(df_history, run_date):
    """
    Build the ISO week containing run_date. Uses full history to compute
    per-ticker moving averages, aggregates to weekly grain, then filters
    to the target week.
    """
    created_at = now_utc()

    if df_history.empty:
        print("  No history available — cannot build fact_weekly_summary")
        return pd.DataFrame()

    df = df_history.copy()
    df["date"]   = pd.to_datetime(df["date"])
    df["ticker"] = df["ticker"].astype(str)
    df["open"]   = df["open"].astype(float)
    df["high"]   = df["high"].astype(float)
    df["low"]    = df["low"].astype(float)
    df["close"]  = df["close"].astype(float)
    df["volume"] = df["volume"].astype(float)
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)

    df["ma_7_day"] = (
        df.groupby("ticker")["close"]
          .rolling(window=7, min_periods=1)
          .mean()
          .reset_index(level=0, drop=True)
          .round(4)
    )
    df["ma_30_day"] = (
        df.groupby("ticker")["close"]
          .rolling(window=30, min_periods=1)
          .mean()
          .reset_index(level=0, drop=True)
          .round(4)
    )

    df["week"] = df["date"].dt.to_period("W")

    weekly = df.groupby(["week", "ticker"]).agg(
        week_start    = ("date",     "min"),
        week_end      = ("date",     "max"),
        weekly_open   = ("open",     "first"),
        weekly_close  = ("close",    "last"),
        weekly_high   = ("high",     "max"),
        weekly_low    = ("low",      "min"),
        weekly_volume = ("volume",   "sum"),
        ma_7_day      = ("ma_7_day", "last"),
        ma_30_day     = ("ma_30_day","last"),
    ).reset_index()

    weekly["weekly_return_pct"] = (
        (weekly["weekly_close"] - weekly["weekly_open"]) / weekly["weekly_open"] * 100
    ).round(4)
    weekly["weekly_volatility"] = (weekly["weekly_high"] - weekly["weekly_low"]).round(4)
    weekly["avg_daily_volume"]  = (weekly["weekly_volume"] / 5).round(0)

    weekly["week_start"]    = pd.to_datetime(weekly["week_start"]).dt.date
    weekly["week_end"]      = pd.to_datetime(weekly["week_end"]).dt.date
    weekly["weekly_volume"] = weekly["weekly_volume"].astype("Int64")
    weekly["created_at"]    = created_at
    weekly = weekly.drop(columns=["week"])

    target_week_monday = iso_week_start(pd.to_datetime(run_date)).date()
    weekly = weekly[weekly["week_start"] == target_week_monday].reset_index(drop=True)

    print(f"  Built fact_weekly_summary: {len(weekly)} rows for week starting {target_week_monday}")
    return weekly


def get_existing_dim_tickers(client, run_date):
    try:
        query = f"""
            SELECT DISTINCT ticker
            FROM `{DIM_CO}`
            WHERE date = DATE('{run_date}')
        """
        result = client.query(query).to_dataframe()
        return set(result["ticker"].tolist())
    except Exception as e:
        print(f"  Note: could not read dim_company (maybe empty/new) — {e}")
        return set()


def get_existing_fact_tickers(client, run_date):
    try:
        query = f"""
            SELECT DISTINCT ticker
            FROM `{FACT_DAILY}`
            WHERE date = DATE('{run_date}')
        """
        result = client.query(query).to_dataframe()
        return set(result["ticker"].tolist())
    except Exception as e:
        print(f"  Note: could not read fact_daily_prices (maybe empty/new) — {e}")
        return set()


def delete_week_from_fact_weekly(client, week_start):
    """Returns rows deleted, or 0 if the table doesn't exist yet."""
    try:
        query = f"""
            DELETE FROM `{FACT_WEEK}`
            WHERE week_start = DATE('{week_start}')
        """
        job = client.query(query)
        job.result()
        return job.num_dml_affected_rows or 0
    except Exception as e:
        print(f"  Note: could not delete from fact_weekly_summary (maybe empty/new) — {e}")
        return 0


def load_to_curated(client, df, table_ref, write_mode="WRITE_APPEND"):
    if df.empty:
        print(f"  Skipped — no rows to load into {table_ref}")
        return

    disposition = (
        bigquery.WriteDisposition.WRITE_TRUNCATE
        if write_mode == "WRITE_TRUNCATE"
        else bigquery.WriteDisposition.WRITE_APPEND
    )
    job_config = bigquery.LoadJobConfig(
        write_disposition=disposition,
        autodetect=True,
    )
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print(f"  Loaded {len(df)} rows -> {table_ref}")


def run(run_date):
    print(f"\nTransform run started for {run_date}")
    print("-" * 60)

    client = bigquery.Client(project=PROJECT_ID)

    # dim_company
    print("\ndim_company")
    df_date = read_raw_for_date(client, run_date)
    if df_date.empty:
        print(f"  No data in fin_raw for {run_date}. Run ingestion+load first.")
        return

    existing_dim = get_existing_dim_tickers(client, run_date)
    if existing_dim:
        print(f"  Already snapshotted: {sorted(existing_dim)} ({len(existing_dim)})")
        df_date_filtered = df_date[~df_date["ticker"].isin(existing_dim)]
    else:
        print(f"  No existing snapshot for {run_date}")
        df_date_filtered = df_date
    print(f"  Tickers to load: {len(df_date_filtered)}")

    if df_date_filtered.empty:
        print("  All tickers already in dim_company — skipping load")
    else:
        dim = build_dim_company(df_date_filtered, run_date)
        load_to_curated(client, dim, DIM_CO, "WRITE_APPEND")

    # Read history once, reuse for both fact tables
    print("\nReading fin_raw history for fact builds")
    df_hist = read_raw_history(client, run_date)
    if df_hist.empty:
        print(f"  No history in fin_raw up to {run_date}. Skipping fact builds.")
        return

    # fact_daily_prices
    print("\nfact_daily_prices")
    existing_fact = get_existing_fact_tickers(client, run_date)
    if existing_fact:
        print(f"  Already loaded: {sorted(existing_fact)} ({len(existing_fact)})")
    else:
        print(f"  No existing fact rows for {run_date}")

    fact_daily = build_fact_daily_prices(df_hist, run_date)

    if not fact_daily.empty and existing_fact:
        before = len(fact_daily)
        fact_daily = fact_daily[~fact_daily["ticker"].isin(existing_fact)].reset_index(drop=True)
        print(f"  Filtered out existing tickers: {before} -> {len(fact_daily)} rows")

    load_to_curated(client, fact_daily, FACT_DAILY, "WRITE_APPEND")

    # fact_weekly_summary
    print("\nfact_weekly_summary")
    target_week_monday = iso_week_start(pd.to_datetime(run_date)).date()

    deleted = delete_week_from_fact_weekly(client, str(target_week_monday))
    print(f"  Deleted {deleted} rows for week starting {target_week_monday}")

    fact_weekly = build_fact_weekly_summary(df_hist, run_date)
    load_to_curated(client, fact_weekly, FACT_WEEK, "WRITE_APPEND")

    print(f"\nTransform complete for {run_date}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Run date (YYYY-MM-DD)")
    args = parser.parse_args()
    run(args.date)