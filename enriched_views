-- enriched_views.sql
-- Analytical views for Looker Studio dashboard
-- Source: fin_curated.fact_daily_prices, fin_curated.dim_company
-- Author: Hari Kadam
-- Project: Financial Data Pipeline


-- ============================================================
-- VIEW 1: view_stock_performance
-- Daily stock returns and rankings for performance dashboard
-- ============================================================

CREATE OR REPLACE VIEW `airflow-portfolio-494006.fin_enriched.view_stock_performance` AS

WITH first_close AS (
    -- One row per ticker with the earliest closing price
    SELECT
        ticker,
        close AS first_close
    FROM (
        SELECT
            ticker,
            close,
            ROW_NUMBER() OVER (
                PARTITION BY ticker
                ORDER BY date ASC
            ) AS rn
        FROM `airflow-portfolio-494006.fin_curated.fact_daily_prices`
    )
    WHERE rn = 1
)

SELECT
    dp.date,
    dp.ticker,
    co.company_name,
    co.sector,
    dp.open,
    dp.high,
    dp.low,
    dp.close,
    dp.volume,
    dp.daily_pct_change,
    dp.intraday_volatility,
    dp.is_up_day,

    -- Total return since first tracked date
    ROUND(
        (dp.close - fc.first_close) / NULLIF(fc.first_close, 0) * 100
    , 2) AS cumulative_return_pct,

    -- Which stock performed best today
    RANK() OVER (
        PARTITION BY dp.date
        ORDER BY dp.daily_pct_change DESC
    ) AS daily_rank,

    -- 7 day high and low
    MAX(dp.high) OVER (
        PARTITION BY dp.ticker
        ORDER BY dp.date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS high_7d,

    MIN(dp.low) OVER (
        PARTITION BY dp.ticker
        ORDER BY dp.date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS low_7d,

    CASE
        WHEN dp.daily_pct_change >= 3.0  THEN 'strong gain'
        WHEN dp.daily_pct_change >= 1.0  THEN 'moderate gain'
        WHEN dp.daily_pct_change >= 0    THEN 'slight gain'
        WHEN dp.daily_pct_change >= -1.0 THEN 'slight loss'
        WHEN dp.daily_pct_change >= -3.0 THEN 'moderate loss'
        WHEN dp.daily_pct_change < -3.0  THEN 'strong loss'
        ELSE 'no data'
    END AS performance_label

FROM `airflow-portfolio-494006.fin_curated.fact_daily_prices` dp
LEFT JOIN `airflow-portfolio-494006.fin_curated.dim_company` co
    ON dp.ticker = co.ticker
LEFT JOIN first_close fc
    ON dp.ticker = fc.ticker;


-- ============================================================
-- VIEW 2: view_volatility_analysis
-- Intraday volatility trends for risk dashboard
-- ============================================================

CREATE OR REPLACE VIEW `airflow-portfolio-494006.fin_enriched.view_volatility_analysis` AS

WITH vol_calc AS (
    SELECT
        dp.date,
        dp.ticker,
        co.company_name,
        co.sector,
        co.beta,
        dp.intraday_volatility,
        dp.intraday_volatility_pct,
        dp.overnight_gap,
        dp.overnight_gap_pct,
        dp.is_high_volatility,
        dp.volume,
        dp.volume_vs_avg_pct,
        dp.is_volume_spike,

        -- 7 day rolling average volatility
        ROUND(AVG(dp.intraday_volatility_pct) OVER (
            PARTITION BY dp.ticker
            ORDER BY dp.date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 2) AS avg_volatility_7d,

        -- 30 day rolling average volatility
        ROUND(AVG(dp.intraday_volatility_pct) OVER (
            PARTITION BY dp.ticker
            ORDER BY dp.date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ), 2) AS avg_volatility_30d,

        -- Rank most volatile stocks per day
        RANK() OVER (
            PARTITION BY dp.date
            ORDER BY dp.intraday_volatility_pct DESC
        ) AS volatility_rank

    FROM `airflow-portfolio-494006.fin_curated.fact_daily_prices` dp
    LEFT JOIN `airflow-portfolio-494006.fin_curated.dim_company` co
        ON dp.ticker = co.ticker
)

SELECT
    date,
    ticker,
    company_name,
    sector,
    beta,
    intraday_volatility,
    intraday_volatility_pct,
    overnight_gap,
    overnight_gap_pct,
    volume,
    volume_vs_avg_pct,
    is_volume_spike,
    avg_volatility_7d,
    avg_volatility_30d,
    volatility_rank,
    is_high_volatility,

    -- How today compares to recent 7 day average
    ROUND(intraday_volatility_pct - avg_volatility_7d, 2) AS volatility_vs_7d_avg,

    CASE
        WHEN beta IS NULL THEN 'no data'
        WHEN beta < 0     THEN 'inverse market'
        WHEN beta < 0.5   THEN 'very low risk'
        WHEN beta < 1.0   THEN 'low risk'
        WHEN beta < 1.5   THEN 'moderate risk'
        ELSE                   'high risk'
    END AS risk_label

FROM vol_calc;


-- ============================================================
-- VIEW 3: view_moving_averages
-- Price trends and moving average state
-- ============================================================

CREATE OR REPLACE VIEW `airflow-portfolio-494006.fin_enriched.view_moving_averages` AS

WITH ma_calc AS (
    SELECT
        dp.date,
        dp.ticker,
        co.company_name,
        co.sector,
        dp.close,
        dp.daily_pct_change,

        ROUND(AVG(dp.close) OVER (
            PARTITION BY dp.ticker
            ORDER BY dp.date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 2) AS ma_7_day,

        ROUND(AVG(dp.close) OVER (
            PARTITION BY dp.ticker
            ORDER BY dp.date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ), 2) AS ma_30_day,

        MAX(dp.high) OVER (
            PARTITION BY dp.ticker
            ORDER BY dp.date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS high_7d,

        MIN(dp.low) OVER (
            PARTITION BY dp.ticker
            ORDER BY dp.date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS low_7d

    FROM `airflow-portfolio-494006.fin_curated.fact_daily_prices` dp
    LEFT JOIN `airflow-portfolio-494006.fin_curated.dim_company` co
        ON dp.ticker = co.ticker
)

SELECT
    date,
    ticker,
    company_name,
    sector,
    close,
    daily_pct_change,
    ma_7_day,
    ma_30_day,
    high_7d,
    low_7d,

    -- How far price sits above or below each moving average
    ROUND((close - ma_7_day)  / NULLIF(ma_7_day,  0) * 100, 2) AS price_vs_ma7_pct,
    ROUND((close - ma_30_day) / NULLIF(ma_30_day, 0) * 100, 2) AS price_vs_ma30_pct,

    -- Point-in-time trend state based on price and MA alignment
    CASE
        WHEN close > ma_7_day AND ma_7_day > ma_30_day THEN 'bullish'
        WHEN close < ma_7_day AND ma_7_day < ma_30_day THEN 'bearish'
        WHEN close > ma_7_day AND ma_7_day < ma_30_day THEN 'recovering'
        WHEN close < ma_7_day AND ma_7_day > ma_30_day THEN 'weakening'
        ELSE 'neutral'
    END AS trend_state

FROM ma_calc;


-- ============================================================
-- VIEW 4: view_fundamentals_screen
-- Key fundamental metrics for investment screening
-- ============================================================

CREATE OR REPLACE VIEW `airflow-portfolio-494006.fin_enriched.view_fundamentals_screen` AS

WITH latest_price AS (
    SELECT
        ticker,
        close AS current_price,
        date  AS price_date
    FROM (
        SELECT
            ticker,
            close,
            date,
            ROW_NUMBER() OVER (
                PARTITION BY ticker
                ORDER BY date DESC
            ) AS rn
        FROM `airflow-portfolio-494006.fin_curated.fact_daily_prices`
    )
    WHERE rn = 1
)

SELECT
    co.ticker,
    co.company_name,
    co.sector,
    co.industry,
    lp.current_price,
    lp.price_date,

    -- Size and valuation
    co.market_cap_billions,
    co.pe_ratio,
    co.eps,
    co.book_value,
    co.price_to_book,
    co.beta,

    -- Dividend
    co.dividend,
    co.dividend_yield_pct,

    -- 52 week range
    co.week_52_high,
    co.week_52_low,

    -- Where current price sits in 52 week range (0% = at low, 100% = at high)
    ROUND(
        LEAST(100, GREATEST(0,
            (lp.current_price - co.week_52_low) /
            NULLIF(co.week_52_high - co.week_52_low, 0) * 100
        ))
    , 1) AS position_in_52w_range_pct,

    -- Profitability
    co.gross_margin_pct,
    co.net_margin_pct,
    co.net_income,
    co.total_revenue,
    co.ebitda,
    co.is_profitable,
    co.is_dividend_paying,

    -- PE valuation bucket
    CASE
        WHEN co.pe_ratio IS NULL THEN 'no data'
        WHEN co.pe_ratio < 0     THEN 'negative earnings'
        WHEN co.pe_ratio < 15    THEN 'undervalued'
        WHEN co.pe_ratio <= 25   THEN 'fair value'
        ELSE                          'overvalued'
    END AS pe_flag,

    -- Composite quality score out of 100
    CASE WHEN co.is_profitable = TRUE    THEN 30 ELSE 0 END
    + CASE WHEN co.is_dividend_paying = TRUE THEN 20 ELSE 0 END
    + CASE
        WHEN co.pe_ratio IS NULL THEN 0
        WHEN co.pe_ratio < 15    THEN 30
        WHEN co.pe_ratio < 25    THEN 15
        ELSE 0
      END
    + CASE
        WHEN co.net_margin_pct IS NULL THEN 0
        WHEN co.net_margin_pct > 20    THEN 20
        WHEN co.net_margin_pct > 10    THEN 10
        ELSE 0
      END AS value_score,

    CASE
        WHEN co.is_profitable = TRUE
             AND co.pe_ratio IS NOT NULL
             AND co.pe_ratio < 25
             AND co.net_margin_pct > 10  THEN 'quality stock'
        WHEN co.is_profitable = TRUE
             AND co.is_dividend_paying = TRUE THEN 'income stock'
        WHEN co.is_profitable = FALSE    THEN 'speculative'
        ELSE 'watch'
    END AS investment_label

FROM `airflow-portfolio-494006.fin_curated.dim_company` co
LEFT JOIN latest_price lp
    ON co.ticker = lp.ticker
ORDER BY value_score DESC;