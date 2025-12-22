-- ======================================================================
-- Daily offline module: sentiment scoring + ARIMA_PLUS_XREG evaluation
-- ======================================================================

CREATE OR REPLACE PROCEDURE
  `big-data-crypto-sentiment-test.analytical_module.run_daily_offline_module`()
BEGIN
  DECLARE eval_days INT64 DEFAULT 7;
  DECLARE cutoff_ts TIMESTAMP DEFAULT TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL eval_days DAY);
  DECLARE sym STRING;
  DECLARE sym_safe STRING;
  DECLARE train_rows INT64;
  DECLARE first BOOL DEFAULT TRUE;
  -- For testing: lower threshold so at least something trains.
  DECLARE min_train_rows INT64 DEFAULT 1;
  DECLARE horizon_minutes INT64 DEFAULT 60;
DECLARE conf_level FLOAT64 DEFAULT 0.9;
  -- --------------------------------------------------------------------
  -- (A) Remote sentiment model (Cloud Natural Language)
  -- --------------------------------------------------------------------
  CREATE OR REPLACE MODEL
    `big-data-crypto-sentiment-test.analytical_module.nlp_sentiment`
  REMOTE WITH CONNECTION `722178606486.eu.nlp_conn`
  OPTIONS(REMOTE_SERVICE_TYPE = 'CLOUD_AI_NATURAL_LANGUAGE_V1');

  -- --------------------------------------------------------------------
  -- (B) Score sentiment for all tweets (unnest tweet_texts)
  -- --------------------------------------------------------------------
  CREATE OR REPLACE TABLE `big-data-crypto-sentiment-test.analytical_module.tweet_sentiment` AS
  SELECT
    tweet_id,
    event_timestamp,
    symbol,
    text_content AS tweet_text,
    SAFE_CAST(
      COALESCE(
        JSON_VALUE(sentiment, '$.documentSentiment.score'),
        JSON_VALUE(sentiment, '$.score')
      ) AS FLOAT64
    ) AS sentiment_score,
    SAFE_CAST(
      COALESCE(
        JSON_VALUE(sentiment, '$.documentSentiment.magnitude'),
        JSON_VALUE(sentiment, '$.magnitude')
      ) AS FLOAT64
    ) AS sentiment_magnitude,
    language,
    ml_understand_text_status
  FROM ML.UNDERSTAND_TEXT(
    MODEL `big-data-crypto-sentiment-test.analytical_module.nlp_sentiment`,
    (
      SELECT
        FARM_FINGERPRINT(CONCAT(CAST(event_timestamp AS STRING), '|', symbol, '|', CAST(off AS STRING))) AS tweet_id,
        event_timestamp,
        symbol,
        TRIM(tweet) AS text_content
      FROM `big-data-crypto-sentiment-test.crypto_analysis_eu.crypto_prices_with_tweets`,
      UNNEST(IFNULL(tweet_texts, [])) AS tweet WITH OFFSET off
      WHERE TRIM(tweet) != ''
      ORDER BY event_timestamp DESC
    ),
    STRUCT('ANALYZE_SENTIMENT' AS nlu_option, TRUE AS flatten_json_output)
  );

  -- --------------------------------------------------------------------
  -- (C) Aggregate sentiment to 1-minute
  -- --------------------------------------------------------------------
  CREATE OR REPLACE TABLE `big-data-crypto-sentiment-test.analytical_module.sentiment_1m` AS
  SELECT
    TIMESTAMP_TRUNC(event_timestamp, MINUTE) AS minute_ts,
    symbol,
    AVG(sentiment_score) AS sentiment_score_mean,
    AVG(sentiment_magnitude) AS sentiment_magnitude_mean,
    COUNT(1) AS tweet_count
  FROM `big-data-crypto-sentiment-test.analytical_module.tweet_sentiment`
  WHERE (ml_understand_text_status IS NULL OR ml_understand_text_status = '')
  GROUP BY 1, 2;

  -- --------------------------------------------------------------------
  -- (D) Build 1-minute price table (last_price = last in minute)
  -- --------------------------------------------------------------------
  CREATE OR REPLACE TABLE `big-data-crypto-sentiment-test.analytical_module.prices_1m` AS
  SELECT
    TIMESTAMP_TRUNC(event_timestamp, MINUTE) AS minute_ts,
    symbol,
    ARRAY_AGG(last_price ORDER BY event_timestamp DESC LIMIT 1)[OFFSET(0)] AS last_price,
    AVG(avg_price) AS avg_price,
    SUM(tweet_volume) AS tweet_volume_1m
  FROM `big-data-crypto-sentiment-test.crypto_analysis_eu.crypto_prices_with_tweets`
  GROUP BY 1, 2;

  -- --------------------------------------------------------------------
  -- (E) Join features + build lag-1 regressors (impute missing to 0)
  -- --------------------------------------------------------------------
  CREATE OR REPLACE TABLE `big-data-crypto-sentiment-test.analytical_module.features_1m` AS
  WITH joined AS (
    SELECT
      p.minute_ts,
      p.symbol,
      p.last_price,
      p.avg_price,
      p.tweet_volume_1m,
      IFNULL(s.sentiment_score_mean, 0.0) AS sentiment_score_mean,
      IFNULL(s.sentiment_magnitude_mean, 0.0) AS sentiment_magnitude_mean,
      IFNULL(s.tweet_count, 0) AS tweet_count
    FROM `big-data-crypto-sentiment-test.analytical_module.prices_1m` p
    LEFT JOIN `big-data-crypto-sentiment-test.analytical_module.sentiment_1m` s
    USING (minute_ts, symbol)
  ),
  lagged AS (
    SELECT
      *,
      LAG(sentiment_score_mean, 1) OVER (PARTITION BY symbol ORDER BY minute_ts) AS sentiment_score_mean_lag1,
      LAG(sentiment_magnitude_mean, 1) OVER (PARTITION BY symbol ORDER BY minute_ts) AS sentiment_magnitude_mean_lag1,
      LAG(tweet_count, 1) OVER (PARTITION BY symbol ORDER BY minute_ts) AS tweet_count_lag1,
      LAG(tweet_volume_1m, 1) OVER (PARTITION BY symbol ORDER BY minute_ts) AS tweet_volume_1m_lag1
    FROM joined
  )
  SELECT
    *
  FROM lagged
  WHERE sentiment_score_mean_lag1 IS NOT NULL;

  -- --------------------------------------------------------------------
  -- (F) Train + evaluate ARIMA_PLUS_XREG per symbol
  --     ARIMA_PLUS_XREG is multivariate but effectively single-series;
  -- --------------------------------------------------------------------

  SET cutoff_ts = (
    WITH bounds AS (
      SELECT
        MIN(minute_ts) AS min_ts,
        MAX(minute_ts) AS max_ts,
        TIMESTAMP_DIFF(MAX(minute_ts), MIN(minute_ts), MINUTE) AS span_min
      FROM `big-data-crypto-sentiment-test.analytical_module.features_1m`
    ),
    params AS (
      SELECT
        max_ts,
        -- Prefer last eval_days days (in minutes). If not enough history,
        -- fall back to last 120 minutes; else last 30; else 20% holdout.
        CASE
          WHEN span_min IS NULL THEN NULL
          WHEN span_min >= (eval_days * 1440) + 60 THEN eval_days * 1440
          WHEN span_min >= 240 THEN 120
          WHEN span_min >= 90 THEN 30
          ELSE GREATEST(1, CAST(span_min * 0.2 AS INT64))
        END AS eval_minutes
      FROM bounds
    )
    SELECT
      TIMESTAMP_SUB(max_ts, INTERVAL eval_minutes MINUTE)
    FROM params
  );

  -- If features_1m is empty, stop early.
  IF cutoff_ts IS NULL THEN
    -- Nothing to train/evaluate on
    RETURN;
  END IF;

  -- reset flag so we CREATE OR REPLACE the *_latest tables every run
  SET first = TRUE;

  FOR r IN (
    SELECT DISTINCT symbol
    FROM `big-data-crypto-sentiment-test.analytical_module.features_1m`
    WHERE symbol IS NOT NULL
    ORDER BY symbol
  ) DO
    SET sym = r.symbol;
    SET sym_safe = REGEXP_REPLACE(sym, r'[^A-Za-z0-9_]', '_');

    SET train_rows = (
      SELECT COUNT(*)
      FROM `big-data-crypto-sentiment-test.analytical_module.features_1m`
      WHERE symbol = sym
        AND minute_ts < cutoff_ts
        AND last_price IS NOT NULL
    );

    IF train_rows < min_train_rows THEN
      CONTINUE;
    END IF;

    -- Train per-symbol model
    EXECUTE IMMEDIATE FORMAT("""
      CREATE OR REPLACE MODEL `big-data-crypto-sentiment-test.analytical_module.arima_price_xreg_%s`
      OPTIONS(
        model_type = 'ARIMA_PLUS_XREG',
        time_series_timestamp_col = 'minute_ts',
        time_series_data_col = 'last_price',
        data_frequency = 'PER_MINUTE',
        horizon = 60,
        auto_arima = TRUE
      ) AS
      SELECT
        minute_ts,
        last_price,
        sentiment_score_mean_lag1,
        sentiment_magnitude_mean_lag1,
        tweet_count_lag1,
        tweet_volume_1m_lag1
      FROM `big-data-crypto-sentiment-test.analytical_module.features_1m`
      WHERE symbol = @sym
        AND minute_ts < @cutoff
        AND last_price IS NOT NULL
    """, sym_safe)
    USING sym AS sym, cutoff_ts AS cutoff;

    -- Candidates
    IF first THEN
      EXECUTE IMMEDIATE FORMAT("""
        CREATE OR REPLACE TABLE `big-data-crypto-sentiment-test.analytical_module.arima_candidates_latest` AS
        SELECT CURRENT_TIMESTAMP() AS run_at, @sym AS symbol, *
        FROM ML.ARIMA_EVALUATE(
          MODEL `big-data-crypto-sentiment-test.analytical_module.arima_price_xreg_%s`
        )
      """, sym_safe)
      USING sym AS sym;
    ELSE
      EXECUTE IMMEDIATE FORMAT("""
        INSERT INTO `big-data-crypto-sentiment-test.analytical_module.arima_candidates_latest`
        SELECT CURRENT_TIMESTAMP() AS run_at, @sym AS symbol, *
        FROM ML.ARIMA_EVALUATE(
          MODEL `big-data-crypto-sentiment-test.analytical_module.arima_price_xreg_%s`
        )
      """, sym_safe)
      USING sym AS sym;
    END IF;

    -- Coefficients
    IF first THEN
      EXECUTE IMMEDIATE FORMAT("""
        CREATE OR REPLACE TABLE `big-data-crypto-sentiment-test.analytical_module.arima_coefficients_latest` AS
        SELECT CURRENT_TIMESTAMP() AS run_at, @sym AS symbol, *
        FROM ML.ARIMA_COEFFICIENTS(
          MODEL `big-data-crypto-sentiment-test.analytical_module.arima_price_xreg_%s`
        )
      """, sym_safe)
      USING sym AS sym;
    ELSE
      EXECUTE IMMEDIATE FORMAT("""
        INSERT INTO `big-data-crypto-sentiment-test.analytical_module.arima_coefficients_latest`
        SELECT CURRENT_TIMESTAMP() AS run_at, @sym AS symbol, *
        FROM ML.ARIMA_COEFFICIENTS(
          MODEL `big-data-crypto-sentiment-test.analytical_module.arima_price_xreg_%s`
        )
      """, sym_safe)
      USING sym AS sym;
    END IF;

    -- Evaluate on holdout window (known prices)
    IF first THEN
      EXECUTE IMMEDIATE FORMAT("""
        CREATE OR REPLACE TABLE `big-data-crypto-sentiment-test.analytical_module.eval_metrics_latest` AS
        SELECT CURRENT_TIMESTAMP() AS run_at, @sym AS symbol, *
        FROM ML.EVALUATE(
          MODEL `big-data-crypto-sentiment-test.analytical_module.arima_price_xreg_%s`,
          (
            SELECT
              minute_ts,
              last_price,
              sentiment_score_mean_lag1,
              sentiment_magnitude_mean_lag1,
              tweet_count_lag1,
              tweet_volume_1m_lag1
            FROM `big-data-crypto-sentiment-test.analytical_module.features_1m`
            WHERE symbol = @sym
              AND minute_ts >= @cutoff
              AND last_price IS NOT NULL
          ),
          STRUCT(TRUE AS perform_aggregation, 60 AS horizon)
        )
      """, sym_safe)
      USING sym AS sym, cutoff_ts AS cutoff;
    ELSE
      EXECUTE IMMEDIATE FORMAT("""
        INSERT INTO `big-data-crypto-sentiment-test.analytical_module.eval_metrics_latest`
        SELECT CURRENT_TIMESTAMP() AS run_at, @sym AS symbol, *
        FROM ML.EVALUATE(
          MODEL `big-data-crypto-sentiment-test.analytical_module.arima_price_xreg_%s`,
          (
            SELECT
              minute_ts,
              last_price,
              sentiment_score_mean_lag1,
              sentiment_magnitude_mean_lag1,
              tweet_count_lag1,
              tweet_volume_1m_lag1
            FROM `big-data-crypto-sentiment-test.analytical_module.features_1m`
            WHERE symbol = @sym
              AND minute_ts >= @cutoff
              AND last_price IS NOT NULL
          ),
          STRUCT(TRUE AS perform_aggregation, 60 AS horizon)
        )
      """, sym_safe)
      USING sym AS sym, cutoff_ts AS cutoff;
    END IF;

IF first THEN
  EXECUTE IMMEDIATE FORMAT("""
    CREATE OR REPLACE TABLE `big-data-crypto-sentiment-test.analytical_module.arima_forecast_latest` AS
    SELECT
      CURRENT_TIMESTAMP() AS run_at,
      @sym AS symbol,
      *
    FROM ML.FORECAST(
      MODEL `big-data-crypto-sentiment-test.analytical_module.arima_price_xreg_sym_%s`,
      (
        SELECT
          minute_ts,
          IFNULL(sentiment_score_mean_lag1, 0.0) AS sentiment_score_mean_lag1,
          IFNULL(sentiment_magnitude_mean_lag1, 0.0) AS sentiment_magnitude_mean_lag1,
          IFNULL(tweet_count_lag1, 0) AS tweet_count_lag1,
          IFNULL(tweet_volume_1m_lag1, 0) AS tweet_volume_1m_lag1
        FROM `big-data-crypto-sentiment-test.analytical_module.features_1m`
        WHERE symbol = @sym
          AND minute_ts >= @cutoff
        ORDER BY minute_ts
        LIMIT @h
      ),
      STRUCT(@h AS horizon, @c AS confidence_level)
    )
  """, sym_safe)
  USING sym AS sym, cutoff_ts_sym AS cutoff, horizon_minutes AS h, conf_level AS c;
ELSE
  EXECUTE IMMEDIATE FORMAT("""
    INSERT INTO `big-data-crypto-sentiment-test.analytical_module.arima_forecast_latest`
    SELECT
      CURRENT_TIMESTAMP() AS run_at,
      @sym AS symbol,
      *
    FROM ML.FORECAST(
      MODEL `big-data-crypto-sentiment-test.analytical_module.arima_price_xreg_sym_%s`,
      (
        SELECT
          minute_ts,
          IFNULL(sentiment_score_mean_lag1, 0.0) AS sentiment_score_mean_lag1,
          IFNULL(sentiment_magnitude_mean_lag1, 0.0) AS sentiment_magnitude_mean_lag1,
          IFNULL(tweet_count_lag1, 0) AS tweet_count_lag1,
          IFNULL(tweet_volume_1m_lag1, 0) AS tweet_volume_1m_lag1
        FROM `big-data-crypto-sentiment-test.analytical_module.features_1m`
        WHERE symbol = @sym
          AND minute_ts >= @cutoff
        ORDER BY minute_ts
        LIMIT @h
      ),
      STRUCT(@h AS horizon, @c AS confidence_level)
    )
  """, sym_safe)
  USING sym AS sym, cutoff_ts_sym AS cutoff, horizon_minutes AS h, conf_level AS c;
END IF;

IF first THEN
  EXECUTE IMMEDIATE("""
    CREATE OR REPLACE TABLE `big-data-crypto-sentiment-test.analytical_module.eval_metrics_manual_latest` AS
    WITH f AS (
      SELECT
        symbol,
        forecast_timestamp,
        forecast_value
      FROM `big-data-crypto-sentiment-test.analytical_module.arima_forecast_latest`
      WHERE symbol = @sym
    ),
    a AS (
      SELECT
        symbol,
        minute_ts,
        last_price
      FROM `big-data-crypto-sentiment-test.analytical_module.features_1m`
      WHERE symbol = @sym
        AND minute_ts >= @cutoff
    )
    SELECT
      CURRENT_TIMESTAMP() AS run_at,
      @sym AS symbol,
      COUNT(*) AS n_points,
      AVG(ABS(a.last_price - f.forecast_value)) AS mae,
      SQRT(AVG(POW(a.last_price - f.forecast_value, 2))) AS rmse,
      AVG(SAFE_DIVIDE(ABS(a.last_price - f.forecast_value), NULLIF(ABS(a.last_price), 0))) AS mape
    FROM f
    JOIN a
      ON a.minute_ts = f.forecast_timestamp
  """)
  USING sym AS sym, cutoff_ts_sym AS cutoff;
ELSE
  EXECUTE IMMEDIATE("""
    INSERT INTO `big-data-crypto-sentiment-test.analytical_module.eval_metrics_manual_latest`
    WITH f AS (
      SELECT
        symbol,
        forecast_timestamp,
        forecast_value
      FROM `big-data-crypto-sentiment-test.analytical_module.arima_forecast_latest`
      WHERE symbol = @sym
    ),
    a AS (
      SELECT
        symbol,
        minute_ts,
        last_price
      FROM `big-data-crypto-sentiment-test.analytical_module.features_1m`
      WHERE symbol = @sym
        AND minute_ts >= @cutoff
    )
    SELECT
      CURRENT_TIMESTAMP() AS run_at,
      @sym AS symbol,
      COUNT(*) AS n_points,
      AVG(ABS(a.last_price - f.forecast_value)) AS mae,
      SQRT(AVG(POW(a.last_price - f.forecast_value, 2))) AS rmse,
      AVG(SAFE_DIVIDE(ABS(a.last_price - f.forecast_value), NULLIF(ABS(a.last_price), 0))) AS mape
    FROM f
    JOIN a
      ON a.minute_ts = f.forecast_timestamp
  """)
  USING sym AS sym, cutoff_ts_sym AS cutoff;
END IF;

    SET first = FALSE;
  END FOR;

END;

-- Run manually any time:
CALL `big-data-crypto-sentiment-test.analytical_module.run_daily_offline_module`();
