# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # Saudi Bronze Data Issues
# MAGIC
# MAGIC Short summary of the problems, red flags and corrections for the bronze data in `abfss://bronze@stsaudifm.dfs.core.windows.net/saudi_stock_market`, based on profiling done on 2026-11-05. The supporting checks are in the `silver` notebook.

# COMMAND ----------

# DBTITLE 1,Red flags
# MAGIC %md
# MAGIC ## Red flags (blockers)
# MAGIC
# MAGIC * **Companies are incomplete.** Only API page 1 was loaded: 100 of 523 securities (`offset=0, limit=100, total=523`). 423 securities are missing, about 81% of the universe.
# MAGIC * **Stock prices hold one symbol.** The file has a single quote (2222 Aramco), while the market summary shows 273 traded stocks.
# MAGIC * **The sector list is truncated.** 20 sectors came back, all with positive change, sorted in descending order. Their stock count (263) is 10 short of the market summary's 273, and their volume is short by 6,947,893. Energy and Telecom are missing, so the feed is returning a top-N list, not the full set.
# MAGIC * **No history.** Each folder has one file that is overwritten in place, so every load erases the previous one. Backfill, audit and time-series analysis are impossible.

# COMMAND ----------

# DBTITLE 1,Data issues
# MAGIC %md
# MAGIC ## Data issues found
# MAGIC
# MAGIC | Dataset | Issue | Impact |
# MAGIC | --- | --- | --- |
# MAGIC | All | UTF-8 BOM, CRLF line endings, single multi-line JSON object per file | Default JSON reads fail or produce corrupt records |
# MAGIC | All | Schema is inferred at read time; no contract | Silent type changes when the API changes |
# MAGIC | All | No ingestion metadata (load time, source file, run id) | No lineage, and you can't tell when data arrived |
# MAGIC | All | Intraday, delayed snapshots (`is_delayed = true`, captured 11:10 Riyadh time), not end-of-day | These must not be reported as daily closes |
# MAGIC | All | Timestamps are ISO strings with an offset (`+00:00`) | They have to be cast to TIMESTAMP and given a Riyadh `trade_date` |
# MAGIC | companies | Sukuk rows have `market = null` (4 of 100) | Breaks filters and group-bys on market |
# MAGIC | companies | No sector attribute | Stocks can't be linked to sectors |
# MAGIC | companies | English names are tickers or transliterations | Poor display names |
# MAGIC | stock_prices | Prices and amounts are DOUBLE; `net_value` = 17897224.390000004 | Float rounding noise in financial values |
# MAGIC | stock_prices | `auction` is always null | A dead column |
# MAGIC | stock_prices | `liquidity` is a nested struct | Needs flattening |
# MAGIC | sector_performance | The `count` field (20) is the returned count, not the true total | A misleading completeness signal |
# MAGIC | market_summary | `market_mood` is a vendor-derived label | A business definition we don't control |
# MAGIC
# MAGIC The internal arithmetic of the data we do have is correct:
# MAGIC * `change = price - previous_close`, and `change_percent` matches.
# MAGIC * high/low bound open and price, and bid is at or below ask.
# MAGIC * Inflow volume plus outflow volume equals total volume.
# MAGIC * Company symbols are unique and all 4 digits.

# COMMAND ----------

# DBTITLE 1,Corrections
# MAGIC %md
# MAGIC ## What to correct
# MAGIC
# MAGIC At the source (fix these before building silver):
# MAGIC 1. Page through companies until `offset + count >= total` (all 523).
# MAGIC 2. Pull prices for every symbol, not only 2222.
# MAGIC 3. Remove the top-N limit on sectors, so all 21 TASI sectors come back, including Energy and Telecom.
# MAGIC 4. Stop overwriting. Write each pull to `<dataset>/ingest_date=YYYY-MM-DD/<dataset>_<run_ts>.json`.
# MAGIC
# MAGIC In the bronze and silver load:
# MAGIC
# MAGIC 5. Read with `multiLine`, an explicit schema and `_rescued_data`, and add `_ingested_at` and `_source_file`.
# MAGIC 6. Cast prices and amounts to DECIMAL(18,4), and timestamps to TIMESTAMP (UTC), and derive `trade_date` in Asia/Riyadh.
# MAGIC 7. Flatten `liquidity`, drop `auction`, and explode the `results` and `sectors` arrays.
# MAGIC 8. Fill the Sukuk null `market` from `market_segment`, and trim and standardize names.
# MAGIC 9. Add a curated symbol-to-sector mapping.
# MAGIC 10. Dedupe on the natural keys and load with MERGE:
# MAGIC     * quotes: (symbol, updated_at)
# MAGIC     * sectors: (sector_name, snapshot_ts)
# MAGIC     * market: (index, timestamp)
# MAGIC 11. Add DQ gates. Fail the load when companies loaded is less than `total`, sector count is not 21, or the sum of sector `num_stocks` doesn't equal advancing + declining + unchanged. Quarantine rows that break price-sanity rules.

# COMMAND ----------

# DBTITLE 1,Fix status
# MAGIC %md
# MAGIC ## Fix status (2026-11-05)
# MAGIC
# MAGIC The fixes are implemented and run in the `silver` notebook.
# MAGIC
# MAGIC | Issue | Status | Where |
# MAGIC | --- | --- | --- |
# MAGIC | BOM, CRLF, multi-line JSON | Fixed | Bronze Auto Loader strips them (0 BOM rows remain) |
# MAGIC | No history (files overwritten) | Fixed on the Databricks side | `dbt_saudi.bronze.raw_*` keeps every file version (`allowOverwrites`) |
# MAGIC | No ingestion metadata | Fixed | `_source_file`, `_file_modification_time`, `_ingested_at`, `_payload_hash` |
# MAGIC | Inferred schema | Fixed | Explicit StructType contracts; unparseable payloads go to quarantine |
# MAGIC | Float noise in money | Fixed | DECIMAL parsed from the JSON text; `net_value` = 17897224.3900, and `inflow - outflow - net` = 0 |
# MAGIC | Timestamps as strings, no trade date | Fixed | UTC TIMESTAMP, `trade_date` (Asia/Riyadh), `is_eod` |
# MAGIC | Nested `liquidity`, dead `auction` | Fixed | Flattened and dropped |
# MAGIC | Sukuk `market = null` | Fixed | Defaults to `market_segment` (SUKUK) |
# MAGIC | No sector on securities | Structure ready | `silver.ref_security_sector` is created but empty and needs a mapping source |
# MAGIC | Duplicates and re-runs | Fixed | MERGE on natural keys; a re-run left all row counts unchanged |
# MAGIC | Row-level DQ | Fixed | Rules plus `silver.dq_quarantine` (0 rows rejected) |
# MAGIC | Incomplete companies, prices, sectors | **Detected, not fixable here** | `silver.dq_run_log` flags it: 100/523 companies, 1/273 quotes, 20/21 sectors |
# MAGIC
# MAGIC Open items:
# MAGIC * Fix the API extract that writes to `bronze@stsaudifm`: paginate companies, fetch all symbols and all sectors, and write timestamped files. Then set `FAIL_ON_INCOMPLETE = True`.
# MAGIC * Populate `ref_security_sector`.
# MAGIC * The silver schema uses the metastore default storage, not the `silver@stsaudifm` container.