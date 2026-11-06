# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # Saudi Stock Market - Silver Layer
# MAGIC
# MAGIC Bronze assessment for `dbt_saudi` (source: `abfss://bronze@stsaudifm.dfs.core.windows.net/saudi_stock_market`) and the build plan for a production-grade silver layer.

# COMMAND ----------

# DBTITLE 1,Bronze profiling
# Reproducible bronze profiling: structure, completeness and cross-dataset reconciliation
from pyspark.sql import functions as F

BRONZE = "abfss://bronze@stsaudifm.dfs.core.windows.net/saudi_stock_market"
read = lambda n: spark.read.option("multiLine", True).json(f"{BRONZE}/{n}/")

companies_raw = read("companies")
market_raw    = read("market_summary")
sectors_raw   = read("sector_performance")
prices_raw    = read("stock_prices")

# 1. Pagination completeness of the companies extract
display(companies_raw.select("count", "total", "limit", "offset",
        F.size("results").alias("rows_in_file"),
        (F.col("total") - F.size("results")).alias("missing_rows")))

# 2. Companies: key uniqueness, nulls, domain values
companies = companies_raw.select(F.explode("results").alias("r")).select("r.*")
display(companies.agg(
    F.count("*").alias("rows"),
    F.countDistinct("symbol").alias("distinct_symbols"),
    F.sum(F.when(~F.col("symbol").rlike("^[0-9]{4}$"), 1).otherwise(0)).alias("bad_symbol_format"),
    F.sum(F.when(F.col("market").isNull(), 1).otherwise(0)).alias("null_market")))
display(companies.groupBy("security_type", "market_segment", "market", "status", "is_etf").count())

# 3. Sector snapshot vs market summary reconciliation (detects truncated sector list)
sectors = sectors_raw.select("count", F.explode("sectors").alias("s")).select("count", "s.*")
recon = (sectors.agg(F.count("*").alias("sector_rows"),
                     F.sum("num_stocks").alias("sector_num_stocks"),
                     F.sum("volume").alias("sector_volume"))
         .crossJoin(market_raw.select((F.col("advancing") + F.col("declining") + F.col("unchanged")).alias("market_traded_stocks"),
                                      "total_volume"))
         .withColumn("stocks_gap", F.col("market_traded_stocks") - F.col("sector_num_stocks"))
         .withColumn("volume_gap", F.col("total_volume") - F.col("sector_volume")))
display(recon)

# 4. Price record internal consistency
prices = prices_raw.select("*", "liquidity.*").drop("liquidity")
display(prices.select(
    "symbol", "updated_at", "is_delayed",
    F.round(F.col("price") - F.col("previous_close") - F.col("change"), 4).alias("change_diff"),
    F.round((F.col("change") / F.col("previous_close")) * 100 - F.col("change_percent"), 3).alias("pct_diff"),
    (F.col("inflow_volume") + F.col("outflow_volume") - F.col("volume")).alias("liquidity_volume_diff"),
    (F.col("low") <= F.least("open", "price")).alias("low_ok"),
    (F.col("high") >= F.greatest("open", "price")).alias("high_ok"),
    (F.col("bid") <= F.col("ask")).alias("spread_ok"),
    "net_value"))

# COMMAND ----------

# DBTITLE 1,Assessment and silver build plan
# MAGIC %md
# MAGIC ## Bronze assessment
# MAGIC
# MAGIC | Dataset | Grain | Shape | Key findings |
# MAGIC | --- | --- | --- | --- |
# MAGIC | companies | 1 row per listed security (reference data) | Paginated API envelope `{results[], count, total, limit, offset}` | Only page 1 landed: 100 of 523 securities (offset 0, limit 100). Symbols unique, 4-digit. Mixes Equity (96) and Sukuk (4); Sukuk have `market = null`. No sector attribute. |
# MAGIC | stock_prices | 1 quote per symbol per snapshot | Single JSON object, nested `liquidity` struct, `auction` always null | Only 1 symbol (2222 Aramco). Internally consistent (change, %, OHLC, bid/ask, inflow+outflow = volume). `net_value` carries float noise (17897224.390000004). Delayed feed. |
# MAGIC | sector_performance | 1 row per sector per snapshot | Envelope with `sectors[]` array | 20 sectors returned, all positive, sorted desc. Sum of `num_stocks` = 263 vs 273 traded stocks in market summary; volume short by \~6.9M. Energy and Telecom are missing - the list is truncated (top-N), not complete. |
# MAGIC | market_summary | 1 row per index per snapshot | Single flat object | TASI snapshot at 08:10 UTC (11:10 Riyadh, intraday). `market_mood` is a derived vendor label. |
# MAGIC
# MAGIC Cross-cutting issues:
# MAGIC * Files have a UTF-8 BOM and CRLF, and are single multi-line objects; they must be read with `multiLine` and an explicit schema.
# MAGIC * Each folder holds one file that is overwritten in place (all written 2026-11-04 08:29 UTC). There is no history, no partitioning by date, and no ingestion metadata.
# MAGIC * All files are intraday, delayed snapshots, not end-of-day closes. Event time (`timestamp`, `updated_at`) and load time are different and both are needed.
# MAGIC * Arabic and English names coexist, and the English names in companies are tickers or transliterations (e.g. a Sukuk named "Skwk Msrf Al Rajhy...").
# MAGIC * Bronze is not registered in Unity Catalog. `dbt_saudi` has no tables yet.
# MAGIC
# MAGIC ## Top priority steps for a production-ready silver layer
# MAGIC
# MAGIC 1. **Fix ingestion completeness at the source (blocker).** Paginate companies until `offset + count >= total`, request all sectors (remove the top-N limit) and all symbols for prices. Silver cannot be trusted while it holds 19% of the universe and 1 of \~273 traded quotes.
# MAGIC 2. **Make bronze append-only and replayable.** Write each pull to `.../<dataset>/ingest_date=YYYY-MM-DD/<dataset>_<run_ts>.json` instead of overwriting, then load with Auto Loader into bronze Delta tables registered in `dbt_saudi.bronze`, keeping `_metadata.file_path`, `_ingested_at` and `_rescued_data`.
# MAGIC 3. **Enforce explicit schemas and contracts.** Define a StructType per dataset (no inference), cast prices and amounts to `DECIMAL(18,4)` (this also removes the float noise), cast timestamps to UTC `TIMESTAMP`, and route schema drift to `_rescued_data`.
# MAGIC 4. **Model silver tables at a clear grain.**
# MAGIC    * `dim_security`: SCD2 on `symbol`, holding names, security_type, segment, status, is_etf and sector. Add a curated `symbol -> sector` mapping, because the API has no sector link.
# MAGIC    * `fct_quote_snapshot`: grain is (symbol, `updated_at`), with the `liquidity` struct flattened. Drop `auction` until it's populated.
# MAGIC    * `fct_sector_snapshot`: grain is (sector_name, snapshot_ts). Explode the array.
# MAGIC    * `fct_market_snapshot`: grain is (index, `timestamp`).
# MAGIC    * Add `trade_date` (Asia/Riyadh), `snapshot_ts`, `is_delayed` and `is_eod` to every fact.
# MAGIC 5. **Load incrementally and idempotently.** MERGE on the natural keys above and dedupe repeated polls (same symbol and `updated_at`), so re-runs and backfills don't create duplicates.
# MAGIC 6. **Add data quality expectations, with quarantine.** Use hard rules (fail or quarantine) and soft rules (warn):
# MAGIC    * Hard rules: key not null and unique, 4-digit symbol, `low <= open/price <= high`, `bid <= ask`, `change = price - previous_close` within tolerance, `inflow_volume + outflow_volume = volume`, non-negative volumes.
# MAGIC    * Soft rules: reconciliation checks (companies count = `total`, sector count = 21, sum of sector stocks = advancing + declining + unchanged, sector volume ≈ market volume).
# MAGIC 7. **Conform reference data.** Normalize `market` (Sukuk null to 'SUKUK' or 'DEBT'), trim and standardize names, keep both `name_ar` and `name_en`, and use UTF-8 throughout.
# MAGIC 8. **Govern and operate.** Use managed tables under `dbt_saudi.silver` on the silver external location, with column comments, PK/FK constraints, liquid clustering on (trade_date, symbol), a scheduled Job aligned to Tadawul hours (Sun-Thu, 10:00-15:00 Riyadh), and alerts on DQ failures and freshness (`max(snapshot_ts)` lag).

# COMMAND ----------

# DBTITLE 1,Build section
# MAGIC %md
# MAGIC ## Build: bronze to silver
# MAGIC
# MAGIC The cells below fix the issues that can be fixed inside Databricks.
# MAGIC
# MAGIC **Bronze.** Auto Loader captures every version of every source file into `dbt_saudi.bronze.raw_*`, so history is kept even though the source overwrites files. It strips the BOM and CRLF, and adds the source file and load time to each row.
# MAGIC
# MAGIC **Silver.** The silver load:
# MAGIC * Applies an explicit schema, with DECIMAL money columns and UTC timestamps plus a Riyadh `trade_date`.
# MAGIC * Flattens and explodes nested fields, and conforms reference values.
# MAGIC * Applies row-level DQ rules and quarantines rows that fail.
# MAGIC * Dedupes and loads by MERGE, so a re-run changes nothing.
# MAGIC * Builds an SCD2 security dimension.
# MAGIC * Logs run-level completeness checks to `dq_run_log`.
# MAGIC
# MAGIC It does not fix completeness at the source: pagination, all symbols and all sectors. That must happen in the API extract that writes to the bronze container.

# COMMAND ----------

# DBTITLE 1,Config and infrastructure
# Configuration and bronze infrastructure
import uuid, datetime
from pyspark.sql import functions as F, types as T, Window
from delta.tables import DeltaTable

spark.conf.set("spark.sql.session.timeZone", "UTC")   # store and compute all timestamps in UTC

CATALOG        = "dbt_saudi"
BRONZE_SCHEMA  = f"{CATALOG}.bronze"
SILVER_SCHEMA  = f"{CATALOG}.silver"
SOURCE_ROOT    = "abfss://bronze@stsaudifm.dfs.core.windows.net/saudi_stock_market"
CHECKPOINTS    = f"/Volumes/{CATALOG}/bronze/checkpoints"
DATASETS       = ["companies", "stock_prices", "sector_performance", "market_summary"]
LOCAL_TZ       = "Asia/Riyadh"
EOD_LOCAL_TIME = "15:20"       # after the Tadawul closing auction prices are final
EXPECTED_SECTORS   = 21

# Job parameters (pass as job task parameters; defaults are for interactive runs)
dbutils.widgets.dropdown("full_refresh", "false", ["true", "false"], "Reprocess all bronze history")
dbutils.widgets.dropdown("fail_on_incomplete", "false", ["true", "false"], "Fail the run on incomplete source data")
FULL_REFRESH       = dbutils.widgets.get("full_refresh") == "true"
FAIL_ON_INCOMPLETE = dbutils.widgets.get("fail_on_incomplete") == "true"   # set true once the source extract is fixed
RUN_ID, RUN_TS = str(uuid.uuid4()), datetime.datetime.now(datetime.timezone.utc)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA} COMMENT 'Raw, append-only copies of every Saudi market API payload'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {BRONZE_SCHEMA}.checkpoints COMMENT 'Auto Loader checkpoints for bronze ingestion'")
print(f"run_id={RUN_ID} run_ts={RUN_TS.isoformat()} full_refresh={FULL_REFRESH} fail_on_incomplete={FAIL_ON_INCOMPLETE}")

# COMMAND ----------

# DBTITLE 1,Bronze ingestion with history
# Bronze: one row per file version. allowOverwrites re-ingests a file each time the source overwrites it,
# which builds the history the source folders do not keep.
def ingest_bronze(dataset: str) -> None:
    raw = (spark.readStream.format("cloudFiles")
           .option("cloudFiles.format", "text")
           .option("wholetext", True)                     # payloads are multi-line JSON objects
           .option("cloudFiles.allowOverwrites", True)
           .option("cloudFiles.partitionColumns", "")     # future ingest_date=... folders are not added as columns
           .option("pathGlobFilter", "*.json")
           .load(f"{SOURCE_ROOT}/{dataset}/"))
    bronze = (raw.select(
                  F.regexp_replace("value", r"^\x{FEFF}|\s+$", "").alias("payload"),   # strip UTF-8 BOM and trailing CRLF
                  F.col("_metadata.file_path").alias("_source_file"),
                  F.col("_metadata.file_modification_time").alias("_file_modification_time"),
                  F.col("_metadata.file_size").alias("_file_size"),
                  F.current_timestamp().alias("_ingested_at"))
              .withColumn("_payload_hash", F.sha2("payload", 256)))
    (bronze.writeStream
        .option("checkpointLocation", f"{CHECKPOINTS}/raw_{dataset}")
        .trigger(availableNow=True)
        .toTable(f"{BRONZE_SCHEMA}.raw_{dataset}")
        .awaitTermination())

for ds in DATASETS:
    ingest_bronze(ds)

display(spark.sql(" UNION ALL ".join(
    f"SELECT '{ds}' AS dataset, count(*) AS file_versions, max(_file_modification_time) AS latest_file_time, "
    f"max(_ingested_at) AS last_ingested_at, sum(CASE WHEN payload LIKE '\uFEFF%' THEN 1 ELSE 0 END) AS rows_with_bom "
    f"FROM {BRONZE_SCHEMA}.raw_{ds}" for ds in DATASETS)))

# COMMAND ----------

# DBTITLE 1,Silver table contracts
# MAGIC %sql
# MAGIC -- Silver data contracts: explicit types, NOT NULL keys, informational primary keys, liquid clustering
# MAGIC CREATE TABLE IF NOT EXISTS dbt_saudi.silver.ref_security_sector (
# MAGIC   symbol      STRING NOT NULL COMMENT '4-digit Tadawul symbol',
# MAGIC   sector_name STRING NOT NULL COMMENT 'Sector name, must match fct_sector_snapshot.sector_name',
# MAGIC   source      STRING COMMENT 'Origin of the mapping',
# MAGIC   updated_at  TIMESTAMP,
# MAGIC   CONSTRAINT pk_ref_security_sector PRIMARY KEY (symbol)
# MAGIC ) COMMENT 'Curated symbol-to-sector mapping. The API has no sector on securities, so this is maintained separately.';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS dbt_saudi.silver.dim_security (
# MAGIC   symbol         STRING NOT NULL COMMENT '4-digit Tadawul symbol',
# MAGIC   name_ar        STRING COMMENT 'Arabic name',
# MAGIC   name_en        STRING COMMENT 'English name or ticker as provided by the API',
# MAGIC   market         STRING COMMENT 'Market; Sukuk default to market_segment because the API sends NULL',
# MAGIC   security_type  STRING COMMENT 'Equity, Sukuk, ...',
# MAGIC   market_segment STRING,
# MAGIC   status         STRING,
# MAGIC   is_etf         BOOLEAN,
# MAGIC   sector_name    STRING COMMENT 'From ref_security_sector; NULL until mapped',
# MAGIC   attr_hash      STRING COMMENT 'Hash of tracked attributes, used to detect changes',
# MAGIC   effective_from TIMESTAMP NOT NULL COMMENT 'UTC time the version was first observed',
# MAGIC   effective_to   TIMESTAMP COMMENT 'UTC time the next version was observed; NULL for current',
# MAGIC   is_current     BOOLEAN,
# MAGIC   _source_file   STRING,
# MAGIC   _ingested_at   TIMESTAMP,
# MAGIC   _loaded_at     TIMESTAMP,
# MAGIC   CONSTRAINT pk_dim_security PRIMARY KEY (symbol, effective_from)
# MAGIC ) CLUSTER BY (symbol)
# MAGIC COMMENT 'SCD2 security master rebuilt deterministically from full bronze history';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS dbt_saudi.silver.fct_quote_snapshot (
# MAGIC   symbol STRING NOT NULL, snapshot_ts TIMESTAMP NOT NULL COMMENT 'Quote updated_at, UTC',
# MAGIC   trade_date DATE COMMENT 'Trading date in Asia/Riyadh',
# MAGIC   name_ar STRING, name_en STRING,
# MAGIC   price DECIMAL(18,4), change DECIMAL(18,4), change_percent DECIMAL(9,4),
# MAGIC   open DECIMAL(18,4), high DECIMAL(18,4), low DECIMAL(18,4), previous_close DECIMAL(18,4),
# MAGIC   volume BIGINT, value DECIMAL(22,4) COMMENT 'Traded value, SAR', trade_count BIGINT,
# MAGIC   bid DECIMAL(18,4), ask DECIMAL(18,4), bid_size BIGINT, ask_size BIGINT,
# MAGIC   inflow_value DECIMAL(22,4), inflow_volume BIGINT, inflow_trades BIGINT,
# MAGIC   outflow_value DECIMAL(22,4), outflow_volume BIGINT, outflow_trades BIGINT,
# MAGIC   net_value DECIMAL(22,4) COMMENT 'inflow_value - outflow_value',
# MAGIC   is_delayed BOOLEAN COMMENT 'Vendor feed is delayed', is_eod BOOLEAN COMMENT 'Snapshot taken after the close (final prices)',
# MAGIC   _source_file STRING, _ingested_at TIMESTAMP, _loaded_at TIMESTAMP,
# MAGIC   CONSTRAINT pk_fct_quote_snapshot PRIMARY KEY (symbol, snapshot_ts)
# MAGIC ) CLUSTER BY (trade_date, symbol)
# MAGIC COMMENT 'Intraday quote snapshots, one row per symbol per vendor update';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS dbt_saudi.silver.fct_sector_snapshot (
# MAGIC   index_code STRING NOT NULL, sector_name STRING NOT NULL,
# MAGIC   snapshot_ts TIMESTAMP NOT NULL COMMENT 'Source file time, because the payload has no timestamp',
# MAGIC   trade_date DATE, sector_rank INT COMMENT 'Position in the vendor list (sorted by change desc)',
# MAGIC   sector_name_ar STRING, change_percent DECIMAL(9,4), avg_change_percent DECIMAL(9,4),
# MAGIC   volume BIGINT, num_stocks BIGINT, is_delayed BOOLEAN, is_eod BOOLEAN,
# MAGIC   _source_file STRING, _ingested_at TIMESTAMP, _loaded_at TIMESTAMP,
# MAGIC   CONSTRAINT pk_fct_sector_snapshot PRIMARY KEY (index_code, sector_name, snapshot_ts)
# MAGIC ) CLUSTER BY (trade_date, sector_name)
# MAGIC COMMENT 'Sector performance snapshots';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS dbt_saudi.silver.fct_market_snapshot (
# MAGIC   index_code STRING NOT NULL, snapshot_ts TIMESTAMP NOT NULL, trade_date DATE,
# MAGIC   index_value DECIMAL(18,4), index_change DECIMAL(18,4), index_change_percent DECIMAL(9,4),
# MAGIC   total_volume BIGINT, advancing BIGINT, declining BIGINT, unchanged BIGINT,
# MAGIC   traded_stocks BIGINT COMMENT 'advancing + declining + unchanged',
# MAGIC   market_mood STRING COMMENT 'Vendor-derived label', is_delayed BOOLEAN, is_eod BOOLEAN,
# MAGIC   _source_file STRING, _ingested_at TIMESTAMP, _loaded_at TIMESTAMP,
# MAGIC   CONSTRAINT pk_fct_market_snapshot PRIMARY KEY (index_code, snapshot_ts)
# MAGIC ) CLUSTER BY (trade_date)
# MAGIC COMMENT 'Index-level market snapshots';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS dbt_saudi.silver.dq_quarantine (
# MAGIC   dataset STRING, record STRING COMMENT 'Offending record as JSON', record_hash STRING,
# MAGIC   failed_rules ARRAY<STRING>, _source_file STRING, _ingested_at TIMESTAMP,
# MAGIC   quarantined_at TIMESTAMP, run_id STRING
# MAGIC ) COMMENT 'Rows rejected by row-level data quality rules';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS dbt_saudi.silver.dq_run_log (
# MAGIC   run_id STRING, run_ts TIMESTAMP, dataset STRING, check_name STRING, severity STRING,
# MAGIC   expected DOUBLE, actual DOUBLE, passed BOOLEAN, details STRING
# MAGIC ) COMMENT 'Run-level completeness, reconciliation and freshness checks';

# COMMAND ----------

# DBTITLE 1,Harden tables for gold
# Hardening for gold consumers (idempotent):
# * enforced CHECK constraints as a backstop behind the quarantine rules (a bad row can never land, even from another writer)
# * change data feed, so gold can build incrementally from table_changes()
# * trade_date on the DQ log, so gold can be gated per trading day
CHECK_CONSTRAINTS = {
    "fct_quote_snapshot": {
        "ck_quote_symbol_format":       "symbol RLIKE '^[0-9]{4}$'",
        "ck_quote_price_positive":      "price > 0",
        "ck_quote_volume_non_negative": "volume >= 0 AND trade_count >= 0",
        "ck_quote_low_le_high":         "volume = 0 OR low <= high",
    },
    "fct_market_snapshot": {
        "ck_market_index_positive":       "index_value > 0",
        "ck_market_breadth_non_negative": "advancing >= 0 AND declining >= 0 AND unchanged >= 0",
    },
    "fct_sector_snapshot": {
        "ck_sector_num_stocks_positive": "num_stocks > 0",
        "ck_sector_volume_non_negative": "volume >= 0",
    },
    "dim_security": {
        "ck_dim_valid_interval": "effective_to IS NULL OR effective_to > effective_from",
        "ck_dim_current_flag":   "is_current = (effective_to IS NULL)",
    },
}
for table, constraints in CHECK_CONSTRAINTS.items():
    fq = f"{SILVER_SCHEMA}.{table}"
    existing = {r["key"] for r in spark.sql(f"SHOW TBLPROPERTIES {fq}").collect()}
    for name, expr in constraints.items():
        if f"delta.constraints.{name}" not in existing:
            spark.sql(f"ALTER TABLE {fq} ADD CONSTRAINT {name} CHECK ({expr})")
    spark.sql(f"ALTER TABLE {fq} SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'layer' = 'silver')")

if "trade_date" not in spark.table(f"{SILVER_SCHEMA}.dq_run_log").columns:
    spark.sql(f"ALTER TABLE {SILVER_SCHEMA}.dq_run_log ADD COLUMNS (trade_date DATE COMMENT 'Market trade date the check refers to')")

from functools import reduce
display(reduce(lambda a, b: a.unionByName(b), [
    spark.sql(f"SHOW TBLPROPERTIES {SILVER_SCHEMA}.{t}")
         .filter(F.col("key").startswith("delta.constraints.") | (F.col("key") == "delta.enableChangeDataFeed"))
         .select(F.lit(t).alias("table_name"), "key", "value")
    for t in CHECK_CONSTRAINTS]))

# COMMAND ----------

# DBTITLE 1,Contracts and helpers
# Explicit payload contracts (no inference). Numbers parse straight from JSON text into DECIMAL, so there is no float noise.
PX, AMT, PCT = T.DecimalType(18, 4), T.DecimalType(22, 4), T.DecimalType(9, 4)
S, L, B = T.StringType(), T.LongType(), T.BooleanType()
def struct(*fields): return T.StructType([T.StructField(n, t) for n, t in fields])

COMPANIES_SCHEMA = struct(
    ("results", T.ArrayType(struct(("symbol", S), ("name_ar", S), ("name_en", S), ("market", S), ("status", S),
                                   ("security_type", S), ("market_segment", S), ("is_etf", B)))),
    ("count", L), ("total", L), ("limit", L), ("offset", L))
QUOTE_SCHEMA = struct(
    ("symbol", S), ("name", S), ("name_en", S), ("price", PX), ("change", PX), ("change_percent", PCT),
    ("open", PX), ("high", PX), ("low", PX), ("previous_close", PX), ("volume", L), ("value", AMT),
    ("trade_count", L), ("bid", PX), ("ask", PX), ("bid_size", L), ("ask_size", L),
    ("liquidity", struct(("inflow_value", AMT), ("inflow_volume", L), ("inflow_trades", L),
                         ("outflow_value", AMT), ("outflow_volume", L), ("outflow_trades", L), ("net_value", AMT))),
    ("auction", S), ("updated_at", S), ("is_delayed", B))
SECTOR_SCHEMA = struct(
    ("index", S), ("count", L), ("is_delayed", B),
    ("sectors", T.ArrayType(struct(("sector_name", S), ("sector_name_ar", S), ("change_percent", PCT),
                                   ("avg_change_percent", PCT), ("volume", L), ("num_stocks", L)))))
MARKET_SCHEMA = struct(
    ("index", S), ("is_delayed", B), ("timestamp", S), ("index_value", PX), ("index_change", PX),
    ("index_change_percent", PCT), ("total_volume", L), ("advancing", L), ("declining", L),
    ("unchanged", L), ("market_mood", S))

META = ["_source_file", "_file_modification_time", "_ingested_at"]
LOADED_AT = F.current_timestamp()

TABLE_COLUMNS = {}
def table_columns(table):
    """Target column list, resolved once per table (avoids repeated schema RPCs)."""
    if table not in TABLE_COLUMNS:
        TABLE_COLUMNS[table] = spark.table(table).columns
    return TABLE_COLUMNS[table]

def watermark(table):
    """Latest bronze _ingested_at already in the target, or None for a full refresh / empty table."""
    return None if FULL_REFRESH else spark.table(table).agg(F.max("_ingested_at")).first()[0]

def parse_records(dataset, schema, since=None):
    """Parse a payload that is a single object or an array of objects. Unparseable payloads give rec = NULL.
    When since is set, only bronze rows ingested after it are processed (incremental load)."""
    as_array = F.when(F.col("payload").startswith("["), F.col("payload")).otherwise(F.concat(F.lit("["), F.col("payload"), F.lit("]")))
    src = spark.table(f"{BRONZE_SCHEMA}.raw_{dataset}")
    if since is not None:
        src = src.filter(F.col("_ingested_at") > F.lit(since))
    return src.select("payload", F.explode_outer(F.from_json(as_array, T.ArrayType(schema))).alias("rec"), *META)

def with_time_attributes(df, ts_col):
    local = F.from_utc_timestamp(F.col(ts_col), LOCAL_TZ)
    return df.withColumns({"trade_date": F.to_date(local),
                           "is_eod": F.date_format(local, "HH:mm") >= F.lit(EOD_LOCAL_TIME)})

def apply_rules(df, rules):
    """Adds failed_rules (array of rule names). A rule that evaluates to NULL counts as failed."""
    checks = [F.when(~F.coalesce(F.expr(expr), F.lit(False)), F.lit(name)) for name, expr in rules.items()]
    return df.withColumn("failed_rules", F.filter(F.array(*checks), lambda x: x.isNotNull()))

def latest_per_key(df, keys):
    w = Window.partitionBy(*keys).orderBy(F.col("_ingested_at").desc(), F.col("_file_modification_time").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

def quarantine(dataset, bad):
    q = (bad.select(F.lit(dataset).alias("dataset"),
                    F.coalesce(F.to_json("rec"), F.col("payload")).alias("record"),
                    "failed_rules", "_source_file", "_ingested_at")
            .withColumns({"record_hash": F.sha2(F.concat_ws("|", "dataset", "record", "_source_file",
                                                            F.col("_ingested_at").cast("string")), 256),
                          "quarantined_at": LOADED_AT,
                          "run_id": F.lit(RUN_ID)}))
    (DeltaTable.forName(spark, f"{SILVER_SCHEMA}.dq_quarantine").alias("t")
        .merge(q.alias("s"), "t.record_hash = s.record_hash").whenNotMatchedInsertAll().execute())

def load_fact(dataset, df, rules, keys, table):
    """Validate, quarantine failures, dedupe on the natural key, MERGE (re-runs are no-ops)."""
    checked = apply_rules(df, rules)
    quarantine(dataset, checked.filter(F.size("failed_rules") > 0))
    final = (latest_per_key(checked.filter(F.size("failed_rules") == 0), keys)
             .withColumn("_loaded_at", LOADED_AT)
             .select(*table_columns(table)))
    cond = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    (DeltaTable.forName(spark, table).alias("t").merge(final.alias("s"), cond)
        .whenMatchedUpdateAll(condition="s._ingested_at > t._ingested_at")
        .whenNotMatchedInsertAll().execute())

# COMMAND ----------

# DBTITLE 1,Load silver facts
# Quotes: flatten liquidity, drop the always-null auction field, derive trade_date / is_eod
q = parse_records("stock_prices", QUOTE_SCHEMA, since=watermark(f"{SILVER_SCHEMA}.fct_quote_snapshot"))
quotes = with_time_attributes(q.select(
    "payload", "rec", *META,
    F.trim("rec.symbol").alias("symbol"),
    F.to_timestamp("rec.updated_at").alias("snapshot_ts"),
    F.trim("rec.name").alias("name_ar"), F.trim("rec.name_en").alias("name_en"),
    *[F.col(f"rec.{c}").alias(c) for c in ["price", "change", "change_percent", "open", "high", "low", "previous_close",
                                          "volume", "value", "trade_count", "bid", "ask", "bid_size", "ask_size", "is_delayed"]],
    "rec.liquidity.*"), "snapshot_ts")
QUOTE_RULES = {
    "symbol_format":         "symbol RLIKE '^[0-9]{4}$'",
    "snapshot_ts_not_null":  "snapshot_ts IS NOT NULL",
    "price_positive":        "price > 0",
    "low_le_high":           "volume = 0 OR low <= high",
    "price_within_range":    "volume = 0 OR price BETWEEN low AND high",
    "open_within_range":     "volume = 0 OR `open` IS NULL OR `open` BETWEEN low AND high",
    "bid_le_ask":            "bid IS NULL OR ask IS NULL OR bid = 0 OR ask = 0 OR bid <= ask",
    "non_negative_volume":   "volume >= 0 AND trade_count >= 0",
    "change_reconciles":     "abs(price - previous_close - `change`) <= 0.01",
    "liquidity_reconciles":  "inflow_volume IS NULL OR inflow_volume + outflow_volume = volume",
}
load_fact("stock_prices", quotes, QUOTE_RULES, ["symbol", "snapshot_ts"], f"{SILVER_SCHEMA}.fct_quote_snapshot")

# Market summary
m = parse_records("market_summary", MARKET_SCHEMA, since=watermark(f"{SILVER_SCHEMA}.fct_market_snapshot"))
market = with_time_attributes(m.select(
    "payload", "rec", *META,
    F.upper(F.trim("rec.index")).alias("index_code"),
    F.to_timestamp("rec.timestamp").alias("snapshot_ts"),
    "rec.index_value", "rec.index_change", "rec.index_change_percent", "rec.total_volume",
    "rec.advancing", "rec.declining", "rec.unchanged",
    (F.col("rec.advancing") + F.col("rec.declining") + F.col("rec.unchanged")).alias("traded_stocks"),
    F.trim("rec.market_mood").alias("market_mood"), "rec.is_delayed"), "snapshot_ts")
MARKET_RULES = {
    "index_code_not_null":   "index_code IS NOT NULL",
    "snapshot_ts_not_null":  "snapshot_ts IS NOT NULL",
    "index_value_positive":  "index_value > 0",
    "breadth_non_negative":  "advancing >= 0 AND declining >= 0 AND unchanged >= 0",
    "volume_non_negative":   "total_volume >= 0",
    "change_pct_reconciles": "abs(index_change / (index_value - index_change) * 100 - index_change_percent) <= 0.01",
}
load_fact("market_summary", market, MARKET_RULES, ["index_code", "snapshot_ts"], f"{SILVER_SCHEMA}.fct_market_snapshot")

# Sectors: explode the array and keep the vendor position. The payload has no timestamp, so the file time is the snapshot time.
s = parse_records("sector_performance", SECTOR_SCHEMA, since=watermark(f"{SILVER_SCHEMA}.fct_sector_snapshot"))
sectors = with_time_attributes(
    s.select("payload", *META, F.col("rec.index").alias("_index"), F.col("rec.is_delayed").alias("is_delayed"),
             F.posexplode_outer("rec.sectors").alias("pos", "rec"))
     .select("payload", "rec", *META, "is_delayed",
             F.upper(F.trim("_index")).alias("index_code"),
             F.trim("rec.sector_name").alias("sector_name"),
             F.col("_file_modification_time").alias("snapshot_ts"),
             (F.col("pos") + 1).cast("int").alias("sector_rank"),
             F.trim("rec.sector_name_ar").alias("sector_name_ar"),
             "rec.change_percent", "rec.avg_change_percent", "rec.volume", "rec.num_stocks"), "snapshot_ts")
SECTOR_RULES = {
    "index_code_not_null":  "index_code IS NOT NULL",
    "sector_name_not_null": "sector_name IS NOT NULL",
    "num_stocks_positive":  "num_stocks > 0",
    "volume_non_negative":  "volume >= 0",
}
load_fact("sector_performance", sectors, SECTOR_RULES, ["index_code", "sector_name", "snapshot_ts"], f"{SILVER_SCHEMA}.fct_sector_snapshot")
print("facts loaded")

# COMMAND ----------

# DBTITLE 1,Seed curated sector mapping
# Curated symbol -> sector seed (the API has no sector on securities).
# Only mappings validated against the feed are seeded: the 10 banks below equal sector_performance 'Banks' num_stocks = 10.
# Extend this list (or replace it with a reference feed) to cover the full market; non-seed rows are never overwritten.
SECTOR_SEED = {
    "Banks":  ["1010", "1020", "1030", "1050", "1060", "1080", "1120", "1140", "1150", "1180"],
    "Energy": ["2222"],
}
SEED_SOURCE = "seed: Tadawul sector classification, validated against sector_performance.num_stocks"
seed = spark.createDataFrame([(s, sector) for sector, syms in SECTOR_SEED.items() for s in syms], "symbol string, sector_name string") \
            .withColumns({"source": F.lit(SEED_SOURCE), "updated_at": F.current_timestamp()})
(DeltaTable.forName(spark, f"{SILVER_SCHEMA}.ref_security_sector").alias("t")
    .merge(seed.alias("s"), "t.symbol = s.symbol")
    .whenMatchedUpdateAll(condition="t.source LIKE 'seed:%' AND t.sector_name <> s.sector_name")
    .whenNotMatchedInsertAll()
    .execute())

# Validate the seed against the vendor sector feed (latest snapshot)
display(spark.sql(f"""
  SELECT r.sector_name, count(*) AS mapped_symbols, max(s.num_stocks) AS vendor_num_stocks,
         count(*) = max(s.num_stocks) AS matches_vendor
  FROM {SILVER_SCHEMA}.ref_security_sector r
  LEFT JOIN {SILVER_SCHEMA}.fct_sector_snapshot s
    ON s.sector_name = r.sector_name
   AND s.snapshot_ts = (SELECT max(snapshot_ts) FROM {SILVER_SCHEMA}.fct_sector_snapshot)
  GROUP BY r.sector_name"""))

# COMMAND ----------

# DBTITLE 1,Build SCD2 security dimension
# dim_security: conform reference data, validate, then derive SCD2 versions from the full bronze history and MERGE them in
# (deterministic and idempotent: new versions are inserted, and closed versions get effective_to / is_current updated)
c = parse_records("companies", COMPANIES_SCHEMA)   # always full history: SCD2 needs every snapshot (reference data is small)
companies = (c.select("payload", *META, F.col("rec.total").alias("api_total"), F.explode_outer("rec.results").alias("rec"))
              .select("payload", "rec", *META, "api_total",
                      F.trim("rec.symbol").alias("symbol"),
                      F.trim("rec.name_ar").alias("name_ar"),
                      F.regexp_replace(F.trim("rec.name_en"), r"\s+", " ").alias("name_en"),
                      F.upper(F.trim(F.coalesce("rec.market", "rec.market_segment"))).alias("market"),  # Sukuk arrive with market = NULL
                      F.trim("rec.security_type").alias("security_type"),
                      F.upper(F.trim("rec.market_segment")).alias("market_segment"),
                      F.lower(F.trim("rec.status")).alias("status"),
                      F.col("rec.is_etf").alias("is_etf"),
                      F.col("_file_modification_time").alias("snapshot_ts")))
COMPANY_RULES = {
    "symbol_format":          "symbol RLIKE '^[0-9]{4}$'",
    "name_present":           "coalesce(name_en, name_ar) IS NOT NULL",
    "security_type_not_null": "security_type IS NOT NULL",
    "status_not_null":        "status IS NOT NULL",
}
checked = apply_rules(companies, COMPANY_RULES)
quarantine("companies", checked.filter(F.size("failed_rules") > 0))

ATTRS = ["name_ar", "name_en", "market", "security_type", "market_segment", "status", "is_etf", "sector_name"]
by_symbol = Window.partitionBy("symbol").orderBy("snapshot_ts")
DIM = f"{SILVER_SCHEMA}.dim_security"
versions = (latest_per_key(checked.filter(F.size("failed_rules") == 0), ["symbol", "snapshot_ts"])
            .join(spark.table(f"{SILVER_SCHEMA}.ref_security_sector").select("symbol", "sector_name"), "symbol", "left")
            .withColumn("attr_hash", F.sha2(F.concat_ws("|", *[F.coalesce(F.col(a).cast("string"), F.lit("~")) for a in ATTRS]), 256))
            .withColumn("prev_hash", F.lag("attr_hash").over(by_symbol))
            .filter(F.col("prev_hash").isNull() | (F.col("prev_hash") != F.col("attr_hash")))   # keep change points only
            .withColumn("effective_from", F.col("snapshot_ts"))
            .withColumn("effective_to", F.lead("snapshot_ts").over(by_symbol))
            .withColumn("is_current", F.col("effective_to").isNull())
            .withColumn("_loaded_at", LOADED_AT))
(DeltaTable.forName(spark, DIM).alias("t")
    .merge(versions.select(*table_columns(DIM)).alias("s"),
           "t.symbol = s.symbol AND t.effective_from = s.effective_from")
    .whenMatchedUpdateAll(condition="t.attr_hash <> s.attr_hash OR NOT (t.effective_to <=> s.effective_to) OR t.is_current <> s.is_current")
    .whenNotMatchedInsertAll()
    .execute())
print("dim_security merged")

# COMMAND ----------

# DBTITLE 1,Run-level completeness checks
# Run-level checks: completeness, reconciliation and freshness, logged to dq_run_log
checks = []
def check(dataset, name, severity, expected, actual, passed, details=""):
    checks.append((RUN_ID, RUN_TS, dataset, name, severity,
                   None if expected is None else float(expected), None if actual is None else float(actual),
                   bool(passed), details, TRADE_DATE))

mkt = spark.table(f"{SILVER_SCHEMA}.fct_market_snapshot").orderBy(F.desc("snapshot_ts")).first()
TRADE_DATE = mkt["trade_date"]   # every check in this run is attributed to the latest market trade date
sec = spark.table(f"{SILVER_SCHEMA}.fct_sector_snapshot")
sec_latest = (sec.filter(F.col("snapshot_ts") == sec.agg(F.max("snapshot_ts")).first()[0])
                 .agg(F.count("*").alias("n"), F.sum("num_stocks").alias("stocks"), F.sum("volume").alias("vol")).first())
comp_latest = (companies.filter(F.col("api_total").isNotNull()).orderBy(F.desc("_file_modification_time"))
               .select("api_total", F.to_date(F.from_utc_timestamp("snapshot_ts", LOCAL_TZ)).alias("d")).first())
comp_loaded = (companies.filter(F.to_date(F.from_utc_timestamp("snapshot_ts", LOCAL_TZ)) == F.lit(comp_latest["d"]))
               .select("symbol").distinct().count())
quote_symbols = (spark.table(f"{SILVER_SCHEMA}.fct_quote_snapshot")
                 .filter(F.col("trade_date") == F.lit(mkt["trade_date"])).select("symbol").distinct().count())
hours_since_market = spark.sql(f"SELECT (unix_timestamp(current_timestamp()) - unix_timestamp(max(snapshot_ts))) / 3600 "
                               f"FROM {SILVER_SCHEMA}.fct_market_snapshot").first()[0]

check("companies", "companies_complete", "error", comp_latest["api_total"], comp_loaded,
      comp_loaded >= comp_latest["api_total"], "Distinct symbols for the latest date vs API total (pagination)")
check("sector_performance", "sector_count", "error", EXPECTED_SECTORS, sec_latest["n"], sec_latest["n"] == EXPECTED_SECTORS,
      "Sectors in the latest snapshot")
check("sector_performance", "sector_stocks_reconcile", "error", mkt["traded_stocks"], sec_latest["stocks"],
      sec_latest["stocks"] == mkt["traded_stocks"], "Sum of sector num_stocks vs advancing + declining + unchanged")
check("sector_performance", "sector_volume_reconcile", "warn", mkt["total_volume"], sec_latest["vol"],
      sec_latest["vol"] >= 0.99 * mkt["total_volume"], "Sum of sector volume vs market total volume (>= 99%)")
check("stock_prices", "quote_coverage", "error", mkt["traded_stocks"], quote_symbols,
      quote_symbols >= mkt["traded_stocks"], "Symbols quoted on the latest trade_date vs traded stocks")
check("market_summary", "freshness_hours", "warn", 24, hours_since_market, hours_since_market <= 24,
      "Hours since the latest market snapshot")
# Integrity checks gold relies on: unique natural keys, one current dim row per symbol, no orphan facts
def scalar(sql): return spark.sql(sql).first()[0]
for table, keys in {"fct_quote_snapshot": "symbol, snapshot_ts", "fct_market_snapshot": "index_code, snapshot_ts",
                    "fct_sector_snapshot": "index_code, sector_name, snapshot_ts", "dim_security": "symbol, effective_from"}.items():
    dups = scalar(f"SELECT count(*) - count(DISTINCT {keys}) FROM {SILVER_SCHEMA}.{table}")
    check(table, "pk_unique", "error", 0, dups, dups == 0, f"Duplicate natural keys ({keys})")
multi_current = scalar(f"SELECT count(*) FROM (SELECT symbol FROM {SILVER_SCHEMA}.dim_security WHERE is_current "
                       f"GROUP BY symbol HAVING count(*) > 1)")
check("dim_security", "one_current_version", "error", 0, multi_current, multi_current == 0, "Symbols with more than one current version")
orphans = scalar(f"SELECT count(DISTINCT q.symbol) FROM {SILVER_SCHEMA}.fct_quote_snapshot q "
                 f"LEFT ANTI JOIN {SILVER_SCHEMA}.dim_security d ON q.symbol = d.symbol")
check("stock_prices", "quote_symbols_in_dim", "error", 0, orphans, orphans == 0, "Quoted symbols missing from dim_security")
current_n, mapped_n = spark.sql(f"SELECT count(*), count(sector_name) FROM {SILVER_SCHEMA}.dim_security WHERE is_current").first()
check("dim_security", "sector_mapping_coverage", "warn", current_n, mapped_n, mapped_n == current_n,
      "Current securities with a sector (needed for sector rollups in gold)")

for ds, n in {r["dataset"]: r["n"] for r in spark.table(f"{SILVER_SCHEMA}.dq_quarantine").filter(F.col("run_id") == RUN_ID)
              .groupBy("dataset").agg(F.count("*").alias("n")).collect()}.items():
    check(ds, "new_quarantined_rows", "warn", 0, n, n == 0, "Rows rejected by row-level rules in this run")

log = spark.createDataFrame(checks, "run_id string, run_ts timestamp, dataset string, check_name string, severity string, "
                                    "expected double, actual double, passed boolean, details string, trade_date date")
log.write.mode("append").saveAsTable(f"{SILVER_SCHEMA}.dq_run_log")
display(log.select("trade_date", "dataset", "check_name", "severity", "expected", "actual", "passed", "details"))

failed_errors = [c[3] for c in checks if c[4] == "error" and not c[7]]
if failed_errors and FAIL_ON_INCOMPLETE:
    raise RuntimeError(f"Silver load is incomplete, failed checks: {failed_errors}")
elif failed_errors:
    print(f"WARNING: incomplete source data, failed checks: {failed_errors} (FAIL_ON_INCOMPLETE=False)")

# COMMAND ----------

# DBTITLE 1,Validate silver output
# Validation: row counts, decimal precision, and conformed reference values
display(spark.sql(" UNION ALL ".join(
    f"SELECT '{t}' AS table_name, count(*) AS rows FROM {SILVER_SCHEMA}.{t}"
    for t in ["dim_security", "fct_quote_snapshot", "fct_sector_snapshot", "fct_market_snapshot", "dq_quarantine", "ref_security_sector"])))
display(spark.table(f"{SILVER_SCHEMA}.fct_quote_snapshot")
        .select("symbol", "snapshot_ts", "trade_date", "is_eod", "price", "change", "value", "inflow_value", "outflow_value",
                "net_value", (F.col("inflow_value") - F.col("outflow_value") - F.col("net_value")).alias("net_value_diff")))
display(spark.table(f"{SILVER_SCHEMA}.dim_security").filter("is_current")
        .groupBy("security_type", "market", "market_segment").agg(F.count("*").alias("securities"),
                                                                   F.count("sector_name").alias("with_sector")))

# COMMAND ----------

# DBTITLE 1,Gold contract views
# MAGIC %sql
# MAGIC -- Gold reads silver only through these views. v_trade_date_status is the publish gate:
# MAGIC -- gold must build only trade dates where is_gold_ready = true.
# MAGIC CREATE OR REPLACE VIEW dbt_saudi.silver.v_security_current
# MAGIC COMMENT 'Current version of every security (SCD2 is_current rows)'
# MAGIC AS SELECT symbol, name_ar, name_en, market, security_type, market_segment, status, is_etf, sector_name,
# MAGIC           effective_from AS valid_since
# MAGIC    FROM dbt_saudi.silver.dim_security
# MAGIC    WHERE is_current;
# MAGIC
# MAGIC CREATE OR REPLACE VIEW dbt_saudi.silver.v_quote_latest
# MAGIC COMMENT 'Latest quote per symbol per trade_date. is_eod = true means the closing (final) price.'
# MAGIC AS SELECT symbol, trade_date, snapshot_ts, is_eod, is_delayed, name_ar, name_en,
# MAGIC           price, `change`, change_percent, `open`, high, low, previous_close, volume, `value`, trade_count,
# MAGIC           bid, ask, bid_size, ask_size, inflow_value, inflow_volume, inflow_trades,
# MAGIC           outflow_value, outflow_volume, outflow_trades, net_value
# MAGIC    FROM dbt_saudi.silver.fct_quote_snapshot
# MAGIC    QUALIFY row_number() OVER (PARTITION BY symbol, trade_date ORDER BY snapshot_ts DESC) = 1;
# MAGIC
# MAGIC CREATE OR REPLACE VIEW dbt_saudi.silver.v_trade_date_status
# MAGIC COMMENT 'Publish gate: per trade_date result of the latest silver run. Gold builds only is_gold_ready dates.'
# MAGIC AS WITH latest_run AS (
# MAGIC      SELECT trade_date, max_by(run_id, run_ts) AS run_id
# MAGIC      FROM dbt_saudi.silver.dq_run_log
# MAGIC      WHERE trade_date IS NOT NULL
# MAGIC      GROUP BY trade_date)
# MAGIC    SELECT l.trade_date, l.run_id, max(d.run_ts) AS checked_at,
# MAGIC           count_if(d.severity = 'error' AND NOT d.passed) AS failed_error_checks,
# MAGIC           count_if(d.severity = 'warn'  AND NOT d.passed) AS failed_warn_checks,
# MAGIC           array_sort(collect_list(CASE WHEN NOT d.passed THEN d.check_name END)) AS failed_checks,
# MAGIC           count_if(d.severity = 'error' AND NOT d.passed) = 0 AS is_gold_ready
# MAGIC    FROM latest_run l
# MAGIC    JOIN dbt_saudi.silver.dq_run_log d ON d.run_id = l.run_id AND d.trade_date = l.trade_date
# MAGIC    GROUP BY l.trade_date, l.run_id;
# MAGIC
# MAGIC SELECT * FROM dbt_saudi.silver.v_trade_date_status ORDER BY trade_date DESC;

# COMMAND ----------

# DBTITLE 1,Gold readiness verdict
# MAGIC %md
# MAGIC ## Gold readiness (2026-11-05)
# MAGIC
# MAGIC **Verdict: the engineering is gold-ready; the data is not.** `v_trade_date_status` returns `is_gold_ready = false` for 2026-11-04. The gate stays closed until the source extract is fixed.
# MAGIC
# MAGIC | Area | Status |
# MAGIC | --- | --- |
# MAGIC | Contracts (explicit schema, DECIMAL, UTC + Riyadh trade date) | Ready |
# MAGIC | Enforced CHECK constraints plus quarantine | Ready (12 constraints on 4 tables) |
# MAGIC | Idempotent, incremental MERGE (watermark on `_ingested_at`, `full_refresh` widget) | Ready; a re-run left all counts unchanged |
# MAGIC | Integrity (unique keys, one current dim row, no orphan quotes) | Passing |
# MAGIC | Change data feed for incremental gold | Enabled; change data is recorded from this run onward |
# MAGIC | Gold interface | `v_security_current`, `v_quote_latest`, `v_trade_date_status` |
# MAGIC | Source completeness | **Blocked**: 100/523 companies, 1/273 quotes, 20/21 sectors |
# MAGIC | Sector mapping | **Blocked for sector rollups**: `ref_security_sector` is empty |
# MAGIC
# MAGIC Rules for gold:
# MAGIC * Read only the `v_*` views, never the fact tables directly.
# MAGIC * Build only trade dates where `v_trade_date_status.is_gold_ready`.
# MAGIC * Use `is_eod = true` rows for daily closes.
# MAGIC
# MAGIC Once the source is fixed:
# MAGIC 1. Run the job with `fail_on_incomplete = true`.
# MAGIC 2. Schedule it for Tadawul hours (Sun-Thu).
# MAGIC 3. Add alerts on `v_trade_date_status` and on freshness.