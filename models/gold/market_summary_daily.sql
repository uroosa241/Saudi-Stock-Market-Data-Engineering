-- Market summary by trade_date: the latest intraday TASI snapshot per Riyadh trading day,
-- plus the intraday range across all snapshots of that day.
-- Values are delayed intraday snapshots, not end-of-day closes.
with snapshots as (
    select
        *,
        min(index_value) over (partition by trade_date, index) as intraday_low,
        max(index_value) over (partition by trade_date, index) as intraday_high,
        min(timestamp)   over (partition by trade_date, index) as first_snapshot_ts,
        count(*)         over (partition by trade_date, index) as snapshots_in_day
    from {{ ref('market_snapshots') }}
    qualify row_number() over (partition by trade_date, index order by timestamp desc) = 1
)

select
    trade_date,
    index                                                               as index_code,
    timestamp                                                           as as_of_ts,
    index_value,
    index_change,
    index_change_percent,
    intraday_low,
    intraday_high,
    total_volume,
    advancing,
    declining,
    unchanged,
    traded_stocks,
    cast(advancing / nullif(declining, 0) as decimal(9,4))              as advance_decline_ratio,
    market_mood,
    first_snapshot_ts,
    snapshots_in_day,
    snapshot_type
from snapshots
