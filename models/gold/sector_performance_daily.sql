-- Sector performance by trade_date: the latest intraday snapshot of each sector per Riyadh trading day.
-- Values are delayed intraday snapshots, not end-of-day closes.
with snapshots as (
    select
        *,
        count(*) over (partition by trade_date, sector_name) as snapshots_in_day
    from {{ ref('sector_snapshots') }}
    qualify row_number() over (partition by trade_date, sector_name order by snapshot_ts desc) = 1
)

select
    trade_date,
    sector_name,
    sector_name_ar,
    index_code,
    snapshot_ts                                                                 as as_of_ts,
    change_percent,
    avg_change_percent,
    volume,
    num_stocks,
    rank() over (partition by trade_date order by change_percent desc)          as performance_rank,
    cast(volume * 100.0 / nullif(sum(volume) over (partition by trade_date), 0) as decimal(9,4)) as volume_share_pct,
    snapshots_in_day,
    snapshot_type
from snapshots
