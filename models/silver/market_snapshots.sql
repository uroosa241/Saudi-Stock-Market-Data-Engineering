-- Intraday delayed market (TASI) snapshots (NOT daily closes). Merge key: (index, timestamp).
{{ config(unique_key=['index', 'timestamp']) }}

with src as (
    select
        from_json(
            payload,
            'index STRING, is_delayed BOOLEAN, timestamp STRING,
             index_value DECIMAL(18,4), index_change DECIMAL(18,4), index_change_percent DECIMAL(18,4),
             total_volume BIGINT, advancing BIGINT, declining BIGINT, unchanged BIGINT, market_mood STRING'
        ) as r,
        _source_file,
        _ingested_at
    from {{ source('bronze', 'raw_market_summary') }}
    {% if is_incremental() %}
    where _ingested_at > (select coalesce(max(_ingested_at), timestamp'1900-01-01') from {{ this }})
    {% endif %}
)

select
    upper(trim(r.index))                                                        as index,
    to_timestamp(r.timestamp)                                                   as timestamp,   -- UTC
    to_date(from_utc_timestamp(to_timestamp(r.timestamp), '{{ var("local_tz") }}')) as trade_date,  -- Riyadh
    r.index_value,
    r.index_change,
    r.index_change_percent,
    r.total_volume,
    r.advancing,
    r.declining,
    r.unchanged,
    r.advancing + r.declining + r.unchanged                                     as traded_stocks,
    trim(r.market_mood)                                                         as market_mood,  -- vendor label
    r.is_delayed,
    'intraday_delayed_snapshot'                                                 as snapshot_type,
    _source_file,
    _ingested_at
from src
where r.index is not null and r.timestamp is not null
qualify row_number() over (
    partition by upper(trim(r.index)), to_timestamp(r.timestamp)
    order by _ingested_at desc
) = 1
