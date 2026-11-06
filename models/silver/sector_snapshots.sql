-- Intraday delayed sector snapshots (NOT daily closes). Merge key: (sector_name, snapshot_ts).
-- The payload has no timestamp, so the bronze file write time is the snapshot time.
{{ config(unique_key=['sector_name', 'snapshot_ts']) }}

with src as (
    select
        from_json(
            payload,
            'index STRING, count BIGINT, is_delayed BOOLEAN,
             sectors ARRAY<STRUCT<sector_name: STRING, sector_name_ar: STRING, change_percent: DECIMAL(18,4),
                                  avg_change_percent: DECIMAL(18,4), volume: BIGINT, num_stocks: BIGINT>>'
        ) as p,
        _file_modification_time,
        _source_file,
        _ingested_at
    from {{ source('bronze', 'raw_sector_performance') }}
    {% if is_incremental() %}
    where _ingested_at > (select coalesce(max(_ingested_at), timestamp'1900-01-01') from {{ this }})
    {% endif %}
),

exploded as (
    select
        upper(trim(p.index))        as index_code,
        p.is_delayed,
        _file_modification_time     as snapshot_ts,
        posexplode(p.sectors)       as (pos, s),
        _source_file,
        _ingested_at
    from src
)

select
    regexp_replace(trim(s.sector_name), '\\s+', ' ')                    as sector_name,
    snapshot_ts,                                                        -- UTC
    to_date(from_utc_timestamp(snapshot_ts, '{{ var("local_tz") }}'))   as trade_date,  -- Riyadh
    index_code,
    trim(s.sector_name_ar)                                              as sector_name_ar,
    cast(pos + 1 as int)                                                as sector_rank,  -- vendor order
    s.change_percent,
    s.avg_change_percent,
    s.volume,
    s.num_stocks,
    is_delayed,
    'intraday_delayed_snapshot'                                         as snapshot_type,
    _source_file,
    _ingested_at
from exploded
where s.sector_name is not null
qualify row_number() over (
    partition by regexp_replace(trim(s.sector_name), '\\s+', ' '), snapshot_ts
    order by _ingested_at desc
) = 1
