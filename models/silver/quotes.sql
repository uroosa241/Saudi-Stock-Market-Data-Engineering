-- Intraday delayed quote snapshots (NOT daily closes). Merge key: (symbol, updated_at).
{{ config(unique_key=['symbol', 'updated_at']) }}

with src as (
    select
        from_json(
            payload,
            'symbol STRING, name STRING, name_en STRING,
             price DECIMAL(18,4), change DECIMAL(18,4), change_percent DECIMAL(18,4),
             open DECIMAL(18,4), high DECIMAL(18,4), low DECIMAL(18,4), previous_close DECIMAL(18,4),
             volume BIGINT, value DECIMAL(18,4), trade_count BIGINT,
             bid DECIMAL(18,4), ask DECIMAL(18,4), bid_size BIGINT, ask_size BIGINT,
             liquidity STRUCT<inflow_value: DECIMAL(18,4), inflow_volume: BIGINT, inflow_trades: BIGINT,
                              outflow_value: DECIMAL(18,4), outflow_volume: BIGINT, outflow_trades: BIGINT,
                              net_value: DECIMAL(18,4)>,
             auction STRING, updated_at STRING, is_delayed BOOLEAN'
        ) as r,
        _source_file,
        _ingested_at
    from {{ source('bronze', 'raw_stock_prices') }}
    {% if is_incremental() %}
    where _ingested_at > (select coalesce(max(_ingested_at), timestamp'1900-01-01') from {{ this }})
    {% endif %}
),

parsed as (
    select
        trim(r.symbol)                                                              as symbol,
        to_timestamp(r.updated_at)                                                  as updated_at,   -- UTC
        to_date(from_utc_timestamp(to_timestamp(r.updated_at), '{{ var("local_tz") }}')) as trade_date,  -- Riyadh
        regexp_replace(trim(r.name), '\\s+', ' ')                                   as name_ar,
        regexp_replace(trim(r.name_en), '\\s+', ' ')                                as name_en,
        r.price, r.change, r.change_percent,
        r.open, r.high, r.low, r.previous_close,
        r.volume, r.value, r.trade_count,
        r.bid, r.ask, r.bid_size, r.ask_size,
        -- liquidity flattened; auction dropped (always null)
        r.liquidity.inflow_value, r.liquidity.inflow_volume, r.liquidity.inflow_trades,
        r.liquidity.outflow_value, r.liquidity.outflow_volume, r.liquidity.outflow_trades,
        r.liquidity.net_value,
        r.is_delayed,
        'intraday_delayed_snapshot'                                                 as snapshot_type,
        _source_file,
        _ingested_at
    from src
    where r.symbol is not null and r.updated_at is not null
    qualify row_number() over (partition by trim(r.symbol), to_timestamp(r.updated_at) order by _ingested_at desc) = 1
)

select
    q.*,
    m.sector_name
from parsed q
left join {{ ref('symbol_sector_map') }} m
    on q.symbol = m.symbol
