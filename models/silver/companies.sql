-- One row per listed security (latest version). Merge key: symbol.
{{ config(unique_key='symbol') }}

with src as (
    select
        from_json(
            payload,
            'results ARRAY<STRUCT<symbol: STRING, name_ar: STRING, name_en: STRING, market: STRING, status: STRING,
             security_type: STRING, market_segment: STRING, is_etf: BOOLEAN>>, count BIGINT, total BIGINT'
        ) as p,
        _source_file,
        _ingested_at
    from {{ source('bronze', 'raw_companies') }}
    {% if is_incremental() %}
    where _ingested_at > (select coalesce(max(_ingested_at), timestamp'1900-01-01') from {{ this }})
    {% endif %}
),

parsed as (
    select
        trim(r.symbol)                                      as symbol,
        regexp_replace(trim(r.name_ar), '\\s+', ' ')        as name_ar,
        regexp_replace(trim(r.name_en), '\\s+', ' ')        as name_en,
        upper(trim(coalesce(r.market, r.market_segment)))   as market,          -- Sukuk rows have market = null
        upper(trim(r.market_segment))                       as market_segment,
        trim(r.security_type)                               as security_type,
        lower(trim(r.status))                               as status,
        r.is_etf,
        api_total,                                          -- vendor total (523); used by the completeness test
        _source_file,
        _ingested_at
    from (select p.total as api_total, explode(p.results) as r, _source_file, _ingested_at from src)
    where r.symbol is not null
    qualify row_number() over (partition by trim(r.symbol) order by _ingested_at desc) = 1
)

select
    c.*,
    m.sector_name
from parsed c
left join {{ ref('symbol_sector_map') }} m
    on c.symbol = m.symbol
