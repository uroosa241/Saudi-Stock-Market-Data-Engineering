-- Per trade_date, the sum of sector num_stocks must equal advancing + declining + unchanged.
-- Returns failing days. Warns until the sector feed returns all 21 sectors (set fail_on_incomplete: true).
{{ config(severity = 'error' if var('fail_on_incomplete') else 'warn') }}

with sectors as (
    select trade_date, sum(num_stocks) as sector_stocks
    from {{ ref('sector_performance_daily') }}
    group by trade_date
),

market as (
    select trade_date, advancing + declining + unchanged as market_stocks
    from {{ ref('market_summary_daily') }}
    where index_code = 'TASI'
)

select
    coalesce(s.trade_date, m.trade_date) as trade_date,
    s.sector_stocks,
    m.market_stocks,
    m.market_stocks - s.sector_stocks    as difference
from sectors s
full outer join market m
    on s.trade_date = m.trade_date
where s.sector_stocks is null
   or m.market_stocks is null
   or s.sector_stocks <> m.market_stocks
