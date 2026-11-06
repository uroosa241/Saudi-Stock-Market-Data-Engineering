-- Top gainers and losers by trade_date, from each symbol's latest intraday quote of the day.
-- Values are delayed intraday snapshots, not end-of-day closes.
with latest as (
    select *
    from {{ ref('quotes') }}
    qualify row_number() over (partition by trade_date, symbol order by updated_at desc) = 1
),

ranked as (
    select
        *,
        row_number() over (partition by trade_date order by change_percent desc, value desc) as gain_rank,
        row_number() over (partition by trade_date order by change_percent asc,  value desc) as loss_rank
    from latest
),

movers as (
    select 'gainer' as mover_type, gain_rank as mover_rank, *
    from ranked
    where change_percent > 0 and gain_rank <= {{ var('top_n_movers') }}

    union all

    select 'loser' as mover_type, loss_rank as mover_rank, *
    from ranked
    where change_percent < 0 and loss_rank <= {{ var('top_n_movers') }}
)

select
    trade_date,
    mover_type,
    mover_rank,
    symbol,
    name_en,
    name_ar,
    sector_name,
    price,
    previous_close,
    change,
    change_percent,
    volume,
    value,
    updated_at as as_of_ts,
    snapshot_type
from movers
