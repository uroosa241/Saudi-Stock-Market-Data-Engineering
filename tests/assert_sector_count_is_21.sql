-- Every sector snapshot must contain all 21 TASI sectors. Returns failing snapshots.
-- Warns until the feed's top-N limit is removed (set fail_on_incomplete: true).
{{ config(severity = 'error' if var('fail_on_incomplete') else 'warn') }}

select
    snapshot_ts,
    count(distinct sector_name) as sector_count
from {{ ref('sector_snapshots') }}
group by snapshot_ts
having count(distinct sector_name) <> {{ var('expected_sector_count') }}
