{{ config(tags=['intermediate', 'performance']) }}

-- Roll up position valuations to one row per portfolio-day.
with valuations as (
    select * from {{ ref('int_position_valuations') }}
)
select
    portfolio_id,
    position_date,
    count(*)                         as position_count,
    sum(market_value_usd)            as portfolio_nav_usd,
    sum(market_value_usd * daily_return)
        / nullif(sum(market_value_usd), 0) as portfolio_daily_return
from valuations
group by portfolio_id, position_date
