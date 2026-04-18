{{ config(tags=['intermediate', 'performance']) }}

-- Position × price → market value per position-day.
with positions as (
    select * from {{ ref('stg_positions_daily') }}
),
prices as (
    select * from {{ ref('stg_prices_daily') }}
)
select
    p.portfolio_id,
    p.security_id,
    p.position_date,
    p.quantity,
    pr.close,
    pr.daily_return,
    (p.quantity * pr.close)::numeric(24, 4) as market_value_usd
from positions p
left join prices pr
    on p.security_id = pr.security_id
   and p.position_date = pr.price_date
