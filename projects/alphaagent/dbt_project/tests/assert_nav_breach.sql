-- Custom DQ test: flag portfolio-days where sum of position market values
-- diverges from stored NAV by > 5 bps. The generator intentionally inserts
-- one such row — this test MUST fail on dirty data and pass on clean data.
-- For this project we mark it as `warn` severity to surface, not block.
{{ config(severity='warn') }}

with pos_sum as (
    select
        portfolio_id,
        position_date as as_of_date,
        sum(market_value_usd) as nav_from_positions
    from {{ ref('int_position_valuations') }}
    group by portfolio_id, position_date
),
stated as (
    select portfolio_id, as_of_date, portfolio_nav_usd as stated_nav
    from {{ ref('fct_portfolio_performance_daily') }}
)

select
    p.portfolio_id,
    p.as_of_date,
    p.nav_from_positions,
    s.stated_nav,
    abs(p.nav_from_positions - s.stated_nav) / nullif(s.stated_nav, 0) as breach_bps
from pos_sum p
join stated s using (portfolio_id, as_of_date)
where abs(p.nav_from_positions - s.stated_nav) / nullif(s.stated_nav, 0) > 0.0005
