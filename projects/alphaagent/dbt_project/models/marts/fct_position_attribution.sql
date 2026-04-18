{{ config(tags=['marts', 'performance', 'attribution']) }}

/*
  Position-level contribution to portfolio return.
  Grain: (portfolio_id, security_id, as_of_date)

  contribution_to_return = (weight at start of period) × security_daily_return
*/
with valuations as (
    select * from {{ ref('int_position_valuations') }}
),
portfolio_totals as (
    select portfolio_id, position_date as as_of_date, portfolio_nav_usd
    from {{ ref('int_portfolio_daily_totals') }}
),
sec as (
    select security_id, ticker, security_name, asset_type, sector, region, is_benchmark
    from {{ ref('stg_securities') }}
)

select
    v.portfolio_id,
    v.security_id,
    sec.ticker,
    sec.sector,
    sec.asset_type,
    v.position_date as as_of_date,
    v.quantity,
    v.market_value_usd,
    v.daily_return as security_daily_return,
    v.market_value_usd / nullif(pt.portfolio_nav_usd, 0) as weight,
    (v.market_value_usd / nullif(pt.portfolio_nav_usd, 0)) * v.daily_return
        as contribution_to_return
from valuations v
join portfolio_totals pt
  on v.portfolio_id = pt.portfolio_id
 and v.position_date = pt.as_of_date
join sec using (security_id)
where sec.is_benchmark = false
