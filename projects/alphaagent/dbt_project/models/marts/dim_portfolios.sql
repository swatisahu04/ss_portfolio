{{ config(tags=['marts', 'dimensions']) }}

-- Portfolio dimension: enrich with latest NAV.
with port as (
    select * from {{ ref('stg_portfolios') }}
),
latest as (
    select portfolio_id, max(position_date) as latest_date
    from {{ ref('int_portfolio_daily_totals') }}
    group by portfolio_id
),
latest_nav as (
    select t.portfolio_id, t.portfolio_nav_usd as current_nav_usd
    from {{ ref('int_portfolio_daily_totals') }} t
    join latest l on l.portfolio_id = t.portfolio_id and l.latest_date = t.position_date
)

select
    p.portfolio_id,
    p.portfolio_name,
    p.strategy,
    p.base_currency,
    p.inception_date,
    p.portfolio_manager,
    p.initial_aum_usd,
    ln.current_nav_usd
from port p
left join latest_nav ln using (portfolio_id)
