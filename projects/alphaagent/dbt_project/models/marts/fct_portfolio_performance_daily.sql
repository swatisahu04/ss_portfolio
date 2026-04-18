{{ config(
    tags=['marts', 'performance'],
    indexes=[
      {'columns': ['portfolio_id', 'as_of_date'], 'unique': true},
      {'columns': ['as_of_date']}
    ]
) }}

/*
  Portfolio performance fact — grain: one row per (portfolio, as_of_date).

  Columns:
    daily_return          — value-weighted return that day
    mtd_return            — compounded month-to-date
    ytd_return            — compounded year-to-date
    spy_excess_return_ytd — portfolio_ytd - spy_ytd
    agg_excess_return_ytd — portfolio_ytd - agg_ytd
*/
with portfolio_returns as (
    select
        portfolio_id,
        position_date as as_of_date,
        portfolio_nav_usd,
        portfolio_daily_return,
        -- MTD: compound daily returns since first of month
        exp(sum(ln(1 + coalesce(portfolio_daily_return, 0)))
            over (partition by portfolio_id, date_trunc('month', position_date)
                  order by position_date rows between unbounded preceding and current row)) - 1
            as mtd_return,
        -- YTD: compound since first of year
        exp(sum(ln(1 + coalesce(portfolio_daily_return, 0)))
            over (partition by portfolio_id, date_trunc('year', position_date)
                  order by position_date rows between unbounded preceding and current row)) - 1
            as ytd_return
    from {{ ref('int_portfolio_daily_totals') }}
),

spy as (
    select
        price_date as as_of_date,
        exp(sum(ln(1 + coalesce(daily_return, 0)))
            over (partition by date_trunc('year', price_date)
                  order by price_date rows between unbounded preceding and current row)) - 1
            as spy_ytd_return
    from {{ ref('int_benchmark_returns') }}
    where benchmark_id = 'BMK-SPY'
),

agg as (
    select
        price_date as as_of_date,
        exp(sum(ln(1 + coalesce(daily_return, 0)))
            over (partition by date_trunc('year', price_date)
                  order by price_date rows between unbounded preceding and current row)) - 1
            as agg_ytd_return
    from {{ ref('int_benchmark_returns') }}
    where benchmark_id = 'BMK-AGG'
),

port_meta as (
    select portfolio_id, portfolio_name, strategy, portfolio_manager
    from {{ ref('stg_portfolios') }}
)

select
    pr.portfolio_id,
    pm.portfolio_name,
    pm.strategy,
    pm.portfolio_manager,
    pr.as_of_date,
    pr.portfolio_nav_usd,
    pr.portfolio_daily_return as daily_return,
    pr.mtd_return,
    pr.ytd_return,
    spy.spy_ytd_return,
    agg.agg_ytd_return,
    (pr.ytd_return - spy.spy_ytd_return) as spy_excess_return_ytd,
    (pr.ytd_return - agg.agg_ytd_return) as agg_excess_return_ytd
from portfolio_returns pr
join port_meta pm using (portfolio_id)
left join spy using (as_of_date)
left join agg using (as_of_date)
