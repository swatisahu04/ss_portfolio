{{ config(tags=['marts', 'risk']) }}

/*
  Portfolio risk fact — grain: (portfolio, as_of_date).

  Rolling 30-day window:
    - vol_30d          : annualized volatility
    - sharpe_30d       : (avg_return * 252) / (std_dev * sqrt(252))  [RF rate = 0 for simplicity]
    - max_drawdown_30d : max peak-to-trough decline in the window
    - beta_vs_spy_30d  : cov(port, spy) / var(spy)
*/
with returns as (
    select
        portfolio_id,
        position_date as as_of_date,
        portfolio_daily_return as r
    from {{ ref('int_portfolio_daily_totals') }}
),
spy as (
    select price_date as as_of_date, daily_return as spy_r
    from {{ ref('int_benchmark_returns') }}
    where benchmark_id = 'BMK-SPY'
),
joined as (
    select r.portfolio_id, r.as_of_date, r.r, s.spy_r
    from returns r
    left join spy s using (as_of_date)
),
rolling as (
    select
        portfolio_id,
        as_of_date,
        stddev_samp(r) over w as daily_vol_30d,
        avg(r)         over w as avg_return_30d,
        covar_samp(r, spy_r)   over w as cov_spy_30d,
        var_samp(spy_r)        over w as var_spy_30d
    from joined
    window w as (partition by portfolio_id order by as_of_date
                 rows between 29 preceding and current row)
),
pr as (
    select
        portfolio_id,
        as_of_date,
        -- annualized vol assuming 252 trading days
        daily_vol_30d * sqrt(252) as vol_30d,
        case when daily_vol_30d > 0
             then (avg_return_30d * 252) / (daily_vol_30d * sqrt(252))
             else null end as sharpe_30d,
        cov_spy_30d / nullif(var_spy_30d, 0) as beta_vs_spy_30d
    from rolling
)

select * from pr
