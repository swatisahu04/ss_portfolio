{{ config(tags=['marts', 'trades']) }}

/*
  Daily trade activity summary — grain: (portfolio_id, trade_date).
  Useful for turnover analysis and trader commentary.
*/
with trades as (
    select * from {{ ref('stg_trades') }}
),
nav as (
    select portfolio_id, position_date as trade_date, portfolio_nav_usd
    from {{ ref('int_portfolio_daily_totals') }}
)

select
    t.portfolio_id,
    t.trade_date,
    count(*)                                            as num_trades,
    count(*) filter (where side = 'BUY')                as num_buys,
    count(*) filter (where side = 'SELL')               as num_sells,
    sum(gross_amount)                                   as total_gross_usd,
    sum(fees)                                           as total_fees_usd,
    sum(case when side = 'BUY' then gross_amount else 0 end)  as buy_notional_usd,
    sum(case when side = 'SELL' then gross_amount else 0 end) as sell_notional_usd,
    -- One-sided turnover: min(buys, sells) / NAV
    least(
        sum(case when side = 'BUY' then gross_amount else 0 end),
        sum(case when side = 'SELL' then gross_amount else 0 end)
    ) / nullif(max(nav.portfolio_nav_usd), 0)           as turnover_ratio
from trades t
left join nav using (portfolio_id, trade_date)
group by t.portfolio_id, t.trade_date
