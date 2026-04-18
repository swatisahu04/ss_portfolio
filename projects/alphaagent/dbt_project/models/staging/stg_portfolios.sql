{{ config(tags=['staging', 'reference']) }}

select
    portfolio_id,
    portfolio_name,
    strategy,
    base_currency,
    inception_date,
    portfolio_manager,
    aum_usd as initial_aum_usd
from {{ source('raw', 'portfolios') }}
