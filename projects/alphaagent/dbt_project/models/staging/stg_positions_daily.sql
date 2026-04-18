{{ config(tags=['staging', 'positions']) }}

select
    portfolio_id,
    security_id,
    position_date,
    quantity
from {{ source('raw', 'positions_daily') }}
