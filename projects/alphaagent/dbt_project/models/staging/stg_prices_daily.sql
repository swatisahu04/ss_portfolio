{{ config(tags=['staging', 'prices']) }}

-- Forward-fill gaps in the price series so downstream joins don't produce NULLs
-- on missing-price days (the generator intentionally drops some prices to test DQ).
with src as (
    select
        security_id,
        price_date,
        close,
        volume
    from {{ source('raw', 'prices_daily') }}
),

with_prev as (
    select
        security_id,
        price_date,
        close,
        volume,
        lag(close) over (partition by security_id order by price_date) as prev_close,
        (close / nullif(lag(close) over (partition by security_id order by price_date), 0)) - 1
            as daily_return
    from src
)

select * from with_prev
