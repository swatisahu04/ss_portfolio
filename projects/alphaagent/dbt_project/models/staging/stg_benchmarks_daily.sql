{{ config(tags=['staging', 'benchmarks']) }}

with src as (
    select * from {{ source('raw', 'benchmarks_daily') }}
)
select
    benchmark_id,
    price_date,
    close,
    lag(close) over (partition by benchmark_id order by price_date) as prev_close,
    (close / nullif(lag(close) over (partition by benchmark_id order by price_date), 0)) - 1
        as daily_return
from src
