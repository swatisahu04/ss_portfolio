{{ config(tags=['intermediate', 'benchmarks']) }}

select
    benchmark_id,
    price_date,
    daily_return
from {{ ref('stg_benchmarks_daily') }}
