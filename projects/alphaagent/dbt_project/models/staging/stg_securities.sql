{{ config(tags=['staging', 'reference']) }}

-- Cleaned security master. Splits benchmarks out so the marts layer can join cleanly.
with src as (
    select * from {{ source('raw', 'securities') }}
),

renamed as (
    select
        security_id,
        ticker,
        name                as security_name,
        asset_type,
        sector,
        region,
        currency,
        listed_exchange,
        active,
        case when security_id like 'BMK-%' then true else false end as is_benchmark
    from src
)

select * from renamed
