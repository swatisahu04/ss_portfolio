{{ config(tags=['staging', 'trades']) }}

-- Union batch and stream; the stream is deduped at consumer.
-- Batch has intentional dupes — we dedupe here using a window.
with batch as (
    select
        trade_id,
        portfolio_id,
        security_id,
        trade_date,
        side,
        quantity,
        price,
        gross_amount,
        fees,
        trader,
        'batch'::text as source_system,
        row_number() over (partition by trade_id order by trade_date) as rn
    from {{ source('raw', 'trades') }}
),

stream as (
    select
        trade_id,
        portfolio_id,
        security_id,
        trade_date,
        side,
        quantity,
        price,
        gross_amount,
        fees,
        trader,
        'stream'::text as source_system,
        1 as rn
    from {{ source('raw', 'trades_stream') }}
),

combined as (
    select * from batch where rn = 1
    union all
    select * from stream
    where not exists (
        select 1 from batch where batch.trade_id = stream.trade_id and batch.rn = 1
    )
)

select
    trade_id,
    portfolio_id,
    security_id,
    trade_date,
    side,
    quantity,
    price,
    gross_amount,
    fees,
    trader,
    source_system
from combined
