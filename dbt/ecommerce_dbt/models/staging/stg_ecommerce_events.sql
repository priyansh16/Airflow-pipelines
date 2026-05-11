-- stg_ecommerce_events.sql
-- Staging model: clean and cast raw e-commerce events
-- This is the first transformation layer — raw data becomes typed and clean

with source as (
    select * from {{ source('raw', 'ecommerce_events') }}
),

cleaned as (
    select
        -- Identifiers
        event_id,
        user_id,
        session_id,
        product_id,

        -- Event details
        event_type,
        product_name,
        category,

        -- Cast numeric fields explicitly
        cast(unit_price  as float64) as unit_price,
        cast(quantity    as int64)   as quantity,
        cast(revenue     as float64) as revenue,

        -- Geography and device
        upper(trim(country))     as country,
        lower(trim(device_type)) as device_type,

        -- Timestamps
        timestamp(created_at)       as created_at,
        date(timestamp(created_at)) as event_date,

        -- Audit column
        current_timestamp() as dbt_loaded_at

    from source

    -- Remove any rows with null event IDs (shouldn't happen but defensive)
    where event_id is not null

      -- Only keep known event types
      and event_type in ('page_view', 'add_to_cart', 'purchase', 'return')

      -- Remove future-dated events (data quality issue)
      and timestamp(created_at) <= current_timestamp()
)

select * from cleaned