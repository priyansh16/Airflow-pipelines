-- mart_revenue_daily.sql
-- Daily revenue summary — the primary dashboard table
-- Answers: how much did we sell today, by country and device?

with purchases as (
    select *
    from {{ ref('stg_ecommerce_events') }}
    where event_type = 'purchase'
),

daily_summary as (
    select
        event_date,
        country,
        device_type,

        -- Volume metrics
        count(distinct user_id)    as unique_buyers,
        count(distinct event_id)   as total_orders,
        sum(quantity)              as total_units_sold,

        -- Revenue metrics
        round(sum(revenue), 2)     as total_revenue,
        round(avg(revenue), 2)     as avg_order_value,
        round(max(revenue), 2)     as max_order_value,

        -- Session metrics
        count(distinct session_id) as total_sessions

    from purchases
    group by event_date, country, device_type
)

select * from daily_summary
order by event_date desc, total_revenue desc