-- mart_product_performance.sql
-- Which products are performing best?
-- Answers: top products by revenue, units sold, and conversion rate

with all_events as (
    select * from {{ ref('stg_ecommerce_events') }}
),

product_funnel as (
    select
        product_id,
        product_name,
        category,
        unit_price,

        -- Funnel counts
        countif(event_type = 'page_view')    as page_views,
        countif(event_type = 'add_to_cart')  as add_to_carts,
        countif(event_type = 'purchase')     as purchases,
        countif(event_type = 'return')       as returns,

        -- Revenue
        round(sum(case when event_type = 'purchase'
                       then revenue else 0 end), 2) as total_revenue,
        sum(case when event_type = 'purchase'
                 then quantity else 0 end)           as total_units_sold,

        -- Conversion rate: what % of page views led to a purchase?
        round(
            safe_divide(
                countif(event_type = 'purchase'),
                nullif(countif(event_type = 'page_view'), 0)
            ) * 100, 2
        ) as conversion_rate_pct,

        -- Return rate: what % of purchases were returned?
        round(
            safe_divide(
                countif(event_type = 'return'),
                nullif(countif(event_type = 'purchase'), 0)
            ) * 100, 2
        ) as return_rate_pct

    from all_events
    group by product_id, product_name, category, unit_price
)

select * from product_funnel
order by total_revenue desc