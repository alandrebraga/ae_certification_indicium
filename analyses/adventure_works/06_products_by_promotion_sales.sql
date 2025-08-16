/*
Question 6: Qual produto tem a maior quantidade de unidades compradas para o motivo de venda "Promotion"?

This analysis identifies products with the highest quantity purchased specifically 
for the "Promotion" sales reason.
*/

with
    sales_facts as (
        select * from {{ ref('fact_sales') }}
    )

    , promotion_sales as (
        select 
            -- Product dimensions
            product_id
            , product_name
            , product_category_name
            , product_subcategory_name
            -- Sales reason
            , sales_reason_name
            -- Metrics
            , count(distinct sales_order_id) as number_of_orders
            , sum(order_qty) as total_quantity_purchased
            , sum(gross_amount) as total_gross_amount
            , sum(net_amount) as total_net_amount
            , avg(order_qty) as average_quantity_per_order
            , avg(gross_amount) as average_order_value
        from sales_facts
        where lower(sales_reason_name) = 'promotion'
        group by 
            product_id
            , product_name
            , product_category_name
            , product_subcategory_name
            , sales_reason_name
    )

    , ranked_products as (
        select 
            *
            , row_number() over (order by total_quantity_purchased desc) as rank_by_quantity
            , row_number() over (order by total_gross_amount desc) as rank_by_revenue
        from promotion_sales
    )

    select 
        product_id
        , product_name
        , product_category_name
        , product_subcategory_name
        , sales_reason_name
        , number_of_orders
        , total_quantity_purchased
        , total_gross_amount
        , total_net_amount
        , average_quantity_per_order
        , average_order_value
        , rank_by_quantity
        , rank_by_revenue
        , round(total_gross_amount, 2) as total_gross_amount_formatted
        , round(total_net_amount, 2) as total_net_amount_formatted
        , round(average_quantity_per_order, 2) as average_quantity_per_order_formatted
        , round(average_order_value, 2) as average_order_value_formatted
    from ranked_products
    order by 
        total_quantity_purchased desc
        , total_gross_amount desc
