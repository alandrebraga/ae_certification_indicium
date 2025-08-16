/*
Question 2: Quais os produtos com maior ticket médio por mês, ano, cidade, estado e país? 
(ticket médio = Faturamento bruto - descontos do produto / número de pedidos no período de análise)

This analysis calculates the average ticket (gross revenue - discounts / number of orders) 
by product across different time and location dimensions.
*/

with
    sales_facts as (
        select * from {{ ref('fact_sales') }}
    )

    , average_ticket_calculation as (
        select 
            -- Product dimensions
            product_name
            , product_category_name
            , product_subcategory_name
            -- Time dimensions
            , order_year
            , order_month
            , case 
                when order_month = 1 then 'January'
                when order_month = 2 then 'February'
                when order_month = 3 then 'March'
                when order_month = 4 then 'April'
                when order_month = 5 then 'May'
                when order_month = 6 then 'June'
                when order_month = 7 then 'July'
                when order_month = 8 then 'August'
                when order_month = 9 then 'September'
                when order_month = 10 then 'October'
                when order_month = 11 then 'November'
                when order_month = 12 then 'December'
                else 'Unknown'
              end as month_name
            -- Location dimensions
            , state
            , country
            -- Metrics for average ticket calculation
            , count(distinct sales_order_id) as number_of_orders
            , sum(gross_amount) as total_gross_revenue
            , sum(total_discount_amount) as total_discounts
            , sum(net_amount) as total_net_revenue
            -- Average ticket calculation: (Gross Revenue - Discounts) / Number of Orders
            , case 
                when count(distinct sales_order_id) > 0 
                then (sum(gross_amount) - sum(total_discount_amount)) / count(distinct sales_order_id)
                else 0
              end as average_ticket
        from sales_facts
        group by 
            product_name
            , product_category_name
            , product_subcategory_name
            , order_year
            , order_month
            , state
            , country
    )

    , ranked_products as (
        select 
            *
            , row_number() over (
                partition by order_year, order_month, state, country 
                order by average_ticket desc
            ) as rank_by_ticket
        from average_ticket_calculation
        where average_ticket > 0  -- Filter out products with no sales
    )

    select 
        product_name
        , product_category_name
        , product_subcategory_name
        , order_year
        , order_month
        , month_name
        , state
        , country
        , number_of_orders
        , total_gross_revenue
        , total_discounts
        , total_net_revenue
        , average_ticket
        , rank_by_ticket
        , round(total_gross_revenue, 2) as total_gross_revenue_formatted
        , round(total_discounts, 2) as total_discounts_formatted
        , round(total_net_revenue, 2) as total_net_revenue_formatted
        , round(average_ticket, 2) as average_ticket_formatted
    from ranked_products
    order by 
        order_year desc
        , order_month
        , state
        , country
        , average_ticket desc
