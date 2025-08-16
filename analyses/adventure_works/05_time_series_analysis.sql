/*
Question 5: Qual o número de pedidos, quantidade comprada, valor total negociado por mês e ano 
(dica: gráfico de série de tempo)?

This analysis provides time series data for creating time-based charts and analyzing 
sales trends over time.
*/

with
    sales_facts as (
        select * from {{ ref('fact_sales') }}
    )

    , monthly_metrics as (
        select 
            -- Time dimensions
            order_year
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
            , concat(order_year, '-', lpad(cast(order_month as string), 2, '0')) as year_month
            -- Metrics
            , count(distinct sales_order_id) as number_of_orders
            , sum(order_qty) as total_quantity_purchased
            , sum(gross_amount) as total_gross_amount
            , sum(net_amount) as total_net_amount
            , sum(total_discount_amount) as total_discount_amount
            , avg(gross_amount) as average_order_value
            , count(distinct customer_id) as unique_customers
            , count(distinct product_id) as unique_products
        from sales_facts
        group by 
            order_year
            , order_month
    )

    , yearly_metrics as (
        select 
            order_year
            , 'Year Total' as month_name
            , concat(order_year, '-Total') as year_month
            , sum(number_of_orders) as number_of_orders
            , sum(total_quantity_purchased) as total_quantity_purchased
            , sum(total_gross_amount) as total_gross_amount
            , sum(total_net_amount) as total_net_amount
            , sum(total_discount_amount) as total_discount_amount
            , avg(average_order_value) as average_order_value
            , count(distinct unique_customers) as unique_customers
            , count(distinct unique_products) as unique_products
        from monthly_metrics
        group by order_year
    )

    , combined_metrics as (
        select * from monthly_metrics
        union all
        select * from yearly_metrics
    )

    , final_metrics as (
        select 
            order_year
            , order_month
            , month_name
            , year_month
            , number_of_orders
            , total_quantity_purchased
            , total_gross_amount
            , total_net_amount
            , total_discount_amount
            , average_order_value
            , unique_customers
            , unique_products
            -- Growth metrics for trend analysis
            , lag(number_of_orders) over (order by year_month) as prev_month_orders
            , lag(total_gross_amount) over (order by year_month) as prev_month_gross
            , case 
                when lag(number_of_orders) over (order by year_month) > 0 
                then ((number_of_orders - lag(number_of_orders) over (order by year_month)) / 
                      lag(number_of_orders) over (order by year_month)) * 100
                else null
              end as order_growth_pct
            , case 
                when lag(total_gross_amount) over (order by year_month) > 0 
                then ((total_gross_amount - lag(total_gross_amount) over (order by year_month)) / 
                      lag(total_gross_amount) over (order by year_month)) * 100
                else null
              end as revenue_growth_pct
        from combined_metrics
    )

    select 
        order_year
        , order_month
        , month_name
        , year_month
        , number_of_orders
        , total_quantity_purchased
        , total_gross_amount
        , total_net_amount
        , total_discount_amount
        , average_order_value
        , unique_customers
        , unique_products
        , prev_month_orders
        , prev_month_gross
        , order_growth_pct
        , revenue_growth_pct
        , round(total_gross_amount, 2) as total_gross_amount_formatted
        , round(total_net_amount, 2) as total_net_amount_formatted
        , round(total_discount_amount, 2) as total_discount_amount_formatted
        , round(average_order_value, 2) as average_order_value_formatted
        , round(order_growth_pct, 2) as order_growth_pct_formatted
        , round(revenue_growth_pct, 2) as revenue_growth_pct_formatted
    from final_metrics
    order by 
        order_year desc
        , case when month_name = 'Year Total' then 13 else order_month end
