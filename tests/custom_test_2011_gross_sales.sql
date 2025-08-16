/*
Custom test to validate Carlos's requirement:
"vendas brutas no ano de 2011 foram de $12.646.112,16"

This test ensures that the total gross sales for 2011 matches the expected value.
*/

with
    sales_facts as (
        select * from {{ ref('fact_sales') }}
    )

    , sales_2011 as (
        select 
            round(sum(gross_amount), 2) as total_gross_sales_2011
        from sales_facts
        where order_year = 2011
    )

select *
from sales_2011
where total_gross_sales_2011 != 12646112.16
