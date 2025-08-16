with
    source as (
        select * from {{ source('adventure_works', 'salesorderheader') }}
    )

    , renamed as (
        select 
            cast(salesorderid as string) as sales_order_id
            , cast(customerid as string) as customer_id
            , cast(salespersonid as string) as salesperson_id
            , cast(territoryid as string) as territory_id
            , cast(creditcardid as string) as credit_card_id
            , cast(orderdate as date) as order_date
            , cast(duedate as date) as due_date
            , cast(shipdate as date) as ship_date
            , cast(status as integer) as order_status
            , cast(subtotal as float) as subtotal
            , cast(taxamt as float) as tax_amount
            , cast(freight as float) as freight
            , cast(totaldue as float) as total_due
            , cast(onlineorderflag as boolean) as is_online_order
        from source
    )

    select * 
    from renamed
