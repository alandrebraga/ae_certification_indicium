with
    source as (
        select * from {{ source('adventure_works', 'salesorderdetail') }}
    )

    , renamed as (
        select 
            cast(salesorderid as string) as sales_order_id
            , cast(salesorderdetailid as string) as sales_order_detail_id
            , cast(productid as string) as product_id
            , cast(orderqty as integer) as order_qty
            , cast(unitprice as float) as unit_price
            , cast(unitpricediscount as float) as unit_price_discount
            , coalesce(unitprice * (1.0 - coalesce(unitpricediscount, 0.0)) * orderqty, 0.0) as line_total
        from source
    )

    select * 
    from renamed
