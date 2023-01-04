with

customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

payments as (
    select * from {{ ref('stg_payments') }}
    where status <> 'fail'
),

order_payments as (
    select 
        order_id,

        max(created_at) as payment_finalized_date, 
        sum(amount) as total_amount_paid

    from payments
    group by 1
),

paid_orders as (
    select 
        orders.order_id,
        orders.customer_id,
        orders.order_date,
        orders.status as order_status,
        order_payments.total_amount_paid,
        order_payments.payment_finalized_date,

        row_number() over 
            (order by orders.order_id) as transaction_seq,

        row_number() over 
            (partition by orders.customer_id order by orders.order_id) as customer_sales_seq,

        sum(order_payments.total_amount_paid) over 
            (partition by orders.customer_id order by orders.order_id asc) as customer_lifetime_value

    from orders
    inner join order_payments 
        on orders.order_id = order_payments.order_id
),

customer_orders as (
    select 
        customers.customer_id,

        min(orders.order_date) as first_order_date,
        max(orders.order_date) as most_recent_order_date,
        count(orders.order_id) as number_of_orders
    from customers 
    left join orders
        on orders.customer_id = customers.customer_id 
    left join paid_orders
        on customers.customer_id = paid_orders.customer_id
    group by 1
),

final as (
    select
        paid_orders.order_id,
        paid_orders.customer_id,
        paid_orders.transaction_seq,
        paid_orders.customer_sales_seq,
        
        paid_orders.order_status,

        customers.first_name as customer_first_name,
        customers.last_name as customer_last_name,
        
        paid_orders.order_date,
        paid_orders.payment_finalized_date,
        
        paid_orders.total_amount_paid,
        paid_orders.customer_lifetime_value,

        customer_orders.first_order_date,
        case 
            when customer_orders.first_order_date = paid_orders.order_date
            then 'new'
            else 'return' 
        end as new_or_returning

        
    from 
        paid_orders
        left join customer_orders 
            on paid_orders.customer_id = customer_orders.customer_id
        left join customers on paid_orders.customer_id = customers.customer_id 
    order by order_id
),

renamed as (
    select 
        customer_id,
        order_id,
        order_date AS order_placed_at,
        order_status,
        total_amount_paid,
        payment_finalized_date,
        customer_first_name,
        customer_last_name,
        transaction_seq,
        customer_sales_seq,
        new_or_returning as nvsr,
        customer_lifetime_value,
        first_order_date as fdos
    
    from final
)

select * from renamed