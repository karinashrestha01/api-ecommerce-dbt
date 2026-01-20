SELECT 
    -- Native PostgreSQL Hashing:
    -- We concatenate the ID + dash + sequential number, then hash it.
    MD5(CAST(order_id AS VARCHAR) || '-' || CAST(payment_sequential AS VARCHAR)) as payment_sk,    order_id,
    payment_sequential,
    payment_type,
    payment_value,
    payment_installments
FROM {{ ref('stg_order_payments') }}