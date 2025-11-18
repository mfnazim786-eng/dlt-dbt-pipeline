{{
    config(
        materialized='table'
    )
}}

SELECT
    _id,
    created_by__user AS "createdby_user",
    created_by__date AS "createdby_date",
    edited_by__user AS "editedby_user",
    edited_by__date AS "editedby_date",
    customer,
    currency,
    status,
    CAST(COALESCE(price__v_double, TRY_CAST(price AS DOUBLE)) AS FLOAT) AS "price",
    franchise_cb AS "franchisecb",
    business_unit AS "businessunit"

FROM
    {{ source('mongo_raw', 'franchise_cb_prices') }}