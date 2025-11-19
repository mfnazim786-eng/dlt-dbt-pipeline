{{
    config(
        materialized='table'
    )
}}

SELECT
    _id,
    created_by__user    AS createdby_user,
    created_by__date    AS createdby_date,
    edited_by__user     AS editedby_user,
    edited_by__date     AS editedby_date,
    TRIM(regexp_replace(description, '\s*\n\s*', ' ', 'g')) AS "description",
    picture,
    status,
    brand,
    category,
    sub_category        AS subcategory,
    product_cb          AS productcb,
    gender,
    "size",
    business_unit       AS businessunit,
    division,
    weight
FROM {{ source('mongo_raw', 'franchise_c_bs') }}