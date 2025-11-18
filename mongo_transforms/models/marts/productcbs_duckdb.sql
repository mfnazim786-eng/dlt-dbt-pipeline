{{
    config(
        materialized='table'
    )
}}

SELECT
    _id AS "_id",
    created_by__user AS "createdby_user",
    created_by__date AS "createdby_date",
    edited_by__user AS "editedby_user",
    edited_by__date AS "editedby_date",
    logo AS "logo",
    status AS "status",
    name AS "name",
    division AS "division",
    CAST(weight AS INTEGER) AS "weight"

FROM {{ source('mongo_raw', 'product_c_bs') }}