{{
    config(
        materialized='table'
    )
}}

SELECT
    _id AS "_id",
    house AS "house",
    brand AS "brand",
    size AS "size",
    edited_by__user AS "editedby_user",
    edited_by__date AS "editedby_date",
    created_by__date AS "createdby_date",
    created_by__user AS "createdby_user",
    status AS "status",
    picture AS "picture",
    description AS "description",
    gender AS "gender",
    category AS "category",
    business_unit AS "businessunit",
    product AS "product",
    CAST(weight AS INTEGER) AS "weight",
    bar_code AS "barcode"

FROM {{ source('mongo_raw', 'items') }}