{{
    config(
        materialized='table'
    )
}}

SELECT
    _id,
    name,
    house,
    logo,
    edited_by__user   AS editedby_user,
    edited_by__date   AS editedby_date,
    created_by__date  AS createdby_date,
    created_by__user  AS createdby_user,
    status,
    weight
FROM {{ source('mongo_raw', 'brands') }} -- This tells dbt to read from the raw 'brands' table