{{
    config(
        materialized='table'
    )
}}

-- Final SELECT to transform and cast all data
SELECT
    _id AS "_id",
    logo AS "logo",
    status AS "status",
    created_by__user AS "createdby_user",
    created_by__date AS "createdby_date",
    edited_by__user AS "editedby_user",
    edited_by__date AS "editedby_date",
    name AS "name",
    business_unit AS "businessunit",
    CAST(weight AS INTEGER) AS "weight",
    division AS "division",
    channel AS "channel"

FROM {{ source('mongo_raw', 'customers') }}