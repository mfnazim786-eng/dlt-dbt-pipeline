{{
    config(
        materialized='table'
    )
}}

SELECT
    _id AS "_id",
    logo AS "logo",
    CAST(mock AS BOOLEAN) AS "mock",
    status AS "status",
    created_by__user AS "createdby_user",
    created_by__date AS "createdby_date",
    edited_by__user AS "editedby_user",
    edited_by__date AS "editedby_date",
    name AS "name",
    city AS "city",
    division AS "division"

FROM {{ source('mongo_raw', 'malls') }}