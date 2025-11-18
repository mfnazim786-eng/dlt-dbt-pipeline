{{
    config(
        materialized='table'
    )
}}

SELECT
    _id AS "_id",
    name AS "name",
    edited_by__date AS "editedby_date",
    edited_by__user AS "editedby_user",
    created_by__date AS "createdby_date",
    created_by__user AS "createdby_user",
    status AS "status",
    currency AS "currency",
    flag AS "flag",
    division AS "division"

FROM {{ source('mongo_raw', 'markets') }}