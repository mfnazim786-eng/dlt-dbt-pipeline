{{
    config(
        materialized='table'
    )
}}

SELECT
    _id,
    name,
    logo,
    edited_by__user   AS "editedby_user",
    edited_by__date   AS "editedby_date",
    created_by__date  AS "createdby_date",
    created_by__user  AS "createdby_user",
    status,
    weight,
    short_name AS shortname,
    hide
FROM {{ source('mongo_raw', 'houses') }}