{{
    config(
        materialized='table'
    )
}}

SELECT
    _id AS "_id",
    name AS "name",
    symbol__grapheme AS "symbol_grapheme",
    symbol__template AS "symbol_template",
    CAST(symbol__rtl AS BOOLEAN) AS "symbol_rtl",
    iso AS "iso",
    created_by__user AS "createdby_user",
    created_by__date AS "createdby_date",
    edited_by__user AS "editedby_user",
    edited_by__date AS "editedby_date",
    status AS "status"

FROM {{ source('mongo_raw', 'currencies') }}