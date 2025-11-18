{{
    config(
        materialized='table'
    )
}}

{# Attempt to load the relation for the 'targets_cb' source #}
{% set source_relation = source('mongo_raw', 'targets_cb') %}
{% set relation_exists = load_relation(source_relation) is not none %}

{# If the relation exists, run the transformation query #}
{% if relation_exists %}

SELECT
    _id AS "_id",
    created_by__user AS "createdby_user",
    created_by__date AS "createdby_date",
    edited_by__user AS "editedby_user",
    edited_by__date AS "editedby_date",
    currency AS "currency",
    shop AS "shop",
    brand AS "brand",
    status AS "status",
    CAST(volume AS INTEGER) AS "volume",
    CAST(year AS INTEGER) AS "year",
    CAST(month AS INTEGER) AS "month",
    month_start_date AS "monthstartdate"

FROM {{ source_relation }}

{% else %}

{# If the relation does not exist, create an empty table with the same schema to prevent errors #}
SELECT
    CAST(NULL AS VARCHAR) AS "_id",
    CAST(NULL AS VARCHAR) AS "createdby_user",
    CAST(NULL AS TIMESTAMP WITH TIME ZONE) AS "createdby_date",
    CAST(NULL AS VARCHAR) AS "editedby_user",
    CAST(NULL AS TIMESTAMP WITH TIME ZONE) AS "editedby_date",
    CAST(NULL AS VARCHAR) AS "currency",
    CAST(NULL AS VARCHAR) AS "shop",
    CAST(NULL AS VARCHAR) AS "brand",
    CAST(NULL AS VARCHAR) AS "status",
    CAST(NULL AS INTEGER) AS "volume",
    CAST(NULL AS INTEGER) AS "year",
    CAST(NULL AS INTEGER) AS "month",
    CAST(NULL AS TIMESTAMP WITH TIME ZONE) AS "monthstartdate"
-- The WHERE 1=0 clause ensures that this query returns no rows.
WHERE 1=0

{% endif %}