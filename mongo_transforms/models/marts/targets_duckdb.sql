{{
    config(
        materialized='table'
    )
}}

{# Attempt to load the relation for the 'targets' source #}
{% set source_relation = source('mongo_raw', 'targets') %}
{% set relation_exists = load_relation(source_relation) is not none %}

{# If the relation exists, run the transformation query #}
{% if relation_exists %}

SELECT
    src._id AS "_id",
    src.house AS "house",
    CAST(src.month AS INTEGER) AS "month",
    src.shop AS "shop",
    CAST(src.year AS INTEGER) AS "year",
    src.created_by__user AS "createdby_user",
    src.created_by__date AS "createdby_date",
    src.currency AS "currency",
    src.edited_by__user AS "editedby_user",
    src.edited_by__date AS "editedby_date",
    CAST(src.highlight AS BOOLEAN) AS "highlight",
    src.status AS "status",
    CAST(src.volume AS INTEGER) AS "volume",
    CAST(NULL AS INTEGER) AS "rank",
    CAST(src.primary_focus AS BOOLEAN) AS "primaryfocus",
    CAST(src.secondary_focus AS BOOLEAN) AS "secondaryfocus",
    src.month_start_date AS "monthstartdate"
FROM {{ source_relation }} AS src

{% else %}

{# If the relation does not exist, create an empty table with the same schema to prevent errors #}
SELECT
    CAST(NULL AS VARCHAR) AS "_id",
    CAST(NULL AS VARCHAR) AS "house",
    CAST(NULL AS INTEGER) AS "month",
    CAST(NULL AS VARCHAR) AS "shop",
    CAST(NULL AS INTEGER) AS "year",
    CAST(NULL AS VARCHAR) AS "createdby_user",
    CAST(NULL AS TIMESTAMP WITH TIME ZONE) AS "createdby_date",
    CAST(NULL AS VARCHAR) AS "currency",
    CAST(NULL AS VARCHAR) AS "editedby_user",
    CAST(NULL AS TIMESTAMP WITH TIME ZONE) AS "editedby_date",
    CAST(NULL AS BOOLEAN) AS "highlight",
    CAST(NULL AS VARCHAR) AS "status",
    CAST(NULL AS INTEGER) AS "volume",
    CAST(NULL AS INTEGER) AS "rank",
    CAST(NULL AS BOOLEAN) AS "primaryfocus",
    CAST(NULL AS BOOLEAN) AS "secondaryfocus",
    CAST(NULL AS TIMESTAMP WITH TIME ZONE) AS "monthstartdate"
-- The WHERE 1=0 clause ensures that this query returns no rows.
WHERE 1=0

{% endif %}