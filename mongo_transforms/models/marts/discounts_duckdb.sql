{{
    config(
        materialized='table'
    )
}}

{# Attempt to load the relation for the 'discounts' source #}
{% set source_relation = source('mongo_raw', 'discounts') %}
{% set relation_exists = load_relation(source_relation) is not none %}

{# If the relation exists, run the transformation query #}
{% if relation_exists %}

SELECT
    _id AS "_id",
    brand AS "brand",
    business_unit AS "businessunit",
    created_at AS "createdat",
    created_by__date AS "createdby_date",
    created_by__user AS "createdby_user",
    customer AS "customer",
    description AS "description",
    discount_level AS "discountlevel",
    discount_status AS "discountstatus",
    division AS "division",
    edited_by__date AS "editedby_date",
    edited_by__user AS "editedby_user",
    end_date AS "enddate",
    location_level AS "locationlevel",
    market AS "market",
    start_date AS "startdate",
    status AS "status",
    type AS "type",
    CAST(weight AS INTEGER) AS "weight"

FROM {{ source_relation }}

{% else %}

{# If the relation does not exist, create an empty table with the same schema to prevent errors #}
SELECT
    CAST(NULL AS VARCHAR) AS "_id",
    CAST(NULL AS VARCHAR) AS "brand",
    CAST(NULL AS VARCHAR) AS "businessunit",
    CAST(NULL AS TIMESTAMP WITH TIME ZONE) AS "createdat",
    CAST(NULL AS TIMESTAMP WITH TIME ZONE) AS "createdby_date",
    CAST(NULL AS VARCHAR) AS "createdby_user",
    CAST(NULL AS VARCHAR) AS "customer",
    CAST(NULL AS VARCHAR) AS "description",
    CAST(NULL AS VARCHAR) AS "discountlevel",
    CAST(NULL AS VARCHAR) AS "discountstatus",
    CAST(NULL AS VARCHAR) AS "division",
    CAST(NULL AS TIMESTAMP WITH TIME ZONE) AS "editedby_date",
    CAST(NULL AS VARCHAR) AS "editedby_user",
    CAST(NULL AS TIMESTAMP WITH TIME ZONE) AS "enddate",
    CAST(NULL AS VARCHAR) AS "locationlevel",
    CAST(NULL AS VARCHAR) AS "market",
    CAST(NULL AS TIMESTAMP WITH TIME ZONE) AS "startdate",
    CAST(NULL AS VARCHAR) AS "status",
    CAST(NULL AS VARCHAR) AS "type",
    CAST(NULL AS INTEGER) AS "weight"
-- The WHERE 1=0 clause ensures that this query returns no rows.
WHERE 1=0

{% endif %}