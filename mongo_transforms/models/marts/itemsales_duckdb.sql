{{
    config(
        materialized='table'
    )
}}

{% set source_relation = source('mongo_raw', 'item_sale') %}
{% set columns = adapter.get_columns_in_relation(source_relation) %}
-- Create a simple list of just the column names for easy checking
{% set column_names = columns | map(attribute='name') | list %}

-- CTE to get the supervised IDs from the child table and pivot them into columns
WITH supervised_ids_pivoted AS (
    SELECT
        _dlt_parent_id,
        MAX(CASE WHEN _dlt_list_idx = 0 THEN value END) AS "supervisedByIds_0",
        MAX(CASE WHEN _dlt_list_idx = 1 THEN value END) AS "supervisedByIds_1",
        MAX(CASE WHEN _dlt_list_idx = 2 THEN value END) AS "supervisedByIds_2",
        MAX(CASE WHEN _dlt_list_idx = 3 THEN value END) AS "supervisedByIds_3",
        MAX(CASE WHEN _dlt_list_idx = 4 THEN value END) AS "supervisedByIds_4"
    FROM {{ source('mongo_raw', 'item_sale__supervised_by_ids') }}
    GROUP BY _dlt_parent_id
)

SELECT
    base._id AS "_id",
    base.created_time_by_server AS "createdtimebyserver",
    pivoted."supervisedByIds_0" AS "supervisedbyids_0",
    pivoted."supervisedByIds_1" AS "supervisedbyids_1",
    pivoted."supervisedByIds_2" AS "supervisedbyids_2",
    pivoted."supervisedByIds_3" AS "supervisedbyids_3",
    pivoted."supervisedByIds_4" AS "supervisedbyids_4",
    base.created_by__user AS "createdby_user",
    base.created_by__date AS "createdby_date",
    base.edited_by__user AS "editedby_user",
    base.edited_by__date AS "editedby_date",
    base.item AS "item",
    base.sale_id AS "saleid",
    base.count AS "count",
    base.discount AS "discount",
    -- CAST(COALESCE(base.total_price__v_double, TRY_CAST(base.total_price AS DOUBLE)) AS FLOAT) AS "totalprice",
    {% if 'total_price__v_double' in column_names %}
        COALESCE(base.total_price__v_double, TRY_CAST(base.total_price AS DOUBLE)) AS totalprice,
    {% else %}
        TRY_CAST(base.total_price AS DOUBLE) AS totalprice,
    {% endif %}
    (base.created_by__date IS NULL) AS "baddate"

FROM {{ source('mongo_raw', 'item_sale') }} AS base

-- Join the main table with the pivoted IDs
LEFT JOIN supervised_ids_pivoted AS pivoted
    ON base._dlt_id = pivoted._dlt_parent_id