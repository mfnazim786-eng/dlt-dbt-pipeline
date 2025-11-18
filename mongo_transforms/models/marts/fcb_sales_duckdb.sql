{{
    config(
        materialized='table'
    )
}}

-- WITH supervised AS (
--     SELECT
--         _dlt_parent_id,
--         value AS supervisedByIds_0
--     FROM {{ source('mongo_raw', 'franchise_cb_sale__supervised_by_ids') }}
--     WHERE _dlt_list_idx = 0
-- ),

-- discounts AS (
--     SELECT
--         link_table._dlt_parent_id,
--         values_table.value AS discountIds_0
--     FROM {{ source('mongo_raw', 'franchise_cb_sale__discount_ids__list') }} AS values_table
--     JOIN {{ source('mongo_raw', 'franchise_cb_sale__discount_ids') }} AS link_table
--       ON values_table._dlt_parent_id = link_table._dlt_id
--     WHERE values_table._dlt_list_idx = 0
-- )

-- SELECT
--     base._id,
--     base.created_by__user           AS createdby_user,
--     base.created_by__date           AS createdby_date,
--     base.edited_by__user            AS editedby_user,
--     base.edited_by__date            AS editedby_date,
--     supervised.supervisedByIds_0 AS supervisedbyids_0,
--     discounts.discountIds_0 AS discountids_0,
--     base.franchise_cb               AS franchisecb,
--     base.sale_cb_id                 AS salecbid,
--     base.count,
--     -- TRY_CAST(base.total_price AS DOUBLE) AS totalprice,
--     CAST(COALESCE(base.total_price__v_double, TRY_CAST(base.total_price AS DOUBLE)) AS FLOAT) AS "totalprice",
--     base.created_time_by_server     AS createdtimebyserver,
--     base.is_discounted              AS isdiscounted,
--     -- TRY_CAST(base.actual_price AS DOUBLE) AS actualprice,
--     CAST(COALESCE(base.actual_price__v_double, TRY_CAST(base.actual_price AS DOUBLE)) AS FLOAT) AS "actualprice",
--     -- TRY_CAST(base.discount_amount AS DOUBLE) AS discountamount,
--     CAST(COALESCE(base.discount_amount__v_double, TRY_CAST(base.discount_amount AS DOUBLE)) AS FLOAT) AS "discountamount",
--     base.discount_type              AS discounttype,
--     -- TRY_CAST(base.discounted_price AS DOUBLE) AS discountedprice,
--     CAST(COALESCE(base.discounted_price__v_double, TRY_CAST(base.discounted_price AS DOUBLE)) AS FLOAT) AS "discountedprice",
--     base.current_supervisor         AS currentsupervisor    

-- FROM {{ source('mongo_raw', 'franchise_cb_sale') }} AS base
-- LEFT JOIN supervised ON base._dlt_id = supervised._dlt_parent_id
-- LEFT JOIN discounts ON base._dlt_id = discounts._dlt_parent_id



-- {{ config( materialized='table' ) }}

-- This Jinja block runs before the SQL is executed.
-- It inspects the source table to see which columns actually exist.
{% set source_relation = source('mongo_raw', 'franchise_cb_sale') %}
{% set columns = adapter.get_columns_in_relation(source_relation) %}
-- Create a simple list of just the column names for easy checking
{% set column_names = columns | map(attribute='name') | list %}


WITH supervised AS (
    SELECT
        _dlt_parent_id,
        value AS supervisedByIds_0
    FROM {{ source('mongo_raw', 'franchise_cb_sale__supervised_by_ids') }}
    WHERE _dlt_list_idx = 0
),

discounts AS (
    SELECT
        link_table._dlt_parent_id,
        values_table.value AS discountIds_0
    FROM {{ source('mongo_raw', 'franchise_cb_sale__discount_ids__list') }} AS values_table
    JOIN {{ source('mongo_raw', 'franchise_cb_sale__discount_ids') }} AS link_table
        ON values_table._dlt_parent_id = link_table._dlt_id
    WHERE values_table._dlt_list_idx = 0
)

-- -- CORRECTED AND SIMPLIFIED DISCOUNTS CTE
-- discounts AS (
--     SELECT
--         _dlt_parent_id,
--         value AS discountIds_0
--     -- Use the correct table name suggested by the error message
--     FROM {{ source('mongo_raw', 'franchise_cb_sale__discount_ids') }}
--     -- We only want the first discount for this logic
--     WHERE _dlt_list_idx = 0
-- )

SELECT
    base._id,
    base.created_by__user AS createdby_user,
    base.created_by__date AS createdby_date,
    base.edited_by__user AS editedby_user,
    base.edited_by__date AS editedby_date,
    supervised.supervisedByIds_0 AS supervisedbyids_0,
    discounts.discountIds_0 AS discountids_0,
    base.franchise_cb AS franchisecb,
    base.sale_cb_id AS salecbid,
    base.count,
    base.created_time_by_server AS createdtimebyserver,
    base.is_discounted AS isdiscounted,
    base.discount_type AS discounttype,
    base.current_supervisor AS currentsupervisor,

    -- Conditionally generate the SQL for totalprice
    {% if 'total_price__v_double' in column_names %}
        COALESCE(base.total_price__v_double, TRY_CAST(base.total_price AS DOUBLE)) AS totalprice,
    {% else %}
        TRY_CAST(base.total_price AS DOUBLE) AS totalprice,
    {% endif %}

    -- Conditionally generate the SQL for actualprice
    {% if 'actual_price__v_double' in column_names %}
        COALESCE(base.actual_price__v_double, TRY_CAST(base.actual_price AS DOUBLE)) AS actualprice,
    {% else %}
        TRY_CAST(base.actual_price AS DOUBLE) AS actualprice,
    {% endif %}

    -- Conditionally generate the SQL for discountamount
    {% if 'discount_amount__v_double' in column_names %}
        COALESCE(base.discount_amount__v_double, TRY_CAST(base.discount_amount AS DOUBLE)) AS discountamount,
    {% else %}
        TRY_CAST(base.discount_amount AS DOUBLE) AS discountamount,
    {% endif %}

    -- Conditionally generate the SQL for discountedprice
    {% if 'discounted_price__v_double' in column_names %}
        COALESCE(base.discounted_price__v_double, TRY_CAST(base.discounted_price AS DOUBLE)) AS discountedprice
    {% else %}
        TRY_CAST(base.discounted_price AS DOUBLE) AS discountedprice
    {% endif %}

FROM {{ source('mongo_raw', 'franchise_cb_sale') }} AS base
LEFT JOIN supervised ON base._dlt_id = supervised._dlt_parent_id
LEFT JOIN discounts ON base._dlt_id = discounts._dlt_parent_id