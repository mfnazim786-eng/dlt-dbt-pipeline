{{
    config(
        materialized='table'
    )
}}

-- Step 1: CTE to get the supervised IDs from the child table and pivot them into columns.
-- This creates columns for each supervisor ID in the nested array.
WITH supervised_ids_pivoted AS (
    SELECT
        _dlt_parent_id,
        MAX(CASE WHEN _dlt_list_idx = 0 THEN value END) AS "supervisedByIds_0",
        MAX(CASE WHEN _dlt_list_idx = 1 THEN value END) AS "supervisedByIds_1",
        MAX(CASE WHEN _dlt_list_idx = 2 THEN value END) AS "supervisedByIds_2",
        MAX(CASE WHEN _dlt_list_idx = 3 THEN value END) AS "supervisedByIds_3"
        -- NOTE: The required table only goes up to index 3.
    FROM {{ source('mongo_raw', 'sales_cb__supervised_by_ids') }}
    GROUP BY _dlt_parent_id
)

-- Final Step: Join the main table with the pivoted IDs and apply all transformations.
SELECT
    base._id,
    base.survey_data__age AS "surveydata_age",
    base.survey_data__gender AS "surveydata_gender",
    base.survey_data__nationality AS "surveydata_nationality",
    base.created_by__user AS "createdby_user",
    base.created_by__date AS "createdby_date",
    base.edited_by__user AS "editedby_user",
    base.edited_by__date AS "editedby_date",
    pivoted.supervisedByIds_0 AS "supervisedbyids_0",
    pivoted.supervisedByIds_1 AS "supervisedbyids_1",
    pivoted.supervisedByIds_2 AS "supervisedbyids_2",
    pivoted.supervisedByIds_3 AS "supervisedbyids_3",
    base.time_slot AS "timeslot",
    base.shift,
    base.shop,
    base.created_time_by_server AS "createdtimebyserver",
    base.current_supervisor AS "currentsupervisor",
    {{ safe_select_column(source('mongo_raw', 'sales_cb'), 'survey_data__phone') }},
    {{ safe_select_column(source('mongo_raw', 'sales_cb'), 'survey_data__email') }},
    {{ safe_select_column(source('mongo_raw', 'sales_cb'), 'survey_data__dob') }},
    {{ safe_select_column(source('mongo_raw', 'sales_cb'), 'survey_data__name') }}

FROM {{ source('mongo_raw', 'sales_cb') }} AS base

-- Join the main table with the pivoted IDs
LEFT JOIN supervised_ids_pivoted AS pivoted
    ON base._dlt_id = pivoted._dlt_parent_id