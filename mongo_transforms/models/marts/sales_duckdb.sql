{{
    config(
        materialized='table'
    )
}}

WITH supervised_ids_pivoted AS (
    SELECT
        _dlt_parent_id,
        MAX(CASE WHEN _dlt_list_idx = 0 THEN value END) AS "supervisedByIds_0",
        MAX(CASE WHEN _dlt_list_idx = 1 THEN value END) AS "supervisedByIds_1",
        MAX(CASE WHEN _dlt_list_idx = 2 THEN value END) AS "supervisedByIds_2",
        MAX(CASE WHEN _dlt_list_idx = 3 THEN value END) AS "supervisedByIds_3",
        MAX(CASE WHEN _dlt_list_idx = 4 THEN value END) AS "supervisedByIds_4"
    FROM {{ source('mongo_raw', 'sales__supervised_by_ids') }}
    GROUP BY _dlt_parent_id
)

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
    pivoted.supervisedByIds_4 AS "supervisedbyids_4",
    base.time_slot AS "timeslot",
    base.shift,
    base.shop,
    base.created_time_by_server AS "createdtimebyserver",
    -- base.survey_data__phone AS "surveydata_phone",
    {{ safe_select_column(source('mongo_raw', 'sales'), 'survey_data__phone') }},
    {{ safe_select_column(source('mongo_raw', 'sales'), 'survey_data__email') }},
    {{ safe_select_column(source('mongo_raw', 'sales'), 'survey_data__dob') }},
    {{ safe_select_column(source('mongo_raw', 'sales'), 'survey_data__name') }}
    -- base.survey_data__name AS "surveydata_name"

FROM {{ source('mongo_raw', 'sales') }} AS base

LEFT JOIN supervised_ids_pivoted AS pivoted
    ON base._dlt_id = pivoted._dlt_parent_id