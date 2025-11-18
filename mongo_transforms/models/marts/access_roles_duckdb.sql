{{
    config(
        materialized='table'
    )
}}

WITH supervisor_for_pivoted AS (
    SELECT
        _dlt_parent_id,
        MAX(CASE WHEN _dlt_list_idx = 0 THEN value END) AS "supervisorFor_0",
        MAX(CASE WHEN _dlt_list_idx = 1 THEN value END) AS "supervisorFor_1"
    FROM {{ source('mongo_raw', 'access_roles__supervisor_for') }}
    GROUP BY _dlt_parent_id
),

supervised_by_pivoted AS (
    SELECT
        _dlt_parent_id,
        MAX(CASE WHEN _dlt_list_idx = 0 THEN value END) AS "supervisedBy_0"
    FROM {{ source('mongo_raw', 'access_roles__supervised_by') }}
    GROUP BY _dlt_parent_id
),


business_unit_pivoted AS (
    SELECT
        _dlt_parent_id,
        MAX(CASE WHEN _dlt_list_idx = 0 THEN value END) AS "businessUnit_0"
    FROM {{ source('mongo_raw', 'access_roles__business_unit') }}
    GROUP BY _dlt_parent_id
)

SELECT
    base._id AS "_id",
    base.allowed_login_from_mobile AS "allowedloginfrommobile",
    supervisor_for."supervisorFor_0" AS "supervisorfor_0",
    supervisor_for."supervisorFor_1" AS "supervisorfor_1",
    supervised_by."supervisedBy_0" AS "supervisedby_0",
    base.permitted_as_supervisor AS "permittedassupervisor",
    base.require_portfolio AS "requireportfolio",
    base.require_supervisor AS "requiresupervisor",
    base.name AS "name",
    base.level AS "level",
    base.created_at AS "createdat",
    base.allowed_login_from_cms AS "allowedloginfromcms",
    business_unit."businessUnit_0" AS "businessunit_0",
    base.division AS "division"

FROM {{ source('mongo_raw', 'access_roles') }} AS base

LEFT JOIN supervisor_for_pivoted AS supervisor_for
    ON base._dlt_id = supervisor_for._dlt_parent_id
LEFT JOIN supervised_by_pivoted AS supervised_by
    ON base._dlt_id = supervised_by._dlt_parent_id
LEFT JOIN business_unit_pivoted AS business_unit
    ON base._dlt_id = business_unit._dlt_parent_id