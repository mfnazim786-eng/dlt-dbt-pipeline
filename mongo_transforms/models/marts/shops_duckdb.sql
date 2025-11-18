{{
    config(
        materialized='table'
    )
}}

SELECT
    _id AS "_id",
    created_by__user AS "createdby_user",
    created_by__date AS "createdby_date",
    edited_by__user AS "editedby_user",
    edited_by__date AS "editedby_date",
    mall AS "mall",
    city AS "city",
    market AS "market",
    logo AS "logo",
    podium_status AS "podiumstatus",
    status AS "status",
    start_date AS "startdate",
    end_date AS "endDate",
    name AS "name",
    type AS "type",
    customer AS "customer",
    business_unit AS "businessunit",
    CAST(weight AS INTEGER) AS "weight",
    created_at AS "createdat",
    division AS "division",
    animation_type AS "animationtype"

FROM {{ source('mongo_raw', 'shops') }}