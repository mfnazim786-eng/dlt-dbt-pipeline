{{
    config(
        materialized='table'
    )
}}

WITH spent_days_pivoted AS (
    SELECT
        _dlt_parent_id,
        MAX(CASE WHEN _dlt_list_idx = 0 THEN value END) AS "previousCheckIn_spentDays_0"
    FROM {{ source('mongo_raw', 'checkins__previous_check_in__spent_days') }}
    GROUP BY _dlt_parent_id
),

spent_days_in_current_month_pivoted AS (
    SELECT
        _dlt_parent_id,
        MAX(CASE WHEN _dlt_list_idx = 0 THEN value END) AS "previousCheckIn_spentDaysInCurrentMonth_0"
    FROM {{ source('mongo_raw', 'checkins__previous_check_in__spent_days_in_current_month') }}
    GROUP BY _dlt_parent_id
)

SELECT
    base._id AS "_id",
    base.customer AS "customer",
    base.day AS "day",
    base.last_check_in AS "lastcheckin",
    base.month AS "month",
    base.previous_check_in__date AS "previouscheckin_date",
    base.previous_check_in__shift AS "previouscheckin_shift",
    base.previous_check_in__shop AS "previouscheckin_shop",
    spent_days."previousCheckIn_spentDays_0" AS "previouscheckin_spentdays_0",
    spent_days_month."previousCheckIn_spentDaysInCurrentMonth_0" AS "previouscheckin_spentdaysincurrentmonth_0",
    base.shift AS "shift",
    base.shop AS "shop",
    base.user AS "user"

FROM {{ source('mongo_raw', 'checkins') }} AS base

LEFT JOIN spent_days_pivoted AS spent_days
    ON base._dlt_id = spent_days._dlt_parent_id
LEFT JOIN spent_days_in_current_month_pivoted AS spent_days_month
    ON base._dlt_id = spent_days_month._dlt_parent_id