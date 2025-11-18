{{
    config(
        materialized='table'
    )
}}

WITH channel_pivoted AS (
    SELECT
        _dlt_parent_id,
        MAX(CASE WHEN _dlt_list_idx = 0 THEN value END) AS "channel_0",
        MAX(CASE WHEN _dlt_list_idx = 1 THEN value END) AS "channel_1"
    FROM {{ source('mongo_raw', 'users__channel') }}
    GROUP BY _dlt_parent_id
),

location_business_unit_pivoted AS (
    SELECT
        _dlt_parent_id,
        MAX(CASE WHEN _dlt_list_idx = 0 THEN value END) AS "location_businessUnit_0"
    FROM {{ source('mongo_raw', 'users__location__business_unit') }}
    GROUP BY _dlt_parent_id
),

location_market_pivoted AS (
    SELECT
        _dlt_parent_id,
        MAX(CASE WHEN _dlt_list_idx = 0 THEN value END) AS "location_market_0"
    FROM {{ source('mongo_raw', 'users__location__market') }} 
    GROUP BY _dlt_parent_id
)

SELECT
    base._id AS "_id",
    base.check_in__shop AS "checkin_shop",
    base.access_role AS "accessrole",
    base.account_lock_count AS "accountlockcount",
    base.avatar AS "avatar",
    base.check_in__customer AS "checkin_customer",
    base.check_in__last_check_in AS "checkin_lastcheckin",
    base.check_in__shift AS "checkin_shift",
    base.company_status AS "companystatus",
    base.contact AS "contact",
    base.created_by__date AS "createdby_date",
    base.created_by__user AS "createdby_user",
    base.date_of_joining AS "dateofjoining",
    base.date_of_promotion AS "dateofpromotion",
    base.designation AS "designation",
    base.distributor AS "distributor",
    base.division AS "division",
    base.edited_by__date AS "editedby_date",
    base.edited_by__user AS "editedby_user",
    base.email AS "email",
    base.is_freelancer AS "isfreelancer",
    base.is_last_check_in_super_sale AS "islastcheckinsupersale",
    base.is_yesterday_sale_disable AS "isyesterdaysaledisable",
    base.last_read_notification_date AS "lastreadnotificationdate",
    base.level AS "level",
    bu."location_businessUnit_0" AS "location_businessunit_0",
    market."location_market_0" AS "location_market_0",
    base.name AS "name",
    base.nationality AS "nationality",
    base.password AS "password",
    base.permanent__city AS "permanent_city",
    base.permanent__customer AS "permanent_customer",
    base.permanent__mall AS "permanent_mall",
    base.portfolio AS "portfolio",
    base.should_checkin AS "shouldcheckin",
    base.staff_id AS "staffid",
    base.status AS "status",
    base.supervisor AS "supervisor",
    base.training_manager AS "trainingmanager",
    channel."channel_0" AS "channel_0",
    channel."channel_1" AS "channel_1"

FROM {{ source('mongo_raw', 'users') }} AS base

LEFT JOIN channel_pivoted AS channel
    ON base._dlt_id = channel._dlt_parent_id
LEFT JOIN location_business_unit_pivoted AS bu
    ON base._dlt_id = bu._dlt_parent_id
LEFT JOIN location_market_pivoted AS market
    ON base._dlt_id = market._dlt_parent_id