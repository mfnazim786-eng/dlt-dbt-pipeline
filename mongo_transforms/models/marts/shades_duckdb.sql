{{
    config(
        materialized='table'
    )
}}

WITH unpivoted_data AS (
    SELECT
        base._id,
        base.description,
        base.division,
        base.status,
        shades._dlt_list_idx,
        shades."Shades_Key",
        shades."Shades_Value"
    FROM
        {{ source('mongo_raw', 'franchise_c_bs') }} AS base
    LEFT JOIN (
        SELECT _dlt_parent_id, _dlt_list_idx, CONCAT('shades_', _dlt_list_idx, '_barCode') AS "Shades_Key", "bar_code" AS "Shades_Value" FROM {{ source('mongo_raw', 'franchise_c_bs__shades') }}
        UNION ALL
        SELECT _dlt_parent_id, _dlt_list_idx, CONCAT('shades_', _dlt_list_idx, '_name') AS "Shades_Key", "name" AS "Shades_Value" FROM {{ source('mongo_raw', 'franchise_c_bs__shades') }}
        UNION ALL
        SELECT _dlt_parent_id, _dlt_list_idx, CONCAT('shades_', _dlt_list_idx, '_id') AS "Shades_Key", "_id" AS "Shades_Value" FROM {{ source('mongo_raw', 'franchise_c_bs__shades') }}
    ) AS shades
        ON base._dlt_id = shades._dlt_parent_id
    WHERE
        shades."Shades_Value" IS NOT NULL
        AND CAST(shades."Shades_Value" AS VARCHAR) <> 'null'
),

pivoted_shades AS (
    SELECT
        _id,
        TRIM(description) AS "description",
        division,
        status,
        _dlt_list_idx,
        MAX(CASE WHEN "Shades_Key" LIKE '%_id' THEN "Shades_Value" END) AS "shade_id",
        TRIM(MAX(CASE WHEN "Shades_Key" LIKE '%_barCode' THEN "Shades_Value" END)) AS "shade_barcode",
        -- MAX(CASE WHEN "Shades_Key" LIKE '%_name' THEN "Shades_Value" END) AS "shade_name"
        TRIM(REPLACE(MAX(CASE WHEN "Shades_Key" LIKE '%_name' THEN "Shades_Value" END), '''', '')) AS "shade_name"
    FROM
        unpivoted_data
    GROUP BY
        _id,
        description,
        division,
        status,
        _dlt_list_idx
)

SELECT
    CONCAT('shades_', _dlt_list_idx, '_', _id) AS "shade_pkey",
    description,
    division,
    status,
    shade_id,
    shade_barcode,
    shade_name
FROM
    pivoted_shades