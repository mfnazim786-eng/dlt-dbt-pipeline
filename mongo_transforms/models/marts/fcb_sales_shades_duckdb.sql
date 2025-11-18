{{
    config(
        materialized='table'
    )
}}

-- Step 1: Join the main table with its 'shades' child table.
WITH franchise_cb_sale_with_shades AS (
    SELECT
        base._id,
        base.franchise_cb       AS "franchiseCB",
        base.sale_cb_id         AS "saleCBId",
        base.created_by__date   AS "createdBy_date",
        row_to_json(shades_table) AS "shades_json",
        shades_table._dlt_list_idx AS "shade_number"
    FROM "local_mongo"."mongo_select"."franchise_cb_sale" AS base
    LEFT JOIN "local_mongo"."mongo_select"."franchise_cb_sale__shades" AS shades_table
        ON base._dlt_id = shades_table._dlt_parent_id
),

-- Step 2: Unpivot the shades_json object into key-value pairs.
unpivoted_data AS (
    SELECT
        t.*,
        UNNEST(
            str_split(
                regexp_replace(t.shades_json::VARCHAR, '[{}"]', '', 'g'),
                ','
            )
        ) AS kv_pair
    FROM franchise_cb_sale_with_shades AS t
),

-- Step 3: Extract the key and value from the generated pairs.
franchiseCBSale_unpivot AS (
    SELECT
        _id,
        "createdBy_date",
        "franchiseCB",
        "saleCBId",
        "shade_number",
        str_split(kv_pair, ':')[1] AS "Shades_Key",
        str_split(kv_pair, ':')[2] AS "Shades_Value"
    FROM unpivoted_data
    WHERE str_split(kv_pair, ':')[1] NOT IN (
        '_dlt_id', '_dlt_parent_id', '_dlt_list_idx'
    )
    AND str_split(kv_pair, ':')[2] IS NOT NULL AND str_split(kv_pair, ':')[2] <> 'null'
)

-- Step 4: Final aggregation to create the fcb_sales table structure.
SELECT
    _id AS "fcb_sales_id",
    "createdBy_date" AS "createdby_date",
    "saleCBId" AS "salecbid",
    CONCAT('shades_',"shade_number", '_', str_split("Shades_Key", '_')[2], "franchiseCB") AS "shade_pkey",
    MAX("Shades_Value") AS "shade_quantity"
FROM franchiseCBSale_unpivot
WHERE "Shades_Key" LIKE '%quantity%'
GROUP BY
    "fcb_sales_id",
    "createdBy_date",
    "saleCBId",
    "shade_pkey"