{{ config(
    materialized='table'
) }}

WITH deduplicated_goods AS (
    SELECT
        goodscode,
        goodscode2,
        goodstype1,
        goodstype2,
        goodstype3,
        goodstype4,
        ROW_NUMBER() OVER (
            PARTITION BY goodscode, goodscode2  -- ユニークキーの組み合わせ
             -- ORDER BY row_id DESC
        ) AS row_num
    FROM
        staging_goods  -- データレイクから取り込んだ重複が含まれるデータ
)

-- row_num = 1（最新のデータのみ）を抽出
SELECT
    goodscode AS goodscode1,
    goodscode2,
    goodstype1,
    goodstype2,
    goodstype3,
    goodstype4
FROM
    deduplicated_goods
WHERE
    row_num = 1
