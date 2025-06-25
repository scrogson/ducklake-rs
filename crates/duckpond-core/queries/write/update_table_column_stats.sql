INSERT OR REPLACE INTO ducklake_table_column_stats (
    table_id,
    column_id,
    contains_null,
    contains_nan,
    min_value,
    max_value
)
WITH existing AS (
    SELECT contains_null, contains_nan, min_value, max_value 
    FROM ducklake_table_column_stats 
    WHERE table_id = $1 AND column_id = $2
)
SELECT 
    $1,
    $2,
    COALESCE(existing.contains_null, false) OR ($3 > 0),
    COALESCE(existing.contains_nan, false) OR ($4 > 0),
    CASE 
        WHEN $5 IS NULL THEN existing.min_value
        WHEN existing.min_value IS NULL THEN $5
        WHEN existing.min_value < $5 THEN existing.min_value
        ELSE $5
    END,
    CASE 
        WHEN $6 IS NULL THEN existing.max_value
        WHEN existing.max_value IS NULL THEN $6
        WHEN existing.max_value > $6 THEN existing.max_value
        ELSE $6
    END
FROM (SELECT 1) dummy
LEFT JOIN existing ON 1=1; 