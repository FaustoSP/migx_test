WITH unnested AS (
	SELECT unnest(conditions) AS condition_name
	FROM {{ ref('stg_study') }}
	WHERE conditions IS NOT NULL
)

SELECT DISTINCT condition_name
FROM unnested
WHERE condition_name IS NOT NULL
