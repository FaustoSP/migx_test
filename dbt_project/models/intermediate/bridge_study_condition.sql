-- Bridge table because condition has a many-to-many relation with study
WITH unnested AS (
	SELECT
		nct_id,
		unnest(conditions) AS condition_name
	FROM {{ ref('stg_study') }}
	WHERE conditions IS NOT NULL
)

SELECT
	u.nct_id,
	d.condition_name
FROM unnested u
JOIN {{ ref('dim_condition') }}	d ON u.condition_name = d.condition_name
