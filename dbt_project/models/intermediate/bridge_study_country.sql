-- To my surprise, country also has a many to many relationship with study.
WITH unnested AS (
	SELECT
		nct_id,
		unnest(locations) AS loc
	FROM {{ ref('stg_study') }}
	WHERE locations IS NOT NULL
)

SELECT DISTINCT
	u.nct_id,
	d.country
FROM unnested					u
JOIN {{ ref('dim_country') }}	d ON u.loc['country'] = d.country
