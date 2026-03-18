-- Bridge table, same as condition
WITH unnested AS (
	SELECT
		nct_id,
		unnest(interventions) AS i
	FROM {{ ref('stg_study') }}
	WHERE interventions IS NOT NULL
)

SELECT
	u.nct_id,
	d.intervention_type,
	d.intervention_name
FROM unnested						u
-- This joing would be simpler with a generated surrogate key
JOIN {{ ref('dim_intervention') }}	d
	ON  u.i['type'] = d.intervention_type
	AND u.i['name'] = d.intervention_name
