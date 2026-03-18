WITH unnested AS (
	SELECT unnest(interventions) AS intervention
	FROM {{ ref('stg_study') }}
	WHERE interventions IS NOT NULL
)

SELECT DISTINCT
	intervention['type']	AS intervention_type,
	intervention['name']	AS intervention_name
FROM unnested
WHERE intervention['name'] IS NOT NULL
