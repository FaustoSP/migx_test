-- How many trials by study type and phase?
-- This one was very simple
SELECT
	study_type,
	phases,
	COUNT(nct_id)	AS trial_count
FROM {{ ref('fact_study') }}
WHERE study_type IS NOT NULL
GROUP BY study_type, phases
ORDER BY study_type, trial_count DESC
