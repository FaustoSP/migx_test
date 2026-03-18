-- What are the most common conditions being studied?
-- This one is a bit more ambiguous, I decided to go for a top 10.
WITH condition_counts AS (
	SELECT
		condition_name,
		COUNT(nct_id)	AS trial_count,
		RANK() OVER (ORDER BY COUNT(nct_id) DESC)	AS rnk
	FROM {{ ref('bridge_study_condition') }}
	GROUP BY condition_name
)

SELECT
	rnk,
	condition_name,
	trial_count
FROM condition_counts
WHERE rnk <= 10
ORDER BY rnk
