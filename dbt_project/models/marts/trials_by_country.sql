-- Geographic distribution of clinical trials
WITH country_counts AS (
	SELECT
		country,
		COUNT(nct_id)	AS trial_count
	FROM {{ ref('bridge_study_country') }}
	GROUP BY country
)

SELECT
	country,
	trial_count,
	RANK() OVER (ORDER BY trial_count DESC)											AS rnk,
	ROUND(100.0 * trial_count / SUM(trial_count) OVER (), 2)						AS pct_of_total
FROM country_counts
ORDER BY rnk
