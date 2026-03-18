-- This struct also contained lat and long, but I decided to drop them because
-- they had little analytical value to the questions asked by the test
WITH unnested AS (
	SELECT unnest(locations) AS loc
	FROM {{ ref('stg_study') }}
	WHERE locations IS NOT NULL
)

SELECT DISTINCT
	loc['country'] AS country
FROM unnested
WHERE loc['country'] IS NOT NULL
