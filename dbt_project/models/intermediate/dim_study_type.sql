-- As per Kimball's theory, dimension tables should have a surrogate key.
-- This is also a degenerate dimension, because it only contains one column.
-- For the purposes of this test, however, its enough.
SELECT DISTINCT
	study_type
FROM {{ ref('stg_study') }}
WHERE study_type IS NOT NULL
