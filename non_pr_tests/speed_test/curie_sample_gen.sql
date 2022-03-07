SELECT curie
FROM (
  SELECT *, row_number() OVER(ORDER BY reference_id ASC) AS row
  FROM "references"
) t
WHERE t.row % 7 = 0
LIMIT 100000
