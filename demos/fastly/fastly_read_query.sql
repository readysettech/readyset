SELECT
	A.id,
	A.title,
	A.keywords,
	A.creation_time,
	A.short_text,
	A.image_url,
	A.url
FROM
	articles A, recommendations R
WHERE
	A.id = R.article_id AND
	R.user_id = ?
LIMIT 5
