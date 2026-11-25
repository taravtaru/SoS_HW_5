SELECT movie_id, countIf(event_type = 'VIEW_STARTED') AS views
FROM cinema.raw_events FINAL
WHERE toDate(timestamp) = {day:Date} AND event_type = 'VIEW_STARTED'
GROUP BY movie_id
ORDER BY views DESC, movie_id
LIMIT 10
