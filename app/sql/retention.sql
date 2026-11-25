WITH cohort AS (
    SELECT user_id
    FROM cinema.raw_events FINAL
    WHERE event_type = 'VIEW_STARTED' AND toDate(timestamp) <= {day:Date}
      AND user_id IN (
          SELECT user_id FROM cinema.raw_events FINAL
          WHERE event_type = 'VIEW_STARTED' AND toDate(timestamp) = {day:Date}
      )
    GROUP BY user_id
    HAVING min(toDate(timestamp)) = {day:Date}
)
SELECT
    dateDiff('day', {day:Date}, toDate(timestamp)) AS day_offset,
    uniqExact(user_id) AS users
FROM cinema.raw_events FINAL
WHERE toDate(timestamp) BETWEEN {day:Date} AND {day:Date} + 7
  AND user_id IN (SELECT user_id FROM cohort)
GROUP BY day_offset
ORDER BY day_offset
