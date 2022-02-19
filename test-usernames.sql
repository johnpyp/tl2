SELECT 
  *
FROM name_changes nc
JOIN (
  SELECT
    twitch_id,
    count() as times
  FROM name_changes
  GROUP BY twitch_id
) d
ON
  d.times > 1 AND
  d.twitch_id = nc.twitch_id
