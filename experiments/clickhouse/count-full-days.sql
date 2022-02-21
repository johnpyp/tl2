-- Get the count of message in each 5 minute interval for a given daterange, 
SELECT t, c
FROM (
  SELECT toStartOfFiveMinute(ts) as t, count() as c
  FROM messages
  WHERE t BETWEEN toDateTime('2022-02-21 00:00:00') AND toDateTime('2022-02-22 00:00:00')
  GROUP BY t
)
ORDER BY t WITH FILL FROM toDateTime('2022-02-21 00:00:00') TO toDateTime('2022-02-22 00:00:00') STEP INTERVAL 5 MINUTE;

-- Count of total and "present" 5 minute intervals in a given date range
-- Present meaning at least 1 message in the 5 minute interval
SELECT count() as n_rows, countIf(c >= 1) as n_rows_present
FROM (
  SELECT t, c 
  FROM (
    SELECT toStartOfFiveMinute(ts) as t, count() as c
    FROM messages
    WHERE t BETWEEN toDateTime('2022-02-21 00:00:00') AND toDateTime('2022-02-22 00:00:00')
    GROUP BY t
  )
  ORDER BY t WITH FILL FROM toDateTime('2022-02-21 00:00:00') TO toDateTime('2022-02-22 00:00:00') STEP INTERVAL 5 MINUTE
)
