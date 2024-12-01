WITH yesterday AS (
    SELECT *
    FROM host_activity_reduced
    WHERE month_start = '2023-01-01'
),
dedup_events AS (
    SELECT *
       , row_number() OVER(PARTITION BY device_id, event_time) AS row_num
    FROM events
    WHERE device_id IS NOT NULL
        AND user_id IS NOT NULL
        AND event_time::date = '2023-01-01'
),
today AS (
    SELECT
        host
        , (event_time::date) AS today_date
        , COUNT(1) AS num_hits
        , COUNT(DISTINCT user_id) AS num_users
    FROM dedup_events e
    WHERE row_num = 1
    GROUP BY host, (event_time::date)
)
INSERT INTO host_activity_reduced
SELECT
    COALESCE(y.host, t.host) AS user_id
    , COALESCE(y.month_start, DATE(DATE_TRUNC('MONTH', t.today_date))) AS month_start
    , CASE WHEN y.hit_array IS NOT NULL THEN
        y.hit_array || ARRAY[COALESCE(t.num_hits, 0)]
        WHEN y.hit_array IS NULL THEN
           ARRAY_FILL(0, ARRAY[t.today_date - DATE(DATE_TRUNC('MONTH', t.today_date))]) || ARRAY[COALESCE(t.num_hits, 0)]
        END AS hit_array
    , CASE WHEN y.unique_visitors IS NOT NULL THEN
        y.unique_visitors || ARRAY[COALESCE(t.num_users, 0)]
        WHEN y.unique_visitors IS NULL THEN
           ARRAY_FILL(0, ARRAY[t.today_date - DATE(DATE_TRUNC('MONTH', t.today_date))]) || ARRAY[COALESCE(t.num_users, 0)]
        END AS unique_visitors
FROM yesterday y
    FULL OUTER JOIN today t
        ON y.host = t.host
ON CONFLICT (host, month_start)
DO
    UPDATE SET hit_array = excluded.hit_array,
       unique_visitors = excluded.unique_visitors;
