WITH yesterday AS (
    SELECT *
    FROM hosts_cumulated
    WHERE date = '2023-01-01'
),
dedup_events AS (
    SELECT *
       , row_number() OVER(PARTITION BY device_id, event_time) AS row_num
    FROM events
    WHERE device_id IS NOT NULL
        AND user_id IS NOT NULL
        AND event_time::date = '2023-01-02'
),
today AS (
    SELECT
        host
        , (event_time::date) AS today_date
        , ARRAY_AGG(event_time::timestamp) AS event_time_list
    FROM dedup_events e
    WHERE row_num = 1
    GROUP BY host, (event_time::date)
)
INSERT INTO hosts_cumulated
SELECT
       COALESCE(t.host, y.host) AS host,
       COALESCE(y.host_activity_datelist, ARRAY[]::TIMESTAMP[])
            || CASE WHEN t.host IS NOT NULL
                THEN event_time_list
                ELSE ARRAY[]::TIMESTAMP[]
                END AS host_activity_datelist
       , COALESCE(t.today_date, y.date + INTERVAL '1 day') AS date
FROM yesterday y
    FULL OUTER JOIN
    today t ON t.host = y.host;



