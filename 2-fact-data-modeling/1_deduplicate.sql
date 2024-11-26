WITH dedup_events AS (
    SELECT *
       , row_number() OVER(PARTITION BY device_id, event_time) AS row_num
    FROM events
    WHERE device_id IS NOT NULL
        AND user_id IS NOT NULL
    )
SELECT *
FROM dedup_events e
WHERE row_num = 1
