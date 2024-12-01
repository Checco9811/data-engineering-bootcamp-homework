WITH yesterday AS (
    SELECT *
    FROM user_devices_cumulated
    WHERE date = '2023-01-30'
),
today AS (
    SELECT user_id::text
         , (event_time::date) AS today_date
         , JSONB_OBJECT_AGG(
             browser_type, ARRAY[(event_time::date)]
         ) as date_list
    FROM events e JOIN devices d ON e.device_id = d.device_id
    WHERE event_time::date = '2023-01-31'
        AND e.device_id IS NOT NULL
        AND user_id IS NOT NULL
    GROUP BY e.user_id, (event_time::date)
)
INSERT INTO user_devices_cumulated
SELECT
       COALESCE(t.user_id, y.user_id) AS user_id,
       (
        SELECT
            JSONB_OBJECT_AGG(
                COALESCE(ka, kb),
                CASE
                    WHEN va IS NULL THEN vb
                    WHEN vb IS NULL THEN va
                    ELSE vb || va
                END
            )
        FROM JSONB_EACH(COALESCE(y.device_activity_datelist, '{}'::JSONB)) e1(ka, va)
        FULL OUTER JOIN JSONB_EACH(date_list) e2(kb, vb) on ka = kb
        ) AS device_activity_datelist,
       COALESCE(t.today_date, y.date + INTERVAL '1 day') AS date
FROM yesterday y
    FULL OUTER JOIN
    today t ON t.user_id = y.user_id;

