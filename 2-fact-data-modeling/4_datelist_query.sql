
WITH date_series AS (
    SELECT GENERATE_SERIES('2023-01-01', '2023-01-31', INTERVAL '1 day') AS valid_date
),
cross_join AS (
    SELECT user_id,
       json.key as browser_id,
       json.value @> TO_JSONB(ARRAY [DATE(d.valid_date)]) AS is_active,
       EXTRACT(DAY FROM DATE('2023-01-31') - d.valid_date) AS days_since
    FROM user_devices_cumulated uc,
        JSONB_EACH(uc.device_activity_datelist) as json
            CROSS JOIN date_series d
    WHERE date = '2023-01-31'
),
bits AS (
    SELECT user_id
        , browser_id
        , SUM(CASE
                WHEN is_active THEN POW(2, 31 - days_since)
                ELSE 0 END
        )::BIGINT::BIT(32) AS device_datelist_int,
        DATE('2023-01-31') as date
    FROM cross_join
    GROUP BY user_id, browser_id
)
SELECT user_id
    , date
    , BIT_OR(device_datelist_int) as datelist_int
FROM bits
GROUP BY user_id, date
