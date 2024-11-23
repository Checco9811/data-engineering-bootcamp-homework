CREATE TYPE scd_type AS (
    quality_class quality_class,
    is_active boolean,
    start_year INTEGER,
    end_year INTEGER
);

WITH last_year_scd AS (
    SELECT *
    FROM actors_history_scd
    WHERE current_year = 1975
    AND end_year = 1975
),
historical_scd AS (
    SELECT actorid
        , actor
        , quality_class
        , is_active
        , start_year
        , end_year
    FROM actors_history_scd
    WHERE current_year = 1975
    AND end_year < 1975
),
this_year AS (
    SELECT *
    FROM actors
    WHERE current_year = 1976
),
unchanged AS (
    SELECT
        t.actorid
        , t.actor
        , t.quality_class
        , t.is_active
        , l.start_year
        , t.current_year AS end_year
    FROM this_year t
    JOIN last_year_scd l ON t.actorid=l.actorid
    WHERE t.quality_class = l.quality_class
        AND t.is_active = l.is_active
),
changed AS (
    SELECT
        t.actorid
        , t.actor
        , UNNEST(
            ARRAY[
                ROW(
                    l.quality_class,
                    l.is_active,
                    l.start_year,
                    l.end_year
                )::scd_type,
                ROW(
                    t.quality_class,
                    t.is_active,
                    t.current_year,
                    t.current_year
                )::scd_type
            ]
        ) as records
    FROM this_year t
    LEFT JOIN last_year_scd l ON t.actorid=l.actorid
    WHERE t.quality_class <> l.quality_class
        OR t.is_active <> l.is_active
),
unnest_changed AS (
    SELECT actorid
        , actor
        , (records::scd_type).quality_class
        , (records::scd_type).is_active
        , (records::scd_type).end_year
        , (records::scd_type).start_year
    FROM changed
),
new_records AS (
    SELECT
        t.actorid
        , t.actor
        , t.quality_class
        , t.is_active
        , t.current_year AS start_year
        , t.current_year AS end_year
    FROM this_year t
    LEFT JOIN last_year_scd l ON t.actorid=l.actorid
    WHERE l.actorid IS NULL
)
SELECT *
    , 1976 AS current_season
FROM (
  SELECT *
  FROM historical_scd

  UNION ALL

  SELECT *
  FROM unchanged

  UNION ALL

  SELECT *
  FROM unnest_changed

  UNION ALL

  SELECT *
  FROM new_records
) a