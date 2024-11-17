CREATE TYPE film_stats AS (
	film TEXT,
	year INTEGER,
	votes INTEGER,
	rating REAL,
	filmid TEXT
);

CREATE TYPE quality_class AS ENUM ('star', 'good', 'average', 'bad');


CREATE TABLE actors (
    actorid TEXT,
	actor TEXT,
	films film_stats[],
	quality_class quality_class,
	is_active BOOLEAN,
	current_year INTEGER,
	PRIMARY KEY (actor, current_year)
);