CREATE TABLE actors_history_scd (
    actorid TEXT,
	actor TEXT,
	quality_class quality_class,
	is_active BOOLEAN,
	current_year INTEGER,
	start_year INTEGER,
	end_year INTEGER,
	PRIMARY KEY (actorid, start_year)
);
