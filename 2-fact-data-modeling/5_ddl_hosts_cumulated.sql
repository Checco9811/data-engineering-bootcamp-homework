CREATE TABLE hosts_cumulated  (
    host TEXT,
    host_activity_datelist TIMESTAMP[],
    date DATE,
    PRIMARY KEY (host, date)
)
