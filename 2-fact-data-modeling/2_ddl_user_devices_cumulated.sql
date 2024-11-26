CREATE TABLE user_devices_cumulated (
    user_id TEXT,
    device_activity_datelist JSONB,
    date DATE,
    PRIMARY KEY (user_id, date)
)