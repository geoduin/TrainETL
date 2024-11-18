CREATE TABLE "Cause" (
                cause_id SERIAL PRIMARY KEY,
                cause VARCHAR(100) UNIQUE NOT NULL,
                statistical_cause VARCHAR(100),
                cause_group VARCHAR(100)
            )
CREATE TABLE "Dim_DateTime" (
                id BIGINT PRIMARY KEY,
                year SMALLINT,
                month SMALLINT,
                day SMALLINT,
                hour SMALLINT,
                minute SMALLINT
            );

CREATE TABLE "Disruptions" (
                rdt_id BIGINT PRIMARY KEY,
                duration_minutes INTEGER,
                cause VARCHAR(100),
                start_time BIGINT REFERENCES "Dim_DateTime",
                end_time BIGINT REFERENCES "Dim_DateTime",
                FOREIGN KEY (cause) REFERENCES "Cause" ON (cause)
            )