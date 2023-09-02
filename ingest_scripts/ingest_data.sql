DROP TABLE IF EXISTS raw.conversions;

CREATE TABLE IF NOT EXISTS raw.conversions
(
  userid BIGINT,
  registration_time      TIMESTAMP
);

COPY raw.conversions (userid, registration_time)
  FROM
  '/opt/source_data/conversions.csv' DELIMITER ',' CSV HEADER
  ;

DROP TABLE IF EXISTS raw.sessions;

CREATE TABLE IF NOT EXISTS raw.sessions
(
  userid BIGINT,
  time_started      TIMESTAMP,
  is_paid BOOLEAN,
  medium VARCHAR
);

COPY raw.sessions (userid, time_started,is_paid,medium)
  FROM
  '/opt/source_data/sessions.csv' DELIMITER ',' CSV HEADER
  ;