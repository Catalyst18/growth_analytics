CREATE TABLE IF NOT EXISTS brenntag_star.public.region
(
    region_id integer,
    region_name varchar,
    description varchar

);

COPY brenntag_star.public.region(region_id,region_name,description)
FROM '/opt/source_data/region.tbl' DELIMITER '|', CSV;

CREATE TABLE IF NOT EXISTS brenntag_star.public.nation
(
    nation_id integer,
    country_name varchar,
    region_id integer,
    description varchar

);