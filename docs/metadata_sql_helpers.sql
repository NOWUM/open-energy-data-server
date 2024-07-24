-- Get the min and max date for each timestamp column in each schema
DO $$
DECLARE
    r RECORD;
    s RECORD;
    overall_min_date TIMESTAMP;
    overall_max_date TIMESTAMP;
    sql TEXT;
    min_date TIMESTAMP;
    max_date TIMESTAMP;
BEGIN
    -- Temp table
    DROP TABLE IF EXISTS temp_date_ranges;
    CREATE TEMP TABLE temp_date_ranges (
        schema_name TEXT,
        table_name TEXT,
        column_name TEXT,
        min_date TIMESTAMP,
        max_date TIMESTAMP
    );

    -- Loop over schemas
    FOR s IN
        SELECT schema_name
        FROM information_schema.schemata
        WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast', 'postgrest')
          AND schema_name NOT LIKE '%timescaledb%'
    LOOP
        overall_min_date := NULL;
        overall_max_date := NULL;

        -- Loop over timestamp columns in the current schema
        FOR r IN
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema = s.schema_name
            AND data_type IN ('timestamp without time zone', 'timestamp with time zone')
        LOOP
            --  MIN MAX Magic
            sql := format('SELECT MIN(%I) AS min_date, MAX(%I) AS max_date FROM %I.%I', r.column_name, r.column_name, s.schema_name, r.table_name);
            EXECUTE sql INTO min_date, max_date;

            IF overall_min_date IS NULL OR min_date < overall_min_date THEN
                overall_min_date := min_date;
            END IF;
            IF overall_max_date IS NULL OR max_date > overall_max_date THEN
                overall_max_date := max_date;
            END IF;

            INSERT INTO temp_date_ranges(schema_name, table_name, column_name, min_date, max_date)
            VALUES (s.schema_name, r.table_name, r.column_name, min_date, max_date);
        END LOOP;

        -- overall min/max
        IF overall_min_date IS NOT NULL AND overall_max_date IS NOT NULL THEN
            INSERT INTO temp_date_ranges(schema_name, table_name, column_name, min_date, max_date)
            VALUES (s.schema_name, 'OVERALL', 'OVERALL', overall_min_date, overall_max_date);
        END IF;
    END LOOP;
END $$;

SELECT * FROM temp_date_ranges;


-- Search for geometry field names
SELECT table_name, column_name
FROM information_schema.columns
WHERE ((column_name LIKE 'lat%')
	OR (column_name LIKE 'Lat%')
	OR (column_name LIKE 'laen%')
	OR (column_name LIKE 'Laen%')
    OR (column_name LIKE 'län%')
	OR (column_name LIKE 'Län%')
	OR (column_name LIKE 'Geo%')
	OR (column_name LIKE 'geo%')
	OR (column_name LIKE 'Land%')
	OR (column_name LIKE 'land%')
	OR (column_name LIKE 'Country%')
	OR (column_name LIKE 'country%')
    )
	and table_schema = 'mastr';

-- Bounding hull from lat lon points
SELECT
    ST_Transform(
        ST_ConcaveHull(
            ST_Collect(
                ST_SetSRID(
                    ST_MakePoint(lon, lat),
                    4326
                )
            ),
            0.5
        ),
        4326
    ) AS bounding_geometry
FROM
    e2watch.buildings;

-- Bounding hull from geometry
SELECT
    ST_Transform(ST_ConcaveHull(ST_Collect(ST_Points(ST_Simplify(geometry, 20))),0.5), 4326) AS bounding_geometry
FROM
    nrw_kwp_waermedichte.waermedichte;

-- Distinct geometries of nuts IDs into a bounding hull
WITH distinct_geometries AS (
    SELECT DISTINCT n.geometry
    FROM opsd_national_capacity.national_generation_capacity ONC
    JOIN public.nuts N ON ONC.country = N.nuts_id
    WHERE N.geometry IS NOT NULL
)

SELECT
    ST_Transform(
        ST_ConcaveHull(
            ST_Collect(
                ST_Points(
                    ST_Simplify(geometry, 20)
                )
            ), 0.5
        ), 4326
    ) AS bounding_geometry
FROM distinct_geometries;



-- Map Nuts data to column names of a table
WITH country_nuts AS (
    SELECT
        cntr_code,
        nuts_id,
        latitude,
        longitude,
        geometry
    FROM
        public.nuts
    WHERE
        levl_code = 0
)

, ninja_country_codes AS (
    SELECT
        column_name AS country_code
    FROM
        information_schema.columns
    WHERE
        table_schema = 'ninja'
        AND table_name = 'capacity_solar_merra2'
        AND column_name ~ '^[a-z]{2}$'
)

, joined_data AS (
    SELECT
        ncc.country_code,
        cn.nuts_id,
        cn.latitude,
        cn.longitude,
        cn.geometry
    FROM
        ninja_country_codes ncc
        LEFT JOIN country_nuts cn ON   Upper(ncc.country_code) = cn.cntr_code
)

SELECT
    country_code,
    nuts_id,
    latitude,
    longitude,
    geometry
FROM
    joined_data
ORDER BY
    nuts_id IS NULL,
    country_code;
