DROP FUNCTION IF EXISTS public.opsd_national_generation_year_country;

DROP TYPE IF EXISTS opsd_national_generation_year_country;

CREATE TYPE opsd_national_generation_year_country AS (
    year bigint,
    country text,
    technology text,
    total_production double precision
);

CREATE OR REPLACE FUNCTION public.opsd_national_generation_year_country()
RETURNS SETOF opsd_national_generation_year_country AS $$
BEGIN
    RETURN QUERY
    SELECT
        year,
        country,
        technology,
        SUM(capacity) AS total_production
    FROM 
        opsd.national_generation_capacity
    GROUP BY
        year,
        country,
        technology
    ORDER BY
        year,
        country,
        technology;
END;
$$ LANGUAGE plpgsql STABLE;
NOTIFY pgrst, 'reload schema';
