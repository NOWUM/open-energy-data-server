-- SPDX-FileCopyrightText: Florian Maurer, Christian Rieke
--
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Fill Values
INSERT INTO public.metadata (schema_name, tables, size)
SELECT 
    nspname AS schema_name, 
    COUNT(*) AS tables,
    SUM(pg_total_relation_size(pg_class.oid)) AS size
FROM pg_class
JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
WHERE nspname NOT IN ('pg_catalog', 'information_schema')
  AND pg_class.relkind = 'r'
  AND nspname NOT LIKE '%timescaledb%'
  AND nspname NOT LIKE '%pg_%'
  AND nspname  NOT LIKE 'pg_%'
  AND NOT EXISTS (
      SELECT 1 FROM public.metadata WHERE schema_name = nspname
  )
GROUP BY nspname;
