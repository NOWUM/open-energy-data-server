from sqlalchemy import create_engine, text

from .config import db_uri

class BaseCrawler:
    def __init__(self, schema_name):
        self.engine = create_engine(db_uri(schema_name))
        self.create_schema(schema_name)
        
    def create_schema(self, schema_name):
        create_schema_only(self.engine, schema_name)

    def set_metadata(self, metadata_info):
        set_metadata_only(self.engine, metadata_info)

def create_schema_only(engine, schema_name):
    with engine.begin() as conn:
        conn.execute(
            text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        )

        
def set_metadata_only(engine, metadata_info):
    with engine.begin() as conn:
        conn.execute(
            text("""
            INSERT INTO public.metadata 
            (schema_name, data_date, data_source, licence, description, contact, concave_hull_geometry, temporal_start, temporal_end)
            VALUES 
            (:schema_name, :data_date, :data_source, :licence, :description, :contact, :concave_hull_geometry, :temporal_start, :temporal_end)
            ON CONFLICT (schema_name) DO UPDATE SET
                data_date = EXCLUDED.data_date,
                data_source = EXCLUDED.data_source,
                licence = EXCLUDED.licence,
                description = EXCLUDED.description,
                contact = EXCLUDED.contact,
                concave_hull_geometry = EXCLUDED.concave_hull_geometry,
                temporal_start = EXCLUDED.temporal_start,
                temporal_end = EXCLUDED.temporal_end
            """),
            metadata_info
        )
        conn.execute(
            text("""
            UPDATE public.metadata
            SET tables = (SELECT COUNT(*) FROM pg_class JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace WHERE nspname = :schema_name AND pg_class.relkind = 'r'),
                size = (SELECT SUM(pg_total_relation_size(pg_class.oid)) FROM pg_class JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace WHERE nspname = :schema_name AND pg_class.relkind = 'r'),
                crawl_date = NOW()
            WHERE schema_name = :schema_name
            """),
            {"schema_name": metadata_info["schema_name"]}
        )