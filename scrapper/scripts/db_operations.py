import psycopg2
from psycopg2.extras import Json
from db_conn_config import get_db_connection
import json

def update_water_mains_data(city, dataset_type, data):
    """Update water mains data in the database."""
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        for feature in data.get('features', []):
            attributes = feature.get("attributes", {})
            geometry = feature.get("geometry", {})
            
            # Extract relevant fields
            object_id = attributes.get('OBJECTID')
            status = attributes.get('STATUS')
            pipe_size = attributes.get('PIPE_SIZE')
            material = attributes.get('MATERIAL')
            installation_date = attributes.get('INSTALLATION_DATE')
            pressure_zone = attributes.get('PRESSURE_ZONE')
            
            # Convert geometry paths to GeoJSON format
            geojson = {
                "type": "LineString",
                "coordinates": geometry.get('paths', [[]])[0]
            }

            cursor.execute("""
                INSERT INTO water_mains (
                    city, dataset_type, object_id, status, pipe_size, 
                    material, installation_date, pressure_zone, geometry
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, 
                    CASE WHEN %s IS NOT NULL THEN to_timestamp(%s / 1000) ELSE NULL END,
                    %s, ST_GeomFromGeoJSON(%s)
                )
                ON CONFLICT (object_id) DO UPDATE 
                SET status = EXCLUDED.status,
                    pipe_size = EXCLUDED.pipe_size,
                    material = EXCLUDED.material,
                    installation_date = EXCLUDED.installation_date,
                    pressure_zone = EXCLUDED.pressure_zone,
                    geometry = EXCLUDED.geometry;
            """, (
                city, dataset_type, object_id, status, pipe_size, 
                material, installation_date, installation_date,
                pressure_zone, json.dumps(geojson)
            ))

        connection.commit()

    except Exception as e:
        if connection:
            connection.rollback()
        raise e

    finally:
        if connection:
            connection.close() 