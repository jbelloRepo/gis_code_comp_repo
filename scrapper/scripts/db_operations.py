import psycopg2
from db_conn_config import get_db_connection

def update_water_mains_data(city, dataset_type, data):
    connection = get_db_connection()
    cursor = connection.cursor()

    for feature in data:
        attributes = feature.get("attributes", {})
        geometry = feature.get("geometry", {})
        
        # Extract relevant fields
        object_id = attributes.get('OBJECTID')
        status = attributes.get('STATUS')
        pipe_size = attributes.get('PIPE_SIZE')
        material = attributes.get('MATERIAL')
        installation_date = attributes.get('INSTALLATION_DATE')
        pressure_zone = attributes.get('PRESSURE_ZONE')
        geometry_paths = json.dumps(geometry.get('paths', []))  # Convert geometry paths to JSON

        # Insert or update the data in the database
        cursor.execute("""
            INSERT INTO water_mains (city, dataset_type, object_id, status, pipe_size, material, installation_date, pressure_zone, geometry)
            VALUES (%s, %s, %s, %s, %s, %s, to_timestamp(%s / 1000), %s, %s)
            ON CONFLICT (object_id) DO UPDATE 
            SET status = EXCLUDED.status,
                pipe_size = EXCLUDED.pipe_size,
                material = EXCLUDED.material,
                installation_date = EXCLUDED.installation_date,
                pressure_zone = EXCLUDED.pressure_zone,
                geometry = EXCLUDED.geometry;
        """, (city, dataset_type, object_id, status, pipe_size, material, installation_date, pressure_zone, geometry_paths))

    connection.commit()
    cursor.close()
    connection.close() 