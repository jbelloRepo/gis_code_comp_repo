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
            
            # Extract all fields from attributes
            object_id = attributes.get('OBJECTID')
            watmain_id = attributes.get('WATMAINID')
            status = attributes.get('STATUS')
            pressure_zone = attributes.get('PRESSURE_ZONE')
            roadsegment_id = attributes.get('ROADSEGMENTID')
            map_label = attributes.get('MAP_LABEL')
            category = attributes.get('CATEGORY')
            pipe_size = attributes.get('PIPE_SIZE')
            material = attributes.get('MATERIAL')
            lined = attributes.get('LINED')
            lined_date = attributes.get('LINED_DATE')
            lined_material = attributes.get('LINED_MATERIAL')
            installation_date = attributes.get('INSTALLATION_DATE')
            acquisition = attributes.get('ACQUISITION')
            consultant = attributes.get('CONSULTANT')
            ownership = attributes.get('OWNERSHIP')
            bridge_main = attributes.get('BRIDGE_MAIN')
            bridge_details = attributes.get('BRIDGE_DETAILS')
            criticality = attributes.get('CRITICALITY')
            rel_cleaning_area = attributes.get('REL_CLEANING_AREA')
            rel_cleaning_subarea = attributes.get('REL_CLEANING_SUBAREA')
            undersized = attributes.get('UNDERSIZED')
            shallow_main = attributes.get('SHALLOW_MAIN')
            condition_score = attributes.get('CONDITION_SCORE')
            oversized = attributes.get('OVERSIZED')
            cleaned = attributes.get('CLEANED')
            shape_length = attributes.get('Shape__Length')

            # Convert geometry paths to GeoJSON format
            geojson = {
                "type": "LineString",
                "coordinates": geometry.get('paths', [[]])[0]
            }

            cursor.execute("""
                INSERT INTO water_mains (
                    city, dataset_type, object_id, watmain_id, status, pressure_zone,
                    roadsegment_id, map_label, category, pipe_size, material,
                    lined, lined_date, lined_material, installation_date, acquisition,
                    consultant, ownership, bridge_main, bridge_details, criticality,
                    rel_cleaning_area, rel_cleaning_subarea, undersized, shallow_main,
                    condition_score, oversized, cleaned, shape_length, geometry
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    CASE WHEN %s IS NOT NULL 
                         THEN timestamp 'epoch' + (%s/1000 * interval '1 second') 
                         ELSE NULL 
                    END,
                    %s,
                    CASE WHEN %s IS NOT NULL 
                         THEN timestamp 'epoch' + (%s/1000 * interval '1 second') 
                         ELSE NULL 
                    END,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    ST_GeomFromGeoJSON(%s)
                )
                ON CONFLICT (object_id) DO UPDATE 
                SET watmain_id = EXCLUDED.watmain_id,
                    status = EXCLUDED.status,
                    pressure_zone = EXCLUDED.pressure_zone,
                    roadsegment_id = EXCLUDED.roadsegment_id,
                    map_label = EXCLUDED.map_label,
                    category = EXCLUDED.category,
                    pipe_size = EXCLUDED.pipe_size,
                    material = EXCLUDED.material,
                    lined = EXCLUDED.lined,
                    lined_date = EXCLUDED.lined_date,
                    lined_material = EXCLUDED.lined_material,
                    installation_date = EXCLUDED.installation_date,
                    acquisition = EXCLUDED.acquisition,
                    consultant = EXCLUDED.consultant,
                    ownership = EXCLUDED.ownership,
                    bridge_main = EXCLUDED.bridge_main,
                    bridge_details = EXCLUDED.bridge_details,
                    criticality = EXCLUDED.criticality,
                    rel_cleaning_area = EXCLUDED.rel_cleaning_area,
                    rel_cleaning_subarea = EXCLUDED.rel_cleaning_subarea,
                    undersized = EXCLUDED.undersized,
                    shallow_main = EXCLUDED.shallow_main,
                    condition_score = EXCLUDED.condition_score,
                    oversized = EXCLUDED.oversized,
                    cleaned = EXCLUDED.cleaned,
                    shape_length = EXCLUDED.shape_length,
                    geometry = EXCLUDED.geometry;
            """, (
                city, dataset_type, object_id, watmain_id, status, pressure_zone,
                roadsegment_id, map_label, category, pipe_size, material,
                lined, lined_date, lined_date, lined_material, installation_date,
                installation_date, acquisition, consultant, ownership, bridge_main,
                bridge_details, criticality, rel_cleaning_area, rel_cleaning_subarea,
                undersized, shallow_main, condition_score, oversized, cleaned,
                shape_length, json.dumps(geojson)
            ))

        connection.commit()

    except Exception as e:
        if connection:
            connection.rollback()
        raise e

    finally:
        if connection:
            connection.close() 