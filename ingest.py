import psycopg2
import json
import os
from pprint import pprint

# Connection parameters
db_params = {
    "dbname": "gisdb",
    "user": "admin",
    "password": "admin",
    "host": "localhost",
    "port": 5432
}

def examine_geojson(file_path):
    """Examine the structure of a GeoJSON file and return details."""
    try:
        # Try with utf-8 encoding first, then fall back to other encodings
        encodings_to_try = ['utf-8', 'latin-1', 'cp1252']
        
        for encoding in encodings_to_try:
            try:
                with open(file_path, 'r', encoding=encoding) as f:
                    data = json.load(f)
                break
            except UnicodeDecodeError:
                if encoding == encodings_to_try[-1]:
                    raise Exception(f"Could not decode file with any of these encodings: {encodings_to_try}")
                continue
        
        feature_count = len(data.get('features', []))
        geometry_types = set()
        properties = None
        
        if feature_count > 0:
            first_feature = data['features'][0]
            properties = first_feature.get('properties', {})
            
            for feature in data['features']:
                if 'geometry' in feature and 'type' in feature['geometry']:
                    geometry_types.add(feature['geometry']['type'])
        
        geometry_type = None
        if 'MultiPolygon' in geometry_types or ('Polygon' in geometry_types and 'MultiPolygon' in geometry_types):
            geometry_type = 'MultiPolygon'
        elif 'MultiLineString' in geometry_types or ('LineString' in geometry_types and 'MultiLineString' in geometry_types):
            geometry_type = 'MultiLineString'
        elif 'MultiPoint' in geometry_types or ('Point' in geometry_types and 'MultiPoint' in geometry_types):
            geometry_type = 'MultiPoint'
        elif geometry_types:
            geometry_type = next(iter(geometry_types))
        
        print("\nGeoJSON Analysis:")
        print(f"Total features: {feature_count}")
        print(f"Geometry types found: {', '.join(geometry_types)}")
        print(f"Selected geometry type for table: {geometry_type}")
        print("Properties structure:")
        pprint(properties)
        
        return {
            'feature_count': feature_count,
            'geometry_type': geometry_type,
            'properties': properties,
            'raw_data': data
        }
    except Exception as e:
        print(f"Error examining GeoJSON: {e}")
        return None

def ingest_geojson_to_postgis(file_path, table_name, conn, source_srid=None, target_srid=4326):
    """Ingest GeoJSON data into a new PostGIS table with coordinate system handling."""
    try:
        table_name = table_name.replace('-', '_')
        
        geojson_info = examine_geojson(file_path)
        
        if not geojson_info:
            return False
        
        cursor = conn.cursor()
        
        properties = geojson_info['properties']
        geometry_type = geojson_info['geometry_type']
        
        postgis_type_map = {
            'Point': 'POINT',
            'LineString': 'LINESTRING',
            'Polygon': 'POLYGON',
            'MultiPoint': 'MULTIPOINT',
            'MultiLineString': 'MULTILINESTRING',
            'MultiPolygon': 'MULTIPOLYGON'
        }
        
        postgis_type = postgis_type_map.get(geometry_type, 'GEOMETRY')
        
        if source_srid is None:
            if 'crs' in geojson_info['raw_data']:
                crs = geojson_info['raw_data']['crs']
                if crs.get('type') == 'name' and 'properties' in crs:
                    name = crs['properties'].get('name', '')
                    if 'EPSG' in name:
                        try:
                            source_srid = int(name.split(':')[-1])
                            print(f"Detected source SRID: {source_srid}")
                        except ValueError:
                            pass
            
            if source_srid is None:
                source_srid = 4326
                print(f"No CRS specified in GeoJSON. Assuming EPSG:{source_srid}")
        
        columns = []
        for prop_name, prop_value in properties.items():
            col_type = "TEXT"
            if isinstance(prop_value, int):
                col_type = "INTEGER"
            elif isinstance(prop_value, float):
                col_type = "NUMERIC"
            elif isinstance(prop_value, bool):
                col_type = "BOOLEAN"
                
            columns.append(f"\"{prop_name}\" {col_type}")
        
        columns_sql = ", ".join(columns)
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            {columns_sql},
            geom GEOMETRY({postgis_type}, {target_srid})
        );
        """
        
        print("\nProposed table structure:")
        print(create_table_sql)
        print(f"Source SRID: {source_srid}, Target SRID: {target_srid}")

        with open(f"{table_name}_table_structure.sql", "w") as f:
            f.write(create_table_sql)
        
        proceed = input("\nDo you want to create the table and insert data? (yes/no): ").lower()
        
        if proceed not in ["yes", "y"]:
            print("Operation cancelled by user.")
            return False
        
        cursor.execute(create_table_sql)
        
        features = geojson_info['raw_data']['features']
        print(f"\nInserting {len(features)} features into the database...")
        
        total = len(features)
        inserted = 0
        
        for feature in features:
            feature_props = feature.get('properties', {})
            
            prop_names = [f"\"{name}\"" for name in feature_props.keys()]
            placeholders = ["%s"] * len(prop_names)
            
            prop_values = list(feature_props.values())
            
            geometry_json = json.dumps(feature.get('geometry', {}))
            
            insert_sql = f"""
            INSERT INTO {table_name} (
                {', '.join(prop_names)},
                geom
            ) VALUES (
                {', '.join(placeholders)},
                ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s), {source_srid}), {target_srid})
            );
            """
            
            try:
                cursor.execute(insert_sql, prop_values + [geometry_json])
                inserted += 1
                
                if inserted % 100 == 0 or inserted == total:
                    print(f"Progress: {inserted}/{total} features inserted ({int(inserted/total*100)}%)")
                
            except Exception as e:
                print(f"Error inserting feature: {e}")
                continue
        
        conn.commit()
        print(f"\nCompleted: {inserted} out of {total} features inserted successfully.")
        print(f"Data was automatically transformed from EPSG:{source_srid} to EPSG:{target_srid}")
        
        return True
        
    except Exception as e:
        print(f"Error ingesting GeoJSON: {e}")
        return False

# Connect to the database
try:
    conn = psycopg2.connect(**db_params)
    
    # Check PostGIS version
    cursor = conn.cursor()
    cursor.execute("SELECT postgis_version();")
    version = cursor.fetchone()
    print(f"Connected to PostGIS version: {version[0]}")
    
    # Example usage:
    geojson_file = input("Enter the path to your GeoJSON file: ")
    if os.path.exists(geojson_file):
        table_name = input("Enter the name for the PostGIS table: ")
        
        # Get coordinate system information
        source_srid = input("Enter source SRID (press Enter for auto-detection): ")
        source_srid = int(source_srid) if source_srid and source_srid.isdigit() else None
        
        target_srid = input("Enter target SRID (press Enter for 4326/WGS84): ")
        target_srid = int(target_srid) if target_srid and target_srid.isdigit() else 4326
        
        ingest_geojson_to_postgis(geojson_file, table_name, conn, source_srid, target_srid)
    else:
        print(f"File not found: {geojson_file}")
    
    # Close connection
    cursor.close()
    conn.close()
except Exception as e:
    print(f"Error: {e}")