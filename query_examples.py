import psycopg2
import os
from datetime import datetime
import pandas as pd
from tabulate import tabulate  # Import the function directly

# Connection parameters
db_params = {
    "dbname": "gisdb",
    "user": "admin",
    "password": "admin",
    "host": "localhost",
    "port": 5432
} 

def run_query(conn, query, description, save_results=True, limit_display=10):
    """Run a query and display/save results"""
    cursor = conn.cursor()
    
    print(f"\n=== {description} ===")
    print(f"SQL: {query}")
    
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        
        # Create DataFrame
        df = pd.DataFrame(results, columns=column_names)
        
        # Display results
        if len(df) > 0:
            print(f"\nResults ({len(df)} rows, showing first {min(limit_display, len(df))}):")
            print(tabulate(df.head(limit_display), headers='keys', tablefmt='psql', showindex=False))
        else:
            print("No results returned.")
        
        # Save results if requested
        if save_results and len(df) > 0:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            # Create a safe filename
            safe_desc = "".join(c if c.isalnum() else "_" for c in description)
            filename = f"results_{safe_desc}_{timestamp}.csv"
            df.to_csv(filename, index=False)
            print(f"Results saved to: {filename}")
            
        return df
    
    except Exception as e:
        print(f"Error executing query: {e}")
        return None
    finally:
        cursor.close()

def run_power_plant_queries(table_name="power_plants"):
    """Run a series of example queries on the power plants data"""
    try:
        conn = psycopg2.connect(**db_params)
        
        # First, let's check the actual column names to avoid errors
        column_query = f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = '{table_name}'
        ORDER BY ordinal_position;
        """
        
        columns_df = run_query(conn, column_query, "Table Columns", save_results=True)
        
        if columns_df is None or len(columns_df) == 0:
            print(f"Error: Could not retrieve columns for table '{table_name}'")
            return
            
        # Get the actual column names from the database to use in our queries
        column_names = columns_df['column_name'].tolist()
        
        # Find capacity-related column (might be named differently)
        capacity_col = None
        for col in column_names:
            if 'capacity' in col.lower() or 'mw' in col.lower():
                capacity_col = col
                print(f"Found capacity column: {capacity_col}")
                break
                
        # Find name-related column
        name_col = None
        for col in column_names:
            if col.lower() in ['name', 'plant_name', 'station_name']:
                name_col = col
                print(f"Found name column: {name_col}")
                break
        
        # Find type-related column
        type_col = None
        for col in column_names:
            if col.lower() in ['type', 'plant_type', 'station_type']:
                type_col = col
                print(f"Found type column: {type_col}")
                break
        
        # Basic queries
        basic_query = f"""
        SELECT * FROM {table_name}
        LIMIT 10;
        """
        run_query(conn, basic_query, "Basic Sample of Data")
        
        # Only run the type-based query if we found a type column
        if type_col:
            count_by_type_query = f"""
            SELECT "{type_col}", COUNT(*) as count
            FROM {table_name}
            GROUP BY "{type_col}"
            ORDER BY count DESC;
            """
            run_query(conn, count_by_type_query, "Power Plants by Type")
        
        # Only run the capacity query if we found name, type, and capacity columns
        if name_col and type_col and capacity_col:
            capacity_query = f"""
            SELECT "{name_col}", "{type_col}", "{capacity_col}" as capacity
            FROM {table_name}
            WHERE "{capacity_col}" IS NOT NULL
            ORDER BY "{capacity_col}" DESC
            LIMIT 20;
            """
            run_query(conn, capacity_query, "Largest Capacity Power Plants")
        
        # Spatial query - find plants within a radius of a point
        # Example: Plants within 100km of Chicago
        chicago_query = f"""
        SELECT 
            {', '.join([f'"{col}"' for col in column_names if col != 'geom'])},
            ST_Distance(
                geom,
                ST_SetSRID(ST_MakePoint(-87.6298, 41.8781), 4326)::geography
            )/1000 as distance_km
        FROM {table_name}
        WHERE ST_DWithin(
            geom,
            ST_SetSRID(ST_MakePoint(-87.6298, 41.8781), 4326)::geography,
            100000  -- 100km in meters
        )
        ORDER BY distance_km
        LIMIT 100;
        """
        run_query(conn, chicago_query, "Power Plants within 100km of Chicago")
        
        # Check if states table exists before running the query
        check_states = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'states'
        );
        """
        states_check = run_query(conn, check_states, "Check if states table exists", save_results=False)
        
        if states_check is not None and states_check.iloc[0, 0]:
            # Spatial aggregation - count plants by state using spatial join
            state_query = f"""
            SELECT states.name as state_name, COUNT(*) as plant_count
            FROM {table_name} plants
            JOIN states ON ST_Contains(states.geom, plants.geom)
            GROUP BY states.name
            ORDER BY plant_count DESC;
            """
            run_query(conn, state_query, "Power Plants Count by State")
        else:
            print("\n=== Power Plants Count by State ===")
            print("Skipped - 'states' table doesn't exist.")
            print("To run this query, you would need to import state boundaries.")
        
        # Simple data aggregation - counts by bounding box quadrants
        quadrants_query = f"""
        WITH bounds AS (
            SELECT 
                ST_XMin(ST_Extent(geom)) as min_lon,
                ST_XMax(ST_Extent(geom)) as max_lon,
                ST_YMin(ST_Extent(geom)) as min_lat,
                ST_YMax(ST_Extent(geom)) as max_lat
            FROM {table_name}
        ),
        quadrants AS (
            SELECT 
                CASE 
                    WHEN ST_X(geom) < (SELECT (min_lon + max_lon)/2 FROM bounds) THEN 'West'
                    ELSE 'East'
                END as longitude_half,
                CASE 
                    WHEN ST_Y(geom) < (SELECT (min_lat + max_lat)/2 FROM bounds) THEN 'South'
                    ELSE 'North'
                END as latitude_half
            FROM {table_name}
        )
        SELECT 
            longitude_half || ' ' || latitude_half as quadrant,
            COUNT(*) as plant_count
        FROM quadrants
        GROUP BY quadrant
        ORDER BY plant_count DESC;
        """
        run_query(conn, quadrants_query, "Power Plants by Geographic Quadrant")
        
        # Distance analysis - nearest neighbors
        if name_col and type_col:
            # Find plants with 'nuclear' in their type
            nuclear_check = f"""
            SELECT COUNT(*) 
            FROM {table_name}
            WHERE "{type_col}" ILIKE '%nuclear%';
            """
            nuclear_result = run_query(conn, nuclear_check, "Check for nuclear plants", save_results=False)
            
            if nuclear_result is not None and nuclear_result.iloc[0, 0] > 0:
                nearest_query = f"""
                WITH nuclear_plants AS (
                    SELECT id, "{name_col}", geom
                    FROM {table_name}
                    WHERE "{type_col}" ILIKE '%nuclear%'
                    LIMIT 20 -- For performance
                )
                SELECT 
                    np."{name_col}" as nuclear_plant,
                    p."{name_col}" as nearest_plant,
                    p."{type_col}" as plant_type,
                    ST_Distance(np.geom, p.geom::geography)/1000 as distance_km
                FROM nuclear_plants np
                CROSS JOIN LATERAL (
                    SELECT "{name_col}", "{type_col}", geom
                    FROM {table_name}
                    WHERE "{type_col}" NOT ILIKE '%nuclear%'
                    ORDER BY np.geom <-> geom
                    LIMIT 1
                ) p
                ORDER BY distance_km
                LIMIT 20;
                """
                run_query(conn, nearest_query, "Nearest Non-Nuclear Plant to Each Nuclear Plant")
        
        # GeoJSON Export for mapping visualization
        if name_col:
            cols_to_include = [col for col in [name_col, type_col, capacity_col] if col is not None]
            properties_json = ", ".join([f"'{col}', \"{col}\"" for col in cols_to_include])
            
            geojson_query = f"""
            SELECT json_build_object(
                'type', 'FeatureCollection',
                'features', json_agg(
                    json_build_object(
                        'type', 'Feature',
                        'geometry', ST_AsGeoJSON(geom)::json,
                        'properties', json_build_object(
                            'id', id,
                            {properties_json}
                        )
                    )
                )
            ) as geojson
            FROM (
                SELECT id, {', '.join([f'"{col}"' for col in cols_to_include])}, geom
                FROM {table_name}
                LIMIT 100  -- Limit for performance
            ) sub;
            """
            run_query(conn, geojson_query, "GeoJSON Export Sample", limit_display=1)
        
        print("\n\nAll queries completed. Results have been saved to CSV files.")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    # Install required packages if needed
    try:
        import pandas as pd
        from tabulate import tabulate  # Corrected import
    except ImportError:
        print("Installing required packages...")
        os.system("pip install pandas tabulate")
        import pandas as pd
        from tabulate import tabulate
    
    table_name = input("Enter the power plants table name (default: power_plants): ")
    if not table_name:
        table_name = "power_plants"
    
    run_power_plant_queries(table_name)