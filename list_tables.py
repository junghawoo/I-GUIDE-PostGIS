import psycopg2
from tabulate import tabulate

# Connection parameters - using the same as in ingest.py
db_params = {
    "dbname": "gisdb",
    "user": "admin",
    "password": "admin",
    "host": "localhost",
    "port": 5432
}

def list_tables():
    """List all tables in the database and their schemas."""
    try:
        # Connect to the database
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        print("Connected to PostgreSQL database.")
        
        # Query to get all user tables
        cursor.execute("""
            SELECT 
                table_schema, 
                table_name 
            FROM 
                information_schema.tables 
            WHERE 
                table_schema NOT IN ('pg_catalog', 'information_schema') 
                AND table_type = 'BASE TABLE'
            ORDER BY 
                table_schema, table_name;
        """)
        
        tables = cursor.fetchall()
        
        if not tables:
            print("No tables found in the database.")
            return
        
        print(f"\nFound {len(tables)} tables in the database:")
        print(tabulate(tables, headers=["Schema", "Table Name"], tablefmt="grid"))
        
        # Ask if user wants to see detailed information about a specific table
        table_info = input("\nEnter a table name to see its schema (or press Enter to skip): ")
        
        if table_info:
            # Get column information for the selected table
            cursor.execute("""
                SELECT 
                    column_name, 
                    data_type, 
                    character_maximum_length,
                    is_nullable
                FROM 
                    information_schema.columns 
                WHERE 
                    table_name = %s
                ORDER BY 
                    ordinal_position;
            """, (table_info,))
            
            columns = cursor.fetchall()
            
            if columns:
                # Format the column information for better readability
                formatted_columns = []
                for col in columns:
                    name, dtype, max_length, nullable = col
                    type_info = dtype
                    if max_length:
                        type_info += f"({max_length})"
                    formatted_columns.append([name, type_info, "YES" if nullable == "YES" else "NO"])
                
                print(f"\nSchema for table '{table_info}':")
                print(tabulate(formatted_columns, headers=["Column", "Data Type", "Nullable"], tablefmt="grid"))
                
                # Get geometry information if it's a spatial table
                cursor.execute("""
                    SELECT 
                        f_geometry_column, 
                        type, 
                        srid,
                        coord_dimension
                    FROM 
                        geometry_columns 
                    WHERE 
                        f_table_name = %s;
                """, (table_info,))
                
                geometry_info = cursor.fetchall()
                
                if geometry_info:
                    print("\nGeometry Information:")
                    print(tabulate(geometry_info, 
                                   headers=["Geometry Column", "Type", "SRID", "Dimensions"], 
                                   tablefmt="grid"))
                    
                # Get row count
                cursor.execute(f"SELECT COUNT(*) FROM {table_info};")
                row_count = cursor.fetchone()[0]
                print(f"\nTotal rows: {row_count}")
            else:
                print(f"Table '{table_info}' not found or has no columns.")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")
        return

if __name__ == "__main__":
    list_tables()