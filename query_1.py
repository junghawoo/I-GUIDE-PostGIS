import psycopg2
from tabulate import tabulate

# ---------------------------------------------------------------------------
# Database configuration
# ---------------------------------------------------------------------------

db_params = {
    "dbname": "gisdb",
    "user": "admin",
    "password": "admin",
    "host": "localhost",
    "port": 5432,
}

def get_actual_column_names(conn, table, schema="public"):
    """Return the case-sensitive column names for schema.table"""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            """,
            (schema, table),
        )
        return [r[0] for r in cur.fetchall()]

def connect_to_db():
    try:
        conn = psycopg2.connect(**db_params)
        print("Connected to PostgreSQL database.")
        return conn
    except Exception as e:
        print("Error connecting to database:", e)
        return None

def list_available_dams(conn, limit=50):
    """Return dams sorted by inundation-polygon area (largest first)."""
    with conn.cursor() as cur:
        dam_cols = get_actual_column_names(conn, "utah_dams")
        name_col = next(c for c in dam_cols if c.upper() == "NAME")
        type_col = next(c for c in dam_cols if c.upper() == "TYPE")
        geom_col = "geom"

        cur.execute(
            f"""
            SELECT "{name_col}", "{type_col}",
                   ST_Area("{geom_col}"::geography) / 1e6 AS area_sq_km
            FROM utah_dams
            ORDER BY area_sq_km DESC
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()

    headers = ["Dam Name", "Type", "Inundation Area (sq km)"]
    print("\nTop", limit, "Utah dams by inundation area:")
    print(tabulate([[r[0], r[1], round(r[2], 2)] for r in rows], headers, tablefmt="grid"))
    return rows  # [(name, type, area)]


def analyze_power_plants_at_risk(conn, dam_name):
    """Print and return the power plants whose points fall within *dam_name* polygon."""
    with conn.cursor() as cur:
        # Column discovery – dams
        dam_cols = get_actual_column_names(conn, "utah_dams")
        name_col_dam = next(c for c in dam_cols if c.upper() == "NAME")
        geom_col_dam = "geom"

        # Column discovery – power plants
        pp_cols = get_actual_column_names(conn, "power_plants")
        name_col_pp = next(c for c in pp_cols if c.upper() == "NAME")
        type_col_pp = next(c for c in pp_cols if c.upper() == "TYPE")
        fuel_col = next(c for c in pp_cols if c.upper() == "PRIM_FUEL")
        cap_col = next(c for c in pp_cols if c.upper() == "SUMMER_CAP")
        geom_col_pp = "geom"

        # Spatial query – intersects rather than contains (safer for points)
        cur.execute(
            f"""
            SELECT p."{name_col_pp}", p."{type_col_pp}", p."{fuel_col}", p."{cap_col}"
            FROM   utah_dams d
            JOIN   power_plants p
              ON   ST_Intersects(d."{geom_col_dam}", p."{geom_col_pp}")
            WHERE  d."{name_col_dam}" = %s
            ORDER  BY p."{cap_col}" DESC NULLS LAST
            """,
            (dam_name,),
        )
        rows = cur.fetchall()

    print("\n--- Power Plants at Risk ---")
    if rows:
        headers = ["Plant Name", "Type", "Primary Fuel", "Capacity (MW)"]
        print(tabulate(rows, headers, tablefmt="grid"))
    else:
        print(f"No power plants found within the inundation zone of '{dam_name}'.")
    return rows


def main():
    conn = connect_to_db()
    assert conn is not None, "Database connection failed."

    try:
        dams = list_available_dams(conn)
        print(
            "\nEnter a dam name to analyze or type 'ALL' for every dam"
        )
        selected = input("Dam name: ").strip()

        if not selected:
            selected = dams[0][0]  # largest dam by area

        if selected.upper() == "ALL":
            dam_names = [d[0] for d in dams]
        else:
            dam_names = [selected]

        for dam in dam_names:
            print("\n" + "=" * 60)
            print(f"POWER-PLANT RISK REPORT FOR {dam}")
            print("=" * 60)
            plants = analyze_power_plants_at_risk(conn, dam)
            print(
                f"\nTotal plants at risk: {len(plants)}\n"
            )

    except Exception as e:
        print("Error during analysis:", e)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
