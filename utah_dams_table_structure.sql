
        CREATE TABLE IF NOT EXISTS utah_dams (
            id SERIAL PRIMARY KEY,
            "OBJECTID" INTEGER, "ID" TEXT, "DAMNUMBER" TEXT, "NAME" TEXT, "TYPE" TEXT, "NOTES" TEXT, "AREA" TEXT, "PERIMETER" TEXT, "Shape__Area" NUMERIC, "Shape__Length" NUMERIC,
            geom GEOMETRY(MULTIPOLYGON, 4326)
        );
        