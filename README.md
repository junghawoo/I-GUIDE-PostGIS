# I-GUIDE

**Geospatial Risk Analysis Using PostGIS**

This repository contains tools developed for analyzing flood risks associated with dam failures, using geospatial data and spatial databases.  
It documents work completed as part of my Research Software Engineer Internship at the [Rosen Center for Advanced Computing](https://www.rcac.purdue.edu/) (Feb '25 – May '25).

The project focuses on setting up a spatial database, ingesting real-world location data, and running geospatial queries to identify critical infrastructure (such as power plants) at risk within dam inundation zones — the areas that would flood if a dam were to fail.

---

## Project Overview

This project provides infrastructure and scripts for:
1. Setting up a PostGIS-enabled database (PostGIS = PostgreSQL + spatial extensions)
2. Ingesting and managing geospatial data (GeoJSON files)
3. Analyzing spatial relationships to assess flood risk

> **Key Terms:**  
> - **PostGIS**: An extension for PostgreSQL that supports geographic objects and spatial queries.  
> - **GeoJSON**: A format for encoding geographic data structures.  
> - **Inundation Zone**: An area predicted to flood if a dam fails.

---

## Repository Structure

| File/Folder | Description |
|:------------|:------------|
| `ingest.py` | Script for loading GeoJSON files into PostGIS tables |
| `list_tables.py` | Utility to list tables within the PostGIS database |
| `main.py` | Simple script to test database connection |
| `query_1.ipynb` | Notebook analyzing power plants at risk from dam inundation |
| `query_1.py` | Python script version of the notebook analysis |
| `query_examples.py` | Additional query examples |
| `data/` | Sample GeoJSON datasets for testing |
| ├── `power_plants.geojson` | U.S. power plant locations |
| └── `utah_dams_inundation.geojson` | Utah dam inundation zones |

---

## Setup Instructions

### 1. Install the PostGIS Database (Using Docker)

```bash
# Pull the PostGIS Docker image
docker pull postgis/postgis:17-3.5

# Run the PostGIS container
docker run -d \
  --name postgis \
  -e POSTGRES_USER=admin \
  -e POSTGRES_PASSWORD=admin \
  -e POSTGRES_DB=gisdb \
  -p 5432:5432 \
  -v postgis-data:/var/lib/postgresql/data \
  postgis/postgis:17-3.5
```

To verify that PostGIS is installed correctly:
```bash
# Open a psql shell inside the running container
docker exec -it postgis psql -U admin -d gisdb

# At the psql prompt, list installed extensions:
\dx
```

### 2. Set Up a Jupyter Environment (Using The Littlest JupyterHub)

> **Why TLJH?**  
> TLJH provides a centralized environment for running Jupyter notebooks, making it easier to manage scripts and analysis in one place.

1. Install WSL (Windows Subsystem for Linux) if not already installed:
```bash
wsl --install
```

2. Install TLJH inside your WSL Ubuntu environment:
```bash
sudo apt update
sudo apt install python3 python3-dev git curl
curl -L https://tljh.jupyter.org/bootstrap.py | sudo -E python3 - --admin <your-admin-username>
```

3. Ensure systemd is enabled for TLJH:
Edit or create `/etc/wsl.conf` with:
```
[boot]
systemd = 1
```

4. Restart WSL, then access JupyterHub at http://localhost and set a password for your admin user.

---

### 3. Import Geospatial Data into PostGIS

This project uses [`uv`](https://github.com/astral-sh/uv), a fast Python package installer and runner, for managing dependencies.

Load GeoJSON data into your PostGIS database:
```bash
uv run ingest.py
```
Follow the prompts to select GeoJSON files and specify table names.

---

## Use Case: Dam Inundation Risk Analysis

The primary analysis in `query_1.ipynb` focuses on:
1. Identifying vulnerable entities (e.g., power plants) within dam inundation zones
2. Counting the number of affected entities per dam

**To run the analysis:**
- Open `query_1.ipynb` in JupyterHub
- Execute cells sequentially
- When prompted, enter a specific dam name or type `"ALL"` to analyze all dams

---

## Future Enhancements

This project was paused due to funding constraints, but potential future work includes:
- Expanding analysis to other vulnerable infrastructure (hospitals, schools, etc.)
- Building an interactive web interface for visualizing risk zones
- Extending coverage beyond Utah to nationwide dam data
- Implementing advanced risk modeling techniques

---

## Requirements

- Python 3.11 or higher  
- PostgreSQL with PostGIS extension  
- Docker (for setting up PostGIS easily)  
- [`uv`](https://github.com/astral-sh/uv) package manager

---

## Contact

For any questions or collaboration inquiries, feel free to reach out:  
**Email**: aniruddh [at] noir [dot] ac