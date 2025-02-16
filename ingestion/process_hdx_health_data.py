import geopandas as gpd
import pandas as pd
import pyarrow.parquet as pq

# Function to load shapefiles and add osm_type column
def load_health_data(file_path, osm_type, encoding="utf-8"):
    """ Load Health facility shapefile and assign an OSM type."""
    try:
        gdf = gpd.read_file(file_path, encoding=encoding)
    except UnicodeDecodeError:
        print(f"⚠️ UTF-8 decoding failed for {file_path}. Trying ISO-8859-1...")
        gdf = gpd.read_file(file_path, encoding="ISO-8859-1")

    gdf["osm_type"] = osm_type
    return gdf

## Load USA Health Data

us_health_node_raw = load_health_data("./data-raw/hdx/United States/United States-node.shp", "node")
us_health_way_raw = load_health_data("./data-raw/hdx/United States/United States-node.shp", "way")

# Combine both USA datasets hdx
us_health_combined = pd.concat([us_health_node_raw, us_health_way_raw], ignore_index=True)

# Rename columns
us_health_data = us_health_combined.rename(columns={
    "operator_t": "operator_type",
    "operationa": "operational",
    "contact_nu": "contact_number",
    "opening_ho": "opening_hours",
    "beds": "num_beds",
    "staff_doct": "staff_docter",
    "staff_nurs": "staff_nursery",
    "health_ame": "health_amenities",
    "water_sour": "water_source",
    "electricit": "electricity",
    "addr_stree": "street",
    "addr_postc": "postcode",
    "addr_city": "city",
    "addr_house": "house_number"
})

# Drop unnecessary columns
cols_to_drop = ["operational", "water_source", "insurance", "staff_docter", "staff_nursery",
                "health_amenities", "wheelchair", "emergency", "electricity",
                "is_in_heal", "is_in_heal.1", "changeset_.1", "dispensing",
                "url", "changeset_", "changeset_.2", "changeset_.3", "contact_number"]

us_health_data = us_health_data.drop(columns=cols_to_drop, errors="ignore")

# Create full address column
us_health_data["iso3c"] = "USA"
us_health_data["downloaded_from"] = "https://data.humdata.org/organization/healthsites"

us_health_data["address"] = us_health_data[["house_number", "street", "postcode", "city", "iso3c"]].apply(
    lambda x: ", ".join(x.dropna().astype(str)), axis=1
)

print(f"Completed processed US HDX Data, records: {len(us_health_data)} ")

# Load UK health data
uk_health_node_raw = load_health_data("./data-raw/hdx/United Kingdom/United Kingdom-node.shp", "node")
uk_health_way_raw = load_health_data("./data-raw/hdx/United Kingdom/United Kingdom-way.shp", "way")

# Combine UK datasets
uk_health_combined = pd.concat([uk_health_node_raw, uk_health_way_raw], ignore_index=True)

# Rename columns
uk_health_data = uk_health_combined.rename(columns={
    "operator_t": "operator_type",
    "operationa": "operational",
    "contact_nu": "contact_number",
    "opening_ho": "opening_hours",
    "beds": "num_beds",
    "staff_doct": "staff_docter",
    "staff_nurs": "staff_nursery",
    "health_ame": "health_amenities",
    "water_sour": "water_source",
    "electricit": "electricity",
    "addr_stree": "street",
    "addr_postc": "postcode",
    "addr_city": "city",
    "addr_house": "house_number"
})

# Drop unnecessary columns
uk_health_data = uk_health_data.drop(columns=cols_to_drop, errors="ignore")

# Create full address column
uk_health_data["iso3c"] = "GBR"
uk_health_data["downloaded_from"] = "https://data.humdata.org/organization/healthsites"

uk_health_data["address"] = uk_health_data[["house_number", "street", "postcode", "city", "iso3c"]].apply(
    lambda x: ", ".join(x.dropna().astype(str)), axis=1
)

print(f"Completed processed UK HDX Data, records: {len(uk_health_data)} ")

# Load Nigeria health data (GeoJSON format)
ng_health_raw = gpd.read_file("./data-raw/hdx/nigeria.geojson")

# Rename columns
ng_health_data = ng_health_raw.rename(columns={
    "addr_street": "street",
    "addr_postcode": "postcode",
    "addr_city": "city",
    "addr_housenumber": "house_number",
    "beds": "num_beds"
})

# Drop unnecessary columns
cols_to_drop_ng = ["operational_status", "water_source", "insurance", "staff_doctors", "staff_nurses",
                   "health_amenity_type", "wheelchair", "emergency", "electricity",
                   "is_in_health_area", "is_in_health_zone", "changeset_id", "dispensing",
                   "url", "changeset_id", "changeset_version", "changeset_timestamp",
                   "uuid", "completeness"]

ng_health_data = ng_health_data.drop(columns=cols_to_drop_ng, errors="ignore")

# Create full address column
ng_health_data["iso3c"] = "NGA"
ng_health_data["downloaded_from"] = "https://data.humdata.org/organization/healthsites"

ng_health_data["address"] = ng_health_data[["house_number", "street", "postcode", "city", "iso3c"]].apply(
    lambda x: ", ".join(x.dropna().astype(str)), axis=1
)

# Select and reorder columns
selected_columns = ["osm_id", "amenity", "healthcare", "name", "operator", "source", "speciality",
                    "operator_type", "opening_hours", "num_beds", "house_number", "street",
                    "postcode", "city", "osm_type", "geometry", "iso3c", "downloaded_from", "address"]

ng_health_data = ng_health_data[selected_columns]
ng_health_data["osm_id"] = ng_health_data["osm_id"].astype(str)

print(f"Completed processed NG HDX Data, records: {len(ng_health_data)} ")



# Combine all datasets
health_data = pd.concat([ng_health_data, us_health_data, uk_health_data], ignore_index=True)

# Display the first few rows
print(health_data.head(10))

print(f"Completed processed data for NG, UK, US HDX Data, records: {len(health_data)} ")