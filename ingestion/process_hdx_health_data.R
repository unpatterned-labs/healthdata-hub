# Load required libraries
library(sf)
library(dplyr)
library(stringr)

# Function to read and process shapefiles
process_shapefile <- function(node_path, way_path, country_code) {
  # Read the shapefiles
  node_data <- st_read(node_path, quiet = TRUE) %>% mutate(osm_type = "node")
  way_data <- st_read(way_path, quiet = TRUE) %>% mutate(osm_type = "way")

  # Combine node and way data
  combined_data <- bind_rows(node_data, way_data)

  # Rename columns for consistency
  cleaned_data <- combined_data %>%
    rename(
      operator_type = operator_t,
      operational = operationa,
      contact_number = contact_nu,
      opening_hours = opening_ho,
      num_beds = beds,
      staff_doctor = staff_doct,
      staff_nursery = staff_nurs,
      health_amenities = health_ame,
      water_source = water_sour,
      electricity = electricit,
      street = addr_stree,
      postcode = addr_postc,
      city = addr_city,
      house_number = addr_house
    ) %>%
    # Drop unnecessary columns
    select(-c(
      operational, water_source, insurance, staff_doctor, staff_nursery,
      health_amenities, wheelchair, emergency, electricity, is_in_heal,
      is_in_heal.1, changeset_.1, dispensing, url, changeset_, changeset_.2,
      changeset_.3, contact_number
    )) %>%
    # Add ISO country code and data source
    mutate(
      iso3c = country_code,
      downloaded_from = "https://data.humdata.org/organization/healthsites",
      address = paste(house_number, street, postcode, city, iso3c, sep = ", ") %>%
        str_replace_all("NA, ", "") %>%
        str_replace_all(", NA", "") %>%
        str_replace_all("NA", "")
    )

  return(cleaned_data)
}

# Process US data
us_health_data <- process_shapefile(
  "data-raw/hdx/United States/United States-node.shp",
  "data-raw/hdx/United States/United States-way.shp",
  "USA"
)

# Process UK data
uk_health_data <- process_shapefile(
  "data-raw/hdx/United Kingdom/United Kingdom-node.shp",
  "data-raw/hdx/United Kingdom/United Kingdom-way.shp",
  "GBR"
)

# Process Nigeria data
ng_health_data <- st_read("data-raw/hdx/nigeria.geojson", quiet = TRUE) %>%
  rename(
    street = addr_street,
    postcode = addr_postcode,
    city = addr_city,
    house_number = addr_housenumber,
    num_beds = beds
  ) %>%
  select(-c(
    operational_status, water_source, insurance, staff_doctors, staff_nurses,
    health_amenity_type, wheelchair, emergency, electricity, is_in_health_area,
    is_in_health_zone, changeset_id, dispensing, url, changeset_version,
    changeset_timestamp, uuid, completeness
  )) %>%
  mutate(
    iso3c = "NGA",
    downloaded_from = "https://data.humdata.org/organization/healthsites",
    address = paste(house_number, street, postcode, city, iso3c, sep = ", ") %>%
      str_replace_all("NA, ", "") %>%
      str_replace_all(", NA", "") %>%
      str_replace_all("NA", "")
  ) %>%
  select(
    osm_id, amenity, healthcare, name, operator, source, speciality,
    operator_type, opening_hours, num_beds, house_number, street, postcode,
    city, osm_type, geometry, iso3c, downloaded_from, address
  ) %>%
  mutate(osm_id = as.character(osm_id))

# Combine all country data
health_data <- bind_rows(ng_health_data, us_health_data, uk_health_data)

# Save to different formats
sfarrow::st_write_parquet(health_data, "data/hdx_health_data_ng_us_uk_v2.parquet")
#st_write(health_data, dsn = "data/hdx_health_data_ng_uk_us.gpkg", layer = "hdx_source", quiet = TRUE)

# Print structure of data for review
glimpse(health_data)
