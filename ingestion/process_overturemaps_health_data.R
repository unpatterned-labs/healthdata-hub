# Load necessary libraries
library(overturemapsr)  # Overture Maps API
library(dplyr)          # Data manipulation
library(tidyr)          # Data tidying
library(purrr)          # Functional programming (if needed)
library(lubridate)      # Working with timestamps

#uk_bbox <- c(-14.02,49.67,2.09,61.06)

bhx_bbox <- c(-2.1099092231,52.3556570611,-1.6529557739,52.5474833593)

# Function to fetch data from Overture Maps based on a bounding box
fetch_overture_data <- function(bbox) {
  start_time <- Sys.time()
  message("📡 Fetching data for bounding box: ", paste(bbox, collapse = ", "))

  # Get place records within the bounding box
  places <- record_batch_reader(schema_type = "place", bbox = bbox)

  end_time <- Sys.time()
  message("⏳ Data fetched in ", round(difftime(end_time, start_time, units = "secs"), 2), " seconds.")
  return(places)
}



# Function to process and filter health-related data
process_health_data <- function(data, country_code = "GB") {
  start_time <- Sys.time()
  message("🩺 Processing health-related places for country: ", country_code)

  # Define relevant health categories
  health_categories <- c(
    "hospital", "doctor", "hospice", "health_and_medical", "surgeon",
    "personal_care_service", "psychiatrist", "rehabilitation_center",
    "walk_in_clinic", "medical_center", "wellness_program", "surgical_center",
    "public_health_clinic", "physical_therapy", "occupational_medicine",
    "occupational_therapy"
  )

  # Filter for relevant categories and high confidence levels
  health_data <- data %>%
    filter(categories$primary %in% health_categories) %>%
    filter(confidence > 0.5) %>%

    # Expand nested fields (unnest lists and structures)
    unnest(socials) %>%
    unnest(sources, names_sep = "_") %>%
    unnest(names, names_sep = ".") %>%
    unnest(addresses) %>%
    unnest(categories) %>%
    unnest(websites) %>%
    unnest(phones) %>%

    # Remove unnecessary columns
    select(-brand, -bbox, -version, -sources_property, -sources_confidence, -alternate) %>%

    # Rename columns for consistency
    rename(
      dataset_source = sources_dataset,
      update_time = sources_update_time,
      phone_number = phones,
      street = freeform,
      uuid = id,
      category = primary
    ) %>%

    # Select final columns
    select(
      uuid, name = names.primary, category, websites, socials, emails, phone_number,
      street, locality, postcode, region, country, dataset_source, update_time, geometry
    ) %>%

    # Filter for specified country
    filter(country == country_code)

  end_time <- Sys.time()
  message("⏳ Data processed in ", round(difftime(end_time, start_time, units = "secs"), 2), " seconds.")
  return(health_data)
}


# # Function to save data to a CSV file
# save_data <- function(data, output_dir, country_code) {
#   if (!dir.exists(output_dir)) {
#     dir.create(output_dir, recursive = TRUE)
#   }
#
#   output_file <- file.path(output_dir, paste0(country_code, "_health_data_v2.csv"))
#   write.csv(data, output_file, row.names = FALSE)
#
#   message("✅ Data saved to: ", output_file)
# }
#save_data(processed_data, output_dir, country_code)

total_start <- Sys.time()

uk_bbox <- c(-14.02, 49.67, 2.09, 61.06)  # Define bounding box for the UK

# Storing the place data
places_data <- fetch_overture_data(bbox = uk_bbox)

# the processed data
processed_data <- process_health_data(places_data, country_code = "GB")
total_end <- Sys.time()
message("⏳ Total execution time: ", round(difftime(total_end, total_start, units = "secs"), 2), " seconds.")



# Example execution

uk_bbox <- c(-2.1099092231,52.3556570611,-1.6529557739,52.5474833593)
uk_bbox <- c(-14.02, 49.67, 2.09, 61.06)  # Define bounding box for the UK
main(uk_bbox, country_code = "GB")
