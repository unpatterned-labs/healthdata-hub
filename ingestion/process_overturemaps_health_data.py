import os
import time
import argparse
import pyarrow.dataset as ds
import pyarrow.compute as pc
import pyarrow as pa
import polars as pl
import pyarrow.fs as fs


def convert_map_to_list_of_structs(table: pa.Table) -> pa.Table:
    """Convert all Map types in the table to List[Struct] for compatibility with Polars."""

    def _convert_type(data_type):
        if isinstance(data_type, pa.MapType):
            return pa.list_(pa.struct([
                pa.field("key", data_type.key_type),
                pa.field("value", data_type.item_type)
            ]))
        elif isinstance(data_type, pa.StructType):
            return pa.struct([pa.field(f.name, _convert_type(f.type)) for f in data_type])
        elif isinstance(data_type, pa.ListType):
            return pa.list_(_convert_type(data_type.value_type))
        return data_type

    new_schema = [pa.field(field.name, _convert_type(field.type)) for field in table.schema]
    return table.cast(pa.schema(new_schema))


def fetch_overture_data(s3_path: str, bbox: tuple) -> pl.DataFrame:
    """Fetch data from Overture Maps within a bounding box and return as Polars DataFrame."""
    dataset = ds.dataset(s3_path, filesystem=fs.S3FileSystem(anonymous=True, region="us-west-2"))

    # Apply bounding box filter
    bbox_filter = (
            (pc.field("bbox", "xmin") < bbox[2]) &
            (pc.field("bbox", "xmax") > bbox[0]) &
            (pc.field("bbox", "ymin") < bbox[3]) &
            (pc.field("bbox", "ymax") > bbox[1])
    )

    batches = dataset.to_batches(filter=bbox_filter)
    non_empty_batches = [b for b in batches if b.num_rows > 0]

    if not non_empty_batches:
        raise ValueError("No data found within the specified bounding box.")

    # Convert to Polars DataFrame
    table = pa.Table.from_batches(non_empty_batches)
    converted_table = convert_map_to_list_of_structs(table)
    return pl.from_arrow(converted_table)


def process_health_data(df: pl.DataFrame, country_code: str) -> pl.DataFrame:
    """Process and filter UK health-related places from Overture Maps data."""

    health_categories = [
        "hospital", "doctor", "hospice", "health_and_medical", "surgeon",
        "personal_care_service", "psychiatrist", "rehabilitation_center",
        "walk_in_clinic", "medical_center", "wellness_program", "surgical_center",
        "public_health_clinic", "physical_therapy", "occupational_medicine",
        "occupational_therapy"
    ]

    return (
        df
        .with_columns(
        pl.col("addresses").list.first().struct.field("freeform").alias("street"),
        pl.col("addresses").list.first().struct.field("locality").alias("locality"),
        pl.col("addresses").list.first().struct.field("postcode").alias("postcode"),
        pl.col("addresses").list.first().struct.field("region").alias("region"),
        pl.col("addresses").list.first().struct.field("country").alias("country"),
        pl.col("categories").struct.field("primary").alias("category"),
        pl.col("names").struct.field("primary").alias("name"),
        pl.col("sources").list.eval(pl.element().struct.field("dataset")).alias("dataset_source"),
        pl.col("sources").list.eval(pl.element().struct.field("update_time")).alias("update_time"),
        )

        # Filter for UK locations
        .filter(pl.col("country") == country_code )

        # Filter for relevant health categories
        .filter(pl.col("category").is_in(health_categories))

        # Filter for confidence > 0.5
        .filter(pl.col("confidence") > 0.5)

        # Flatten list columns properly
        .explode("socials")
        .explode("websites")
        .explode("phones")
        .explode("emails")

        # Rename and select final columns
        .rename({"id": "uuid", "phones": "phone_number"})
        .select([
            "uuid", "name", "category", "websites", "socials", "emails",
            "phone_number", "street", "locality", "postcode", "region",
            "country", "dataset_source", "update_time", "geometry"
        ])
    )


def main(bbox: tuple, country_code: str, output_dir: str):
    """Main function to fetch, process, and save UK health data."""
    s3_path = "overturemaps-us-west-2/release/2025-02-19.0/theme=places/type=place/"
    start_time = time.time()
    print(f"📡 Fetching data for bbox: {bbox} ...")
    df = fetch_overture_data(s3_path, bbox)

    print("🩺 Processing health-related places...")
    processed_df = process_health_data(df, country_code)

    print(f"✅ Processed {country_code} health data successfully!")

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    output_path = os.path.join(output_dir, f"{country_code.lower()}_health_data.parquet")
    processed_df.write_parquet(output_path)

    end_time = time.time()
    print(f"✅ Data saved to {output_path}")
    print(f"⏳ Total execution time: {end_time - start_time:.2f} seconds.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process UK Health Data from Overture Maps.")
    parser.add_argument("--bbox", type=float, nargs=4, required=True,
                        help="Bounding box in format: xmin ymin xmax ymax")
    parser.add_argument("--country", type=str, default="GB",
                        help="Country code filter (default: GB)")
    parser.add_argument("--output_dir", type=str, default="data/processed",
                        help="Directory to save the processed data")

    args = parser.parse_args()
    bbox = tuple(args.bbox)
    country_code = args.country.upper()  # Ensure country code is uppercase
    main(bbox, country_code, args.output_dir)

## -14.02 49.67 2.09 61.06
## -2.1099092231,52.3556570611,-1.6529557739,52.5474833593
## How to run?
## python .\ingestion\process_overturemaps_health_data.py --bbox -14.02 49.67 2.09 61.06 --country GB
