#!/bin/bash

# Create directories

mkdir -p {ingestion,processing,utils, notebooks, examples,tests,docs,pipeline,data}

# Create essential files
touch {README,setup.py,requirements.txt,.gitignore}

# Add a basic readme
echo "# Health Data Hub" > README.md
echo "An open-source workflow for harmonizing health facility data from multiple sources." > README.md

# Add an initial requirements file
echo "pandas" > requirements.txt
echo "requests" > requirements.txt
echo "geopandas" > requirements.txt
echo "placekey" >  requirements.txt
echo "duckdb" > requirements.txt

echo "Project setup complete!"