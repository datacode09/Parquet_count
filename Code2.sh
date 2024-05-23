#!/bin/bash

# Load environment variables
source /path/to/your/environment/file

# Set the HDFS directory containing the Parquet files and search string
HDFS_PARQUET_DIR="hdfs:///path/to/your/parquet/directory"
SEARCH_STRING="2023-05"  # Year and month to filter files

# Specify the output file
OUTPUT_FILE="/path/to/your/output/file.txt"

# Generate the list of Parquet files
PARQUET_FILES=$(hdfs dfs -ls $HDFS_PARQUET_DIR | grep $SEARCH_STRING | awk '{print $8}')

# Check if any Parquet files were found
if [ -z "$PARQUET_FILES" ]; then
    echo "No Parquet files found for the specified search string."
    exit 1
fi

# Define a temporary Python script to run the PySpark job
TEMP_PY_SCRIPT="/tmp/pyspark_count_rows.py"

cat << EOF > ${TEMP_PY_SCRIPT}
from pyspark.sql import SparkSession
import sys

def main(parquet_files, output_file):
    spark = SparkSession.builder.appName("Count Parquet Rows").getOrCreate()

    # Load all Parquet files into a single DataFrame
    df = spark.read.parquet(*parquet_files)

    # Create a temporary view to run SQL queries
    df.createOrReplaceTempView("parquet_table")

    # SQL query to extract yyyy-mm-dd from datecompleted and count rows grouped by the date
    result_df = spark.sql("""
        SELECT 
            date_format(datecompleted, 'yyyy-MM-dd') AS datecompleted_date, 
            COUNT(*) AS count 
        FROM parquet_table 
        GROUP BY date_format(datecompleted, 'yyyy-MM-dd')
    """)

    # Collect the result and append to the output file
    result = result_df.collect()
    with open(output_file, "a") as file:
        for row in result:
            file.write(f"{row['datecompleted_date']} | {row['count']}\n")

    spark.stop()

if __name__ == "__main__":
    parquet_files = sys.argv[1:-1]
    output_file = sys.argv[-1]
    main(parquet_files, output_file)
EOF

# Convert the list of Parquet files to space-separated string for passing to the Python script
PARQUET_FILES_STR=$(printf " %s" ${PARQUET_FILES[@]})

# Run the PySpark job with the list of Parquet files and the output file as arguments
pyspark --master yarn --queue PRIE0 < ${TEMP_PY_SCRIPT} ${PARQUET_FILES_STR} ${OUTPUT_FILE}

# Clean up the temporary Python script
rm ${TEMP_PY_SCRIPT}

# Check the PySpark job output
if [ $? -eq 0 ]; then
    echo "PySpark job completed successfully."
else
    echo "PySpark job failed."
    exit 1
fi
