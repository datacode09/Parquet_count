#!/bin/bash

# Load environment variables
source /path/to/your/environment/file

# Set the HDFS directory containing the Parquet files
HDFS_PARQUET_DIR="hdfs:///path/to/your/parquet/directory"
OUTPUT_FILE="/path/to/your/output/file.txt"

# Define a temporary Python script to run the PySpark job
TEMP_PY_SCRIPT="/tmp/pyspark_count_rows.py"

cat << 'EOF' > ${TEMP_PY_SCRIPT}
from pyspark.sql import SparkSession
import sys

def main(hdfs_path, output_file):
    spark = SparkSession.builder.appName("Count Parquet Rows").getOrCreate()

    # List all Parquet files in the HDFS directory
    parquet_files = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jsc.hadoopConfiguration()
    ).listStatus(spark._jvm.org.apache.hadoop.fs.Path(hdfs_path))

    for file_status in parquet_files:
        if file_status.isFile() and file_status.getPath().getName().endswith('.parquet'):
            file_path = file_status.getPath().toString()
            df = spark.read.parquet(file_path)
            row_count = df.count() - 1
            result = f"{file_path} | {row_count}"

            with open(output_file, "a") as file:
                file.write(result + "\n")

    spark.stop()

if __name__ == "__main__":
    hdfs_path = sys.argv[1]
    output_file = sys.argv[2]
    main(hdfs_path, output_file)
EOF

# Run the PySpark job with the HDFS directory and output file as arguments
pyspark --master yarn --queue PRIE0 ${TEMP_PY_SCRIPT} ${HDFS_PARQUET_DIR} ${OUTPUT_FILE}

# Clean up the temporary Python script
rm ${TEMP_PY_SCRIPT}

# Check the PySpark job output
if [ $? -eq 0 ]; then
    echo "PySpark job completed successfully."
else
    echo "PySpark job failed."
    exit 1
fi
