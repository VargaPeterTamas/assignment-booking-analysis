# Commercial Booking Analysis

Provide detailed information on your product and how to run it here

## Documentation:

### Requirements:

The scripts assume that you have Apache Spark and PySpark installed in your environment.

## Script 1 - transform_bookings.py:

### Inputs:

bookings_path: Path to the directory containing raw bookings data (JSON format).
output_path: Path to the output directory where the transformed data will be stored (Parquet format).

### Functionality:

Reads raw bookings data.
Expands nested structures in the data (passengersList, productsList).
Joins relevant information about passengers and flights.
Extracts the date part from the timestamp and partitions the data by arrival date.
Writes the transformed data as Parquet files.

## Script 2 - create_ranking.py:

### Inputs:

bookings_parquet_path: Path to the Parquet file containing the transformed booking data.
airports_path: Path to the CSV file containing airport information.
start_date and end_date: Date range for filtering the bookings.
output_path: Path to store the final result in CSV format.

### Functionality:

Reads the transformed bookings data from Parquet.
Filters data based on specified criteria (KLM flights, confirmed bookings, etc.).
Joins with airport information and performs additional time zone adjustments.
Aggregates booking statistics per country, day of the week, and season.
Writes the final result as a CSV file.

## Usage Recommendations:

It's recommended to use a workflow orchestration tool like Apache Airflow for scheduling and automating these data transformation tasks.
Note:

These scripts are designed as examples, and additional adjustments may be needed based on specific data and analysis requirements.
