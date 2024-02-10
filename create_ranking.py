import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    IntegerType,
    FloatType,
    StringType,
    StructField,
)


def main(
    bookings_parquet_path: str,
    airports_path: str,
    start_date: str,
    end_date: str,
    output_path: str,
) -> None:
    """
    Main function to perform Spark data transformation on flight bookings.

    :param bookings_parquet_path: Path to the Parquet file containing booking data
    :param airports_path: Path to the CSV file containing airport information
    :param start_date: Start date for filtering the bookings
    :param end_date: End date for filtering the bookings
    :param output_path: Output path to store the result in CSV format
    """

    # Start spark session
    spark = SparkSession.builder.getOrCreate()

    # Convert start and end dates to datetime objects
    start_date = F.to_date(F.lit(start_date))
    end_date = F.to_date(F.lit(end_date))

    # Read specific partitions based on the constructed path
    # Filter the DataFrame based on the date range
    bookings = spark.read.parquet(bookings_parquet_path).filter(
        (F.col("arrival_date") >= start_date) & (F.col("arrival_date") <= end_date)
    )

    airports_schema = StructType(
        [
            StructField("airport_id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("city", StringType(), True),
            StructField("country", StringType(), True),
            StructField("IATA", StringType(), True),
            StructField("ICAO", StringType(), True),
            StructField("lat", FloatType(), True),
            StructField("lon", FloatType(), True),
            StructField("alt", IntegerType(), True),
            StructField("timezone", FloatType(), True),
            StructField("DST", StringType(), True),
            StructField("timezone_tz", StringType(), True),
            StructField("type", StringType(), True),
            StructField("source", StringType(), True),
        ]
    )

    # Load airport data
    airports = (
        spark.read.format("csv")
        .option("header", "false")
        .schema(airports_schema)
        .load(airports_path)
    )

    # Transform the data into the specified format
    bookings_klm_sorted = main_transformation(bookings, airports)

    # Write CSV file with column header (column names)
    bookings_klm_sorted.write.option("header", True).csv(output_path)


def main_transformation(bookings: "DataFrame", airports: "DataFrame") -> "DataFrame":
    """
    Function to perform specific transformations on flight bookings data.

    :param bookings: Spark DataFrame containing flight booking information
    :param airports: Spark DataFrame containing airport information
    :return: Transformed DataFrame with sorted results
    """

    # get the proper NL airport names
    nl_airport_list = (
        airports.filter((F.col("country") == "Netherlands"))
        .select("IATA")
        .distinct()
        .rdd.flatMap(lambda x: x)
        .collect()
    )

    # Filter out flights:
    # not operated by KLM (KL)
    # CONFIRMED booking
    # origin airport is from Netherlands
    bookings_klm = bookings.filter(
        (
            (F.col("bookingStatus") == "CONFIRMED")
            & (F.col("operatingAirline") == "KL")
            & (F.col("age").isNotNull())
            & (F.col("originAirport").isin(nl_airport_list))
        )
    )

    # Join and apply transformations
    bookings_klm_joined = bookings_klm.join(
        airports.select("IATA", "timezone_tz", "country"),
        bookings.destinationAirport == airports.IATA,
        "left",
    )
    bookings_klm_joined = bookings_klm_joined.withColumn(
        "arrivalTimestamp",
        F.from_utc_timestamp(F.col("arrivalDate"), F.col("timezone_tz")),
    )

    # Get the day of the week as a string (e.g., 'Monday')
    bookings_klm_joined = bookings_klm_joined.withColumn(
        "day_of_week", F.date_format(F.col("arrivalTimestamp"), "EEEE")
    )

    # Get the month and use it to determine the season
    bookings_klm_joined = bookings_klm_joined.withColumn(
        "month", F.month(F.col("arrivalTimestamp"))
    )

    # Apply the UDF to get the season
    bookings_klm_joined = bookings_klm_joined.withColumn(
        "season", get_season_udf(F.col("month"))
    )

    # Count the number of passengers per country, per day of the week, and per season
    bookings_klm_sorted = bookings_klm_joined.groupBy(
        "country", "day_of_week", "season"
    ).count()

    # Sort the result in descending order by the number of bookings
    bookings_klm_sorted = bookings_klm_sorted.orderBy(
        [F.col("season").asc(), F.col("day_of_week").asc(), F.col("count").desc()]
    )

    return bookings_klm_sorted


# Define a custom function to get the season
def get_season(month: int) -> str:
    """
    Helper function to determine the season based on the month.

    :param month: Integer representing the month
    :return: String representing the season
    """

    if 3 <= month <= 5:
        return "Spring"
    elif 6 <= month <= 8:
        return "Summer"
    elif 9 <= month <= 11:
        return "Autumn"
    else:
        return "Winter"


# Register the custom function as a Spark User Defined Function (UDF)
get_season_udf = F.udf(get_season, StringType())


if __name__ == "__main__":
    # Example usage:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    bookings_parquet_path = os.path.join(script_dir, "data/bookings_parquet")
    airports_path = os.path.join(script_dir, "data/airports")
    start_date = "1000-01-01"
    end_date = "3000-12-31"
    output_path = os.path.join(script_dir, "data/output")

    # I would use Airflow to schedule this data transformation to run daily
    # in that case the parameters wouldn't be here but in the airflow DAG configuration
    exit_code = main(
        bookings_parquet_path,
        airports_path,
        start_date,
        end_date,
        output_path,
    )
    sys.exit(exit_code)
