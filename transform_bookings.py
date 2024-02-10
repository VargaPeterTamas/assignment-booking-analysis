from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys
import os


def main(bookings_path: str, output_path: str) -> None:
    """
    Transforms the bookings data into parquet files partitioned by date
    :param bookings_path: Path to the bookings directory
    :param output_path: Path to the output directory
    """

    # Start spark session
    spark = SparkSession.builder.getOrCreate()

    if bookings_path.startswith("hdfs://"):
        # Handle HDFS URI
        bookings = spark.read.json(bookings_path + "/*")
    else:
        # Handle local directory
        bookings = spark.read.json(bookings_path)

    # Explode passengersList
    passengers_df = bookings.select(
        "timestamp",
        F.explode("event.DataElement.travelrecord.passengersList").alias("passenger"),
    ).select("timestamp", "passenger.uci", "passenger.age", "passenger.passengerType")

    # Explode productsList
    products_df = bookings.select(
        "timestamp",
        F.explode("event.DataElement.travelrecord.productsList").alias("product"),
    ).select("timestamp", "product.bookingStatus", "product.flight")

    # Explode flight within productsList
    flights_df = products_df.select(
        "timestamp",
        "bookingStatus",
        "flight.operatingAirline",
        "flight.originAirport",
        "flight.destinationAirport",
        "flight.departureDate",
        "flight.arrivalDate",
    )

    # Join passengers and flights DataFrames
    bookings_result = passengers_df.join(
        flights_df, on="timestamp", how="inner"
    ).select(
        "timestamp",
        "uci",
        "age",
        "passengerType",
        "bookingStatus",
        "operatingAirline",
        "originAirport",
        "destinationAirport",
        "departureDate",
        "arrivalDate",
    )

    # Extract date part from the timestamp and create a new column for partitioning
    bookings_result = bookings_result.withColumn(
        "arrival_date", F.to_date("arrivalDate")
    )

    # Save the result as a Parquet file partitioned by the new column
    bookings_result.write.partitionBy("arrival_date").parquet(
        output_path,
        mode="overwrite",
    )


if __name__ == "__main__":
    # Example usage:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    bookings_path = os.path.join(script_dir, "data/bookings")
    output_path = os.path.join(script_dir, "data/bookings_parquet")

    # I would use Airflow to schedule this data transformation to run daily
    # in that case the parameters wouldn't be here but in the airflow DAG configuration

    exit_code = main(bookings_path, output_path)
    sys.exit(exit_code)
