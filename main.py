import asyncio

import fire
import pyspark.sql.functions as f
from pyspark.sql import SparkSession

from configs.configs import spark
from pedestrian_count_system.api_call import APIUrl
from pedestrian_count_system.etl import read_from_loc, write_to_loc


async def api_deploy(
    url: str, dir_name: str, bucket, years: list = None, month: str = None
):
    """
    Call the api to fetch the data and write to location. Expected to be s3 bucket.
    :param url: api url to call.
    :param dir_name: Location to write the file to. Expected to be s3.
    :param bucket: s3 bucket
    :param year: To filter the payload by year.
    :param month: To filter the payload by month
    :return:
    """

    apiCall = APIUrl(url, bucket, dir_name)

    if years is not None:
        tasks = []
        for year in years:
            tasks.append(apiCall.write_json_to_s3(year))

        return await asyncio.gather(*tasks)

    else:
        return await apiCall.write_json_to_s3()


def etl_deploy(
    spark: SparkSession, read_from: str, read_from_2: str, *write_to
) -> None:
    """
    Read two path of json files into two different dataframes.
    And join the two dataframes to create a new dataframe.
    Write the new dataframe as a parquet file to s3 bucket.
    Use the dataframe to answer the following questions:
        1. Top 10 locations by day
        2. Top 10 location by month.
    The results of the questions are writing to s3 bucket.

    :param spark: Spark session
    :param read_from: The path to read the file from. Expected to be s3 bucket
    :param read_from_2: The path to read the second file from. Expected to be s3 bucket
    """

    write_to_1, write_to_2, write_to_3 = write_to

    trip_df = read_from_loc(spark, read_from)
    trip_df = trip_df.withColumn("date_time", f.to_timestamp("date_time")).withColumn(
        "hourly_counts", f.col("hourly_counts").cast("int")
    )

    hg_df = read_from_loc(spark, read_from_2)
    hg_df = (
        hg_df.withColumn("installation_date", f.to_timestamp("installation_date"))
        .withColumn("latitude", f.col("latitude").cast("float"))
        .withColumn("longitude", f.col("longitude").cast("float"))
        .drop("location", "note", "sensor_name")
    )

    df = trip_df.join(hg_df, on="sensor_id", how="left")

    write_to_loc(df, write_to_1)

    # Create SQL View
    df.createOrReplaceTempView("df_sql")

    # Top 10 locations by day
    top_10_loc_by_day = spark.sql(
        """
            WITH source_table as (
                SELECT day, sensor_id, sensor_name, latitude, longitude, sum(hourly_counts) total_count
                FROM df_sql
                GROUP BY day, sensor_id, sensor_name, latitude, longitude
            ),
            staging_table as (
             SELECT day, sensor_id, sensor_name, latitude, longitude, total_count, ROW_NUMBER() OVER(PARTITION BY day ORDER BY total_count DESC) AS row_num
            from source_table
            )
            SELECT day, sensor_id, sensor_name as location, latitude, longitude, total_count
            FROM staging_table
            WHERE row_num <=10

    """
    )
    top_10_loc_by_day = top_10_loc_by_day.coalesce(1)
    write_to_loc(top_10_loc_by_day, write_to_2)

    # Top 10 locations by month
    top_10_loc_by_month = spark.sql(
        """
            WITH source_table as (
                SELECT month, sensor_id, sensor_name, latitude, longitude,
                sum(hourly_counts) total_count
                FROM df_sql
                GROUP BY month, sensor_id, sensor_name, latitude, longitude
            ),
            staging_table as (
             SELECT month, sensor_id, sensor_name, latitude, longitude, total_count,
             ROW_NUMBER() OVER(PARTITION BY month ORDER BY total_count DESC) AS row_num
            from source_table
            )
            SELECT month, sensor_id, sensor_name as location, latitude, longitude, total_count
            FROM staging_table
            WHERE row_num <=10

    """
    )
    top_10_loc_by_month = top_10_loc_by_month.coalesce(1)
    write_to_loc(top_10_loc_by_month, write_to_3)


def main(
    url="https://data.melbourne.vic.gov.au/resource/b2ak-trbp.json",
    url2="https://data.melbourne.vic.gov.au/resource/h57g-5234.json",
    years=[2009 + i for i in range(13)],
    bucket="ml-artifact-store",
    dir_name="data",
):

    dir_name_1 = f"{dir_name}/raw"
    read_from = f"s3a://{bucket}/{dir_name_1}/*"
    read_from2 = f"s3a://{bucket}/{dir_name}/file.json"
    write_to_1 = f"s3a://{bucket}/{dir_name}/staging"
    write_to_2 = f"s3a://{bucket}/{dir_name}/top_10_loc_by_day"
    write_to_3 = f"s3a://{bucket}/{dir_name}/top_10_loc_by_month"

    # read_from=f"{dir_name}/raw/*"
    # read_from2= f"{dir_name}/file.json"
    # write_to_1=f"{dir_name}/staging"
    # write_to_2=f"{dir_name}/top_10_loc_by_day"
    # write_to_3=f"{dir_name}/top_10_loc_by_month"

    asyncio.run(api_deploy(url, dir_name_1, bucket, years))
    asyncio.run(api_deploy(url2, dir_name, bucket))

    etl_deploy(spark, read_from, read_from2, write_to_1, write_to_2, write_to_3)


if __name__ == "__main__":
    fire.Fire(main)
