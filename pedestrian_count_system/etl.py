from dataclasses import dataclass

import pyspark.sql.functions as f
from eliot import start_action
from pyspark.sql import DataFrame, SparkSession

from logger.logger import __LOGGER__

__LOGGER__


def read_from_loc(spark: SparkSession, read_from: str) -> DataFrame:
    """
    :param spark: This is SparkSession
    :param read_from: The path to read from. Expected to be s3 bucket.
    :return: A spark dataframe.

    """
    with start_action(action_type="read_from_loc", read_from=read_from) as action:
        try:
            dataframe = (
                spark.read.option("header", True)
                .option("multiline", True)
                .json(read_from)
            )
            action.log(
                message_type="info",
                number_of_observations=dataframe.select(f.count("sensor_id")).first()[
                    "count(sensor_id)"
                ],
            )
            return dataframe

        except Exception as e:
            raise ValueError(str(e))


def write_to_loc(dataframe: DataFrame, write_to) -> None:
    """
    :param dataframe: A spark dataframe.
    :param write_to: A location to write to. Expected to be s3 bucket
    :return:
    """
    with start_action(action_type="read_from_loc", write_to=write_to):
        try:
            if dataframe is not None:
                (
                    dataframe.write.mode("overwrite")
                    .option("header", True)
                    .parquet(write_to)
                )
        except Exception as e:
            raise ValueError(str(e))
