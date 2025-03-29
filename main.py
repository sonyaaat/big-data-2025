from pyspark.sql import SparkSession
import os
from pyspark.sql import DataFrame
from typing import Dict
from config import Config

from schemas import (
    schema_title_basics,
    schema_title_episode,
    schema_title_crew,
    schema_title_akas
)

from imdb_spark_utils import (
    initialize_spark,
    load_dataframe,
    transform_title_basics,
    transform_title_akas,
    transform_title_crew,
    transform_title_episode,
    display_dataframe_info
)

spark_session = initialize_spark("IMDB Data Processing")


def check_pyspark() -> None:
    print("Initializing Spark Session...")
    spark = SparkSession.builder \
        .appName("IMDB Data Check") \
        .getOrCreate()

    print("Spark Session initialized.")
    print("Checking available files...")
    files = [f for f in os.listdir(Config.DATA_DIR) if f.endswith(".tsv")]

    if not files:
        print("No TSV files found. Make sure data is downloaded and extracted.")
        return

    print("Found files:", files)

    sample_file = os.path.join(Config.DATA_DIR, files[0])
    print(f"Loading sample file: {sample_file}")

    df = spark.read.option("header", "true").option("sep", "\t").csv(sample_file)

    print("Schema of the loaded file:")
    df.printSchema()

    print("Showing first 5 rows:")
    df.show(5)

    print("PySpark check complete!")

    spark.stop()


def process_imdb_data() -> Dict[str, DataFrame]:
    dataframes = {}

    dataframes["basics"] = transform_title_basics(
        load_dataframe(spark_session, schema_title_basics, f"{Config.DATA_DIR}/title.basics{Config.FILE_EXTENSION}")
    )
    dataframes["akas"] = transform_title_akas(
        load_dataframe(spark_session, schema_title_akas, f"{Config.DATA_DIR}/title.akas{Config.FILE_EXTENSION}")
    )
    dataframes["crew"] = transform_title_crew(
        load_dataframe(spark_session, schema_title_crew, f"{Config.DATA_DIR}/title.crew{Config.FILE_EXTENSION}")
    )
    dataframes["episode"] = transform_title_episode(
        load_dataframe(spark_session, schema_title_episode, f"{Config.DATA_DIR}/title.episode{Config.FILE_EXTENSION}")
    )

    for name, df in dataframes.items():
        display_dataframe_info(df, name)

    return dataframes


if __name__ == "__main__":
    # check_pyspark()
    process_imdb_data()
