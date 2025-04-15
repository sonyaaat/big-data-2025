from pyspark.sql import SparkSession
import os
from pyspark.sql import DataFrame
from typing import Dict
from config import Config
import pyspark.sql.types as t

from schemas import (
    schema_title_basics,
    schema_title_episode,
    schema_title_crew,
    schema_title_akas,
    schema_title_ratings,
    schema_title_principals,
    schema_name_basics
)

from imdb_spark_utils import (
    initialize_spark,
    load_dataframe,
    transform_title_basics,
    transform_title_akas,
    transform_title_crew,
    transform_title_episode,
    display_dataframe_info,
    display_numerical_statistics,
    display_categorical_distincts,
    correlation_runtime_rating,
    display_title_type_info,
    transform_title_ratings,
    transform_title_principals,
    transform_name_basics
)

from queries import(
    queries_Bohdan_Yarema,
    queries_Khrystyna_Melnychuk,
    queries_Sofiia_Tkach,
    queries_Ostapenko_Angelina,
    queries_Vasyl_Hunia,
    queries_Dmytro_Chupryna
)


spark_session = initialize_spark("IMDB Data Processing")


def check_pyspark() -> None:
    print("Initializing Spark Session...")
    spark = SparkSession.builder \
        .appName("IMDB Data Check") \
        .getOrCreate()

    print("Spark Session initialized.")
    print("Checking available files...")
    files = [f for f in os.listdir(Config.DATA_DIR) if f.endswith(Config.FILE_EXTENSION)]

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
    dataframes["ratings"] = transform_title_ratings(
        load_dataframe(spark_session, schema_title_ratings, f"{Config.DATA_DIR}/title.ratings{Config.FILE_EXTENSION}")
    )
    dataframes["principals"] = transform_title_principals(
        load_dataframe(spark_session, schema_title_principals, f"{Config.DATA_DIR}/title.principals{Config.FILE_EXTENSION}")
    )
    dataframes["name"] = transform_name_basics(
        load_dataframe(spark_session, schema_name_basics, f"{Config.DATA_DIR}/name.basics{Config.FILE_EXTENSION}")
    )

    for name, df in dataframes.items():

        # general info: rows, columns, schema
        display_dataframe_info(df, name)

        # choosing only Integer & Double columns
        df.select([f.name for f in df.schema.fields if isinstance(f.dataType, t .IntegerType) or isinstance(f.dataType, t .DoubleType)]).summary().show()
        # or by .decribe() method (will not show quartiles) -> call display_numerical_statistics() function

        # showing distinct values of each column
        display_categorical_distincts(df, name)

    # correlation between runtime and rating
    correlation_runtime_rating(dataframes["basics"], dataframes["ratings"])

    # categories of titleTypes (e.g. movie, tvSeries, short, video etc)
    display_title_type_info(dataframes["basics"])

    return dataframes


def main() -> None:
    dataframes = process_imdb_data()
    
    queries_Bohdan_Yarema.execute_analytical_requests(dataframes)
    queries_Ostapenko_Angelina.execute_analytical_requests(dataframes)
    queries_Khrystyna_Melnychuk.execute_analytical_requests(dataframes)
    queries_Vasyl_Hunia.execute_analytical_requests(dataframes)
    queries_Sofiia_Tkach.execute_analytical_requests(dataframes)
    queries_Dmytro_Chupryna.execute_analytical_requests(dataframes)


if __name__ == "__main__":
    main()
