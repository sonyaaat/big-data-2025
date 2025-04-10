from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.types as t
import pyspark.sql.functions as F
from typing import List, Optional


def initialize_spark(app_name: str = "IMDB Data Processor") -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def load_dataframe(spark: SparkSession, schema: t.StructType, file_path: str) -> DataFrame:
    return spark.read.option("delimiter", "\t") \
        .option("header", "true") \
        .option("nullValue", "\\N") \
        .option("dateFormat", "MM/dd/yyyy") \
        .schema(schema) \
        .csv(file_path)


def clean_null_values(df: DataFrame) -> DataFrame:
    return df.replace("\\N", None)


def transform_title_basics(df: DataFrame) -> DataFrame:
    df = clean_null_values(df)
    df = df.withColumn("genres", F.split(F.col("genres"), ",").cast("array<string>"))
    df = df.withColumn("isAdult", F.col("isAdult").cast(t.BooleanType()))
    return df


def transform_title_akas(df: DataFrame) -> DataFrame:
    df = clean_null_values(df)
    df = df.withColumn("isOriginalTitle", F.col("isOriginalTitle").cast(t.BooleanType()))
    df = df.withColumn("types", F.split(F.col("types"), ",").cast("array<string>"))
    df = df.withColumn("attributes", F.split(F.col("attributes"), ",").cast("array<string>"))
    return df


def transform_title_crew(df: DataFrame) -> DataFrame:
    df = clean_null_values(df)
    df = df.withColumn("directors", F.split(F.col("directors"), ",").cast("array<string>"))
    df = df.withColumn("writers", F.split(F.col("writers"), ",").cast("array<string>"))
    return df


def transform_title_episode(df: DataFrame) -> DataFrame:
    return clean_null_values(df)


def display_dataframe_info(df: DataFrame, name: str) -> None:
    print(f"\n=== {name} ===")
    print("First 5 rows:")
    df.show(5, truncate=False)

    print("Schema:")
    df.printSchema()

    print("Columns:", df.columns)
    print("Number of columns:", len(df.columns))
    print("Number of rows:", df.count())


def display_numerical_statistics(df, columns: list[str]):
    print("\nStatistics for numerical columns:")
    df.describe(columns).show()


def display_categorical_distincts(df: DataFrame, name: str, max_distinct: int = 20, sample_size: int = 5) -> None:
    print(f"\nDistinct categorical values for '{name}' (≤ {max_distinct} unique values):\n")

    string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, t.StringType)]

    for col_name in string_cols:
        distinct_vals = df.select(col_name).distinct()
        distinct_count = distinct_vals.count()

        # displaying as categories only when there are <= 20 distinct vals
        if distinct_count <= max_distinct:
            sample_vals = distinct_vals.limit(sample_size).rdd.flatMap(lambda x: x).collect()
            print(f"  Column: {col_name}")
            print(f"  Distinct values: {distinct_count}")
            print(f"  Sample: {sample_vals}")  # 5 samples of categories of given column
            print("   -----")


def correlation_runtime_rating(basics_df, ratings_df):  # correlation between runtime and average rating of the movies
    title_data = basics_df.join(ratings_df, on="tconst")

    # Step 1: Filter out rows with missing or null values for runtimeMinutes and averageRating
    title_data = title_data.filter(F.col("runtimeMinutes").isNotNull() & F.col("averageRating").isNotNull())

    # Step 2: Ensure columns are of appropriate types
    title_data = title_data.withColumn("runtimeMinutes", F.col("runtimeMinutes").cast("int"))
    title_data = title_data.withColumn("averageRating", F.col("averageRating").cast("float"))

    correlation = title_data.stat.corr("runtimeMinutes", "averageRating")

    print(f"Correlation between runtime and average rating: {correlation}")

    return correlation


def display_title_type_info(df: DataFrame) -> None:  # titleType & count of corresponding rows & average runtime
    # number of each titleType
    title_type_counts = df.groupBy("titleType").count()

    # average runtime for each titleType
    avg_runtime_df = df.filter(df.runtimeMinutes.isNotNull()) \
        .groupBy("titleType") \
        .agg(F.avg("runtimeMinutes").alias("average_runtime"))

    # joining results
    joined_df = title_type_counts.join(avg_runtime_df, on="titleType", how="left")

    # if average runtime is NULL (runtime values are not given)
    joined_df = joined_df.fillna({"average_runtime": 0})

    print("TitleType counts and average runtime:")
    joined_df.show(truncate=False)


def transform_title_ratings(df: DataFrame) -> DataFrame:
    return clean_null_values(df)


def transform_title_principals(df: DataFrame) -> DataFrame:
    return clean_null_values(df)


def transform_name_basics(df: DataFrame) -> DataFrame:
    df = clean_null_values(df)
    df = df.withColumn("primaryProfession", F.split(F.col("primaryProfession"), ",").cast("array<string>"))
    df = df.withColumn("knownForTitles", F.split(F.col("knownForTitles"), ",").cast("array<string>"))
    return df
