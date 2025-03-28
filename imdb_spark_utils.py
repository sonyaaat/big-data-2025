from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.types as t
import pyspark.sql.functions as F


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

