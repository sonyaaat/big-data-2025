from pyspark.sql import SparkSession
import os


DATA_DIR = "imdb_data"


def check_pyspark() -> None:
    print("Initializing Spark Session...")
    spark = SparkSession.builder \
        .appName("IMDB Data Check") \
        .getOrCreate()
    
    print("Spark Session initialized.")
    print("Checking available files...")
    files = [f for f in os.listdir(DATA_DIR) if f.endswith(".tsv")]
    
    if not files:
        print("No TSV files found. Make sure data is downloaded and extracted.")
        return
    
    print("Found files:", files)

    sample_file = os.path.join(DATA_DIR, files[0])
    print(f"Loading sample file: {sample_file}")
    
    df = spark.read.option("header", "true").option("sep", "\t").csv(sample_file)
    
    print("Schema of the loaded file:")
    df.printSchema()
    
    print("Showing first 5 rows:")
    df.show(5)
    
    print("PySpark check complete!")
    
    spark.stop()


if __name__ == "__main__":
    check_pyspark()