from pyspark.sql import Window, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.functions import desc


def english_high_rated_movies(basics: DataFrame, ratings: DataFrame) -> DataFrame:
    result = basics.filter((F.col("titleType") == "movie") &
                           (F.col("startYear").isNotNull())) \
                   .join(ratings, "tconst", "inner") \
                   .filter(F.col("averageRating") > 7.0) \
                   .select("tconst", "primaryTitle", "startYear", "averageRating", "numVotes")
    return result


def directors_best_decade(basics: DataFrame, ratings: DataFrame, crew: DataFrame, name_df: DataFrame) -> DataFrame:
    movies = basics.filter((F.col("titleType") == "movie") & F.col("startYear").isNotNull())

    joined = movies.join(ratings, "tconst") \
                   .join(crew, "tconst") \
                   .filter(F.col("directors").isNotNull())

    with_decade = joined.withColumn("decade", (F.col("startYear").cast("int") / 10).cast("int") * 10)
    exploded = with_decade.withColumn("director", F.explode("directors"))

    grouped = exploded.groupBy("director", "decade").agg(
        F.avg("averageRating").alias("avg_rating"),
        F.count("*").alias("movie_count")
    )

    windowSpec = Window.partitionBy("director").orderBy(F.desc("avg_rating"))

    best_decade = grouped.withColumn("rank", F.row_number().over(windowSpec)) \
                         .filter(F.col("rank") == 1) \
                         .drop("rank")

    result = best_decade.join(name_df, best_decade.director == name_df.nconst, "left") \
                        .select("primaryName", "director", "decade", "avg_rating", "movie_count") \
                        .orderBy(F.desc("avg_rating"))

    return result

def top_thriller_directors(basics: DataFrame, ratings: DataFrame, crew: DataFrame, name_df: DataFrame) -> DataFrame:
    thriller_movies = basics.filter((F.array_contains(F.col("genres"), "Thriller")) &
                                    (F.col("startYear").isNotNull()))
    with_ratings = thriller_movies.join(ratings, "tconst")
    with_directors = with_ratings.join(crew, "tconst").filter(F.col("directors").isNotNull())
    exploded = with_directors.withColumn("director", F.explode("directors"))

    result = exploded.groupBy("director").agg(
        F.count("tconst").alias("film_count"),
        F.avg("averageRating").alias("avg_rating")
    ).filter((F.col("film_count") >= 5) & (F.col("avg_rating") >= 7.0)).orderBy(F.desc("avg_rating"))

    final = result.join(name_df, result.director == name_df.nconst, "left") \
                  .select("director", "film_count", "avg_rating", "primaryName")
    return final


def high_rated_documentaries(basics: DataFrame, ratings: DataFrame) -> DataFrame:
    documentaries = basics.filter((F.col("titleType") == "movie") &
                                  (F.col("genres").isNotNull()) &
                                  (F.array_contains(F.col("genres"), "Documentary")))
    docs_with_ratings = documentaries.join(ratings, "tconst").filter(F.col("averageRating") > 7.5)
    result = docs_with_ratings.select("tconst", "primaryTitle", "averageRating", "numVotes")
    return result


def dual_role_persons(principals: DataFrame, crew: DataFrame, name_df: DataFrame) -> DataFrame:
    actors = principals.filter(F.col("category").isin("actor", "actress")).select("nconst").distinct()
    directors = crew.filter(F.col("directors").isNotNull()) \
                    .withColumn("director", F.explode("directors")) \
                    .select(F.col("director").alias("nconst")).distinct()

    dual_role = actors.join(directors, "nconst", "inner")

    result = dual_role.join(name_df, "nconst", "left") \
                      .filter(F.col("primaryName").isNotNull()) \
                      .filter(F.col("primaryName").rlike("^[A-Za-z .'-]+$")) \
                      .select("nconst", "primaryName")
    return result


def versatile_directors(basics: DataFrame, ratings: DataFrame, crew: DataFrame, name_df: DataFrame) -> DataFrame:
    movies = basics.filter((F.col("titleType") == "movie") & (F.col("genres").isNotNull()))

    joined = movies.join(ratings, "tconst") \
        .join(crew, "tconst") \
        .filter(F.col("directors").isNotNull())

    exploded = joined.withColumn("director", F.explode("directors")) \
        .withColumn("genre", F.explode("genres"))

    stats = exploded.groupBy("director").agg(
        F.countDistinct("genre").alias("genre_count"),
        F.avg("averageRating").alias("avg_rating")
    ).filter((F.col("genre_count") >= 3) & (F.col("avg_rating") > 7.5))

    result = stats.join(name_df, stats.director == name_df.nconst, "left") \
        .select("primaryName", "genre_count", "avg_rating") \
        .orderBy(F.desc("avg_rating"))

    return result
