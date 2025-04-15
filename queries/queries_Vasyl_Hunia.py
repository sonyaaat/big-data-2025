from pyspark.sql import Window
import pyspark.sql.functions as F
from pyspark.sql.functions import col, desc, first, count, avg, when, array_contains, dense_rank
import os
from pyspark.sql import DataFrame
from typing import Dict


def most_popular_genres_by_region(
        movie_basics: DataFrame,
        akas: DataFrame,
        ratings: DataFrame,
        results_dir: str,
        min_votes: int = 1000) -> None:

    filtered_basics = movie_basics.filter(
        F.col("genres").isNotNull() &
        (F.size("genres") > 0) 
    )

    basics_with_ratings = filtered_basics.join(
        ratings.filter(F.col("numVotes") >= min_votes),
        "tconst",
        "inner"
    )

    region_data = akas.filter(F.col("region").isNotNull())

    pre_join_df = basics_with_ratings.join(
        region_data,
        basics_with_ratings.tconst == region_data.titleId,
        "inner"
    ).drop(region_data.titleId)

    exploded_df = pre_join_df.withColumn("genre", F.explode("genres"))

    filtered_df = exploded_df.filter(F.col("genre").isNotNull())

    filtered_df = filtered_df.persist()

    agg_df = filtered_df.groupBy("region", "genre").agg(
        F.countDistinct(filtered_basics.tconst).alias("title_count"),
        F.avg("averageRating").alias("avg_rating")
    )

    window_spec = Window.partitionBy("region").orderBy(F.desc("title_count"))
    ranked_df = agg_df.withColumn("rank", F.dense_rank().over(window_spec)) \
                      .orderBy("region", "rank")

    filtered_df.unpersist()

    return ranked_df


def yearly_genre_trend_analysis(
        movie_basics: DataFrame,
        ratings: DataFrame,
        results_dir: str,
        min_votes: int = 1000) -> None:

    filtered_basics = movie_basics.filter(
        F.col("startYear").isNotNull() &
        F.col("genres").isNotNull() &
        (F.size("genres") > 0) 
    )

    filtered_ratings = ratings.filter(
        F.col("averageRating").isNotNull() &
        (F.col("numVotes") >= min_votes)
    ).select("tconst", "averageRating", "numVotes")

    join_df = filtered_basics.join(filtered_ratings, "tconst", "inner")

    join_df = join_df.persist()

    exploded_df = join_df.withColumn("genre", F.explode("genres"))

    agg_df = exploded_df.groupBy("startYear", "genre").agg(
        F.count("*").alias("movie_count"),
        F.avg("averageRating").alias("avg_rating"),
        F.avg("numVotes").alias("avg_votes")
    )

    window_spec = Window.partitionBy("startYear").orderBy(F.desc("avg_rating"))
    ranked_df = agg_df.withColumn("rank", F.dense_rank().over(window_spec)) \
                      .orderBy(F.desc("startYear"), "rank")

    join_df.unpersist()

    return ranked_df


def most_successful_directors(
        movie_basics: DataFrame,
        crew: DataFrame,
        ratings: DataFrame,
        name_df: DataFrame,
        results_dir: str,
        min_votes: int = 1000,
        min_movies: int = 10) -> None:

    directors_df = (crew.filter(F.col("directors").isNotNull())
                    .join(movie_basics.select("tconst"), "tconst", "inner")
                    .withColumn("director_id", F.explode("directors"))
                    .join(ratings, "tconst", "inner")
                    .filter((F.col("numVotes") >= min_votes) & F.col("averageRating").isNotNull())
                    .join(name_df.select("nconst", "primaryName"),
                          F.col("director_id") == F.col("nconst"), "left")
                    .select("tconst", F.col("primaryName").alias("director"), "averageRating", "numVotes"))

    directors_agg = (directors_df.groupBy("director")
                     .agg(
                         F.count("*").alias("movie_count"),
                         F.avg("averageRating").alias("avg_rating")
                     )
                     .filter(F.col("movie_count") >= min_movies))

    window_spec_dir = Window.orderBy(F.desc("avg_rating"))

    directors_ranked = (directors_agg.withColumn("rank", F.dense_rank().over(window_spec_dir))
                        .withColumn("role", F.lit("director"))
                        .orderBy("rank"))

    return directors_ranked


def top_actors_by_genre(
        movie_basics: DataFrame,
        principals: DataFrame,
        ratings: DataFrame,
        name_df: DataFrame,
        results_dir: str,
        min_votes: int = 1000,
        min_movies: int = 10,
        high_rating_threshold: float = 7) -> None:

    movies_with_ratings = (movie_basics.join(ratings, "tconst", "inner")
                           .filter(
                               F.col("startYear").isNotNull() &
                               F.col("averageRating").isNotNull() &
                               F.col("genres").isNotNull() &
                               (F.col("numVotes") >= min_votes)
                           )
                           .withColumn("genre", F.explode("genres"))
                           )

    actors_df = (principals.filter(F.col("category").isin("actor", "actress"))
                 .join(movies_with_ratings, "tconst", "inner"))

    actors_with_name = actors_df.join(name_df, "nconst", "inner")

    high_rated_movies = actors_with_name.filter(F.col("averageRating") >= high_rating_threshold)

    actor_movie_count = (actors_with_name.groupBy("primaryName")
                         .agg(F.count("*").alias("movie_count"))
                         .filter(F.col("movie_count") >= min_movies))

    high_rated_movies_filtered = high_rated_movies.join(actor_movie_count, "primaryName", "inner")

    agg_df = high_rated_movies_filtered.groupBy("genre", "primaryName").agg(
        F.count("*").alias("high_rated_movie_count")
    )

    window_spec = Window.partitionBy("genre").orderBy(F.desc("high_rated_movie_count"))

    ranked_df = agg_df.withColumn("rank", F.dense_rank().over(window_spec)) \
                      .orderBy("genre", "rank")

    return ranked_df


def most_successful_genre_combinations(
        movie_basics: DataFrame,
        ratings: DataFrame,
        results_dir: str,
        min_votes: int = 1000) -> None:

    basics_filtered = movie_basics.filter(F.col("genres").isNotNull() & (F.size("genres") > 1))

    basics_sorted = (basics_filtered
                     .withColumn("sorted_genres", F.array_sort("genres"))
                     .withColumn("genre_combo", F.concat_ws(", ", F.col("sorted_genres"))))

    joined = (basics_sorted
              .join(ratings, "tconst", "inner")
              .filter((F.col("averageRating").isNotNull()) & (F.col("numVotes") >= min_votes)))

    agg_df = (joined.groupBy("genre_combo")
              .agg(
                  F.count("*").alias("movie_count"),
                  F.avg("averageRating").alias("avg_rating"),
                  F.max("startYear").alias("latest_year")
              )
              .orderBy(F.desc("latest_year"), F.desc("movie_count"), F.desc("avg_rating")))

    return agg_df


def top_directors_by_genre(
        movie_basics: DataFrame,
        principals: DataFrame,
        name_basics: DataFrame,
        ratings: DataFrame,
        results_dir: str,
        min_votes: int = 1000) -> None:
   
    movies = movie_basics.filter(
        (F.col("titleType") == "movie") & F.col("genres").isNotNull()
    ).select("tconst", "genres")

    movies_with_ratings = movies.join(ratings, "tconst", "inner") \
                                .filter(F.col("numVotes") >= min_votes) \
                                .select("tconst", "genres", "averageRating")

    directors = principals.filter(F.col("category") == "director") \
                          .select("tconst", "nconst")

    movies_with_directors = movies_with_ratings.join(
        directors, "tconst", "inner"
    ).join(
        name_basics.withColumnRenamed("primaryName", "director_name"),
        "nconst",
        "inner"
    ).select(
        "tconst", "genres", "director_name", "averageRating"
    )

    exploded_genres = movies_with_directors.withColumn("genre", F.explode(F.col("genres")))

    director_genre_stats = exploded_genres.groupBy("genre", "director_name").agg(
        F.avg("averageRating").alias("avg_rating"),
        F.count("*").alias("movie_count")
    ).filter(F.col("movie_count") >= 10) 

    window_spec = Window.partitionBy("genre").orderBy(F.desc("avg_rating"), F.desc("movie_count"))
    ranked_directors = director_genre_stats.withColumn("rank", F.row_number().over(window_spec))

    top_directors_by_genre = ranked_directors.filter(F.col("rank") <= 5).orderBy("genre", "rank")

    return top_directors_by_genre
