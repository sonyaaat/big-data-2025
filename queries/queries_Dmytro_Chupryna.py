from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from imdb_spark_utils import export_result
from typing import Dict
import config


def get_ukrainian_titles(akas: DataFrame) -> DataFrame:
    return akas.filter(F.col("language") == "uk") \
               .select("titleId", "title")
 

def get_long_movies_with_ranking(movie_basics: DataFrame, ratings: DataFrame) -> DataFrame:
    window_spec = Window.orderBy(F.desc("averageRating"))
    return movie_basics.filter(F.col("runtimeMinutes") > 120) \
        .join(ratings, on="tconst") \
        .withColumn("rank_among_long", F.dense_rank().over(window_spec)) \
        .select("primaryTitle", "runtimeMinutes", "averageRating", "rank_among_long")
 

def get_top_rated_movies(movie_basics: DataFrame, ratings: DataFrame) -> DataFrame:
    return ratings.filter((F.col("averageRating") > 8) & (F.col("numVotes") > 10000)) \
        .join(movie_basics, on="tconst") \
        .select("tconst", "primaryTitle", "averageRating", "numVotes")
 

def get_genre_avg_ratings(movie_basics: DataFrame, ratings: DataFrame) -> DataFrame:
    return movie_basics.join(ratings, on="tconst") \
        .withColumn("genre", F.explode("genres")) \
        .groupBy("genre") \
        .agg(
            F.count("*").alias("count"),
            F.avg("averageRating").alias("avg_rating")
        ) \
        .orderBy(F.desc("avg_rating"))
 

def get_top_productive_actors(principals: DataFrame, names: DataFrame) -> DataFrame:
    return principals.filter(F.col("category") == "actor") \
        .groupBy("nconst") \
        .agg(F.count("*").alias("film_count")) \
        .filter(F.col("film_count") >= 10) \
        .orderBy(F.desc("film_count")) \
        .limit(3) \
        .join(names, on="nconst") \
        .select("primaryName", "film_count")
 

def get_top_actors_in_high_rated_popular_movies(dataframes):
 
    high_rated_movies = dataframes["ratings"] \
        .filter((F.col("averageRating") > 8.0) & (F.col("numVotes") > 1000)) \
        .select("tconst")
 
    actors_in_good_movies = dataframes["principals"] \
        .filter(F.col("category") == "actor") \
        .join(high_rated_movies, on="tconst") \
        .groupBy("nconst") \
        .agg(F.count("*").alias("high_rating_appearances")) \
        .orderBy(F.desc("high_rating_appearances")) \
        .limit(10)
 
    top_actors_in_good_movies = actors_in_good_movies \
        .join(dataframes["name"], on="nconst") \
        .select("primaryName", "high_rating_appearances")
    return top_actors_in_good_movies
 

def execute_analytical_requests(dataframes: Dict[str, DataFrame]) -> None:
    basics = dataframes["basics"]
    movie_basics = basics.filter(F.col("titleType") == "movie")

    request1_result = get_ukrainian_titles(dataframes["akas"])
    export_result(request1_result, f"{config.RESULT_DIR}/get_ukrainian_titles",
                  title="What movies have Ukrainian titles?")

    request2_result = get_long_movies_with_ranking(movie_basics, dataframes["ratings"])
    export_result(request2_result, f"{config.RESULT_DIR}/get_long_movies_with_ranking",
                  title="Which movies are longer than 2 hours and have the highest rating?")

    request3_result = get_top_rated_movies(movie_basics, dataframes["ratings"])
    export_result(request3_result, f"{config.RESULT_DIR}/get_top_rated_movies",
                  title="Which movies have a rating of more than 8 and more than 10,000 votes?")

    request4_result = get_genre_avg_ratings(movie_basics, dataframes["ratings"])
    export_result(request4_result, f"{config.RESULT_DIR}/get_genre_avg_ratings",
                  title="What is the average rating of movies in each genre?")

    request5_result = get_top_productive_actors(dataframes['principals'], dataframes["name"])
    export_result(request5_result, f"{config.RESULT_DIR}/get_top_productive_actors",
                  title="Who are the top 3 most productive actors based on the number of films they have appeared in, among those who have acted in at least 10 films?")

    request6_result = get_top_actors_in_high_rated_popular_movies(dataframes)
    export_result(request6_result, f"{config.RESULT_DIR}/get_top_actors_in_high_rated_popular_movies",
                  title="Which actors most often star in movies with a high rating (over 8.0)?")
