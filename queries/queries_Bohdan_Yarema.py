from pyspark.sql import Window
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.functions import desc


def find_top_rated_recent_movies(basics_df: DataFrame, ratings_df: DataFrame) -> DataFrame:
    """
    Request 1: Find top 10 highest-rated movies with at least 5,000 votes released after 2000.
    """
    print("\nExecuting Request 1: Top 10 Highest-Rated Recent Movies")
    movies_df = basics_df.filter(F.col("titleType") == "movie")

    result = movies_df.join(ratings_df, "tconst") \
                     .filter((F.col("numVotes") >= 5000) & (F.col("startYear") > 2000)) \
                     .select("tconst", "primaryTitle", "startYear", "genres", "averageRating", "numVotes") \
                     .orderBy(F.col("averageRating").desc()) \
                     .limit(10)

    return result


def find_popular_actors_in_highly_rated_movies(basics_df: DataFrame, ratings_df: DataFrame,
                                                principals_df: DataFrame, name_df: DataFrame) -> DataFrame:
    """
    Request 2: Find actors who starred in movies with average rating > 8.2 and have at least 3 known for titles.
    """
    print("\nExecuting Request 2: Popular Actors in Highly Rated Movies")
    highly_rated_movies = basics_df.join(ratings_df, "tconst") \
                                    .filter((F.col("titleType") == "movie") & (F.col("averageRating") > 8.2)) \
                                    .select("tconst")

    actors_in_movies = principals_df.join(highly_rated_movies, "tconst") \
                                     .filter(F.col("category").isin("actor", "actress")) \
                                     .select("nconst", "tconst")

    actor_known_titles = name_df.filter(F.size(F.col("knownForTitles")) >= 3) \
                                .select("nconst", "primaryName")

    result = actors_in_movies.join(actor_known_titles, "nconst") \
                             .groupBy("nconst", "primaryName") \
                             .agg(F.countDistinct("tconst").alias("movies_starred_in")) \
                             .orderBy(F.col("movies_starred_in").desc())

    return result


def calculate_genre_popularity_rating(basics_df: DataFrame, ratings_df: DataFrame) -> DataFrame:
    """
    Request 3: Calculate the average number of votes and average rating for each genre.
    """
    print("\nExecuting Request 3: Average Votes and Rating by Genre")
    movies_with_ratings = basics_df.join(ratings_df, "tconst")
    exploded_genres = movies_with_ratings.select(F.explode("genres").alias("genre"), "averageRating", "numVotes")

    result = exploded_genres.groupBy("genre") \
                            .agg(F.avg("averageRating").alias("avg_rating"),
                                 F.avg("numVotes").alias("avg_votes")) \
                            .orderBy(F.col("avg_rating").desc())

    return result


def average_runtime_by_title_type(basics_df: DataFrame) -> DataFrame:
    """
    Request 4: Find the average runtime for each title type, filtering out title types with fewer than 1000 entries.
    """
    print("\nExecuting Request 4: Average Runtime by Title Type (>= 1000 entries)")
    title_type_counts = basics_df.groupBy("titleType").count().filter(F.col("count") >= 1000)

    result = basics_df.join(title_type_counts, "titleType") \
                      .groupBy("titleType") \
                      .agg(F.avg("runtimeMinutes").alias("avg_runtime")) \
                      .orderBy(F.col("avg_runtime").desc())

    return result


def rank_movies_by_rating_within_genre(basics_df: DataFrame, ratings_df: DataFrame) -> DataFrame:
    """
    Request 5: Rank movies within each genre based on their average rating.
    """
    print("\nExecuting Request 5: Rank Movies by Rating within Genre")
    movies_with_ratings = basics_df.join(ratings_df, "tconst")
    exploded_genres = movies_with_ratings.select("tconst", "primaryTitle", F.explode("genres").alias("genre"), "averageRating")

    window_spec = Window.partitionBy("genre").orderBy(F.col("averageRating").desc())

    result = exploded_genres.withColumn("rank_within_genre", F.rank().over(window_spec)) \
                            .orderBy("genre", "rank_within_genre")

    return result


def cumulative_avg_rating_tv_series(basics_df: DataFrame, ratings_df: DataFrame, episode_df: DataFrame) -> DataFrame:
    """
    Request 6: Calculate the cumulative average rating of episodes within each TV series,
               ordered by season and episode number (uses window functions and joins).
    """
    print("\nExecuting Request 6: Cumulative Average Rating of Episodes within TV Series")
    tv_episodes = basics_df.filter(F.col("titleType") == "tvEpisode")
    episodes_with_ratings = tv_episodes.join(ratings_df, "tconst") \
                                       .join(episode_df, "tconst") \
                                       .join(basics_df.alias("series"), F.col("parentTconst") == F.col("series.tconst")) \
                                       .select(F.col("series.primaryTitle").alias("series_title"),
                                               "seasonNumber", "episodeNumber", "averageRating") \
                                       .orderBy("series_title", "seasonNumber", "episodeNumber")

    window_spec = Window.partitionBy("series_title").orderBy("seasonNumber", "episodeNumber")

    result = episodes_with_ratings.withColumn("cumulative_avg_rating", F.avg("averageRating").over(window_spec))

    return result