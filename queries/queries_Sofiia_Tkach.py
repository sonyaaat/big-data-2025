from pyspark.sql import Window
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.functions import desc
from imdb_spark_utils import export_result
from typing import Dict
from config import Config


def highest_rated_ukraine_titles(videogames: DataFrame, tv_episodes: DataFrame, ratings: DataFrame) -> DataFrame:
    ukraine_titles = videogames.filter(
        F.col("primaryTitle").contains("Ukraine") | F.col("originalTitle").contains("Ukraine")
    ).union(
        tv_episodes.filter(
            F.col("primaryTitle").contains("Ukraine") | F.col("originalTitle").contains("Ukraine")
        )
    )

    highest_rated_ukraine = ukraine_titles.join(
        ratings, ukraine_titles["tconst"] == ratings["tconst"]
    ).filter(
        (F.col("averageRating") >= 7.5) & (F.col("numVotes") >= 20)
    ).select(
        "originalTitle", "averageRating", "numVotes", "startYear"
    ).orderBy(F.desc("averageRating"))

    return highest_rated_ukraine


def most_popular_genres_by_count(videogames: DataFrame) -> DataFrame:
    current_year = 2025
    videogames_last_5_years = videogames.filter(
        (F.col("startYear") >= current_year - 5) & (F.col("startYear").isNotNull())
    )

    videogames_last_5_years = videogames_last_5_years.withColumn(
        "genre", F.explode(F.col("genres"))
    )

    genre_counts = videogames_last_5_years.groupBy("genre").count()

    most_frequent_title_in_genre = videogames_last_5_years.groupBy("genre") \
        .agg(F.first("primaryTitle").alias("mostFrequentTitle"))

    genre_summary = genre_counts.join(
        most_frequent_title_in_genre, "genre", "left"
    )

    top_genres_by_count = genre_summary.orderBy(desc("count")).limit(20)

    return top_genres_by_count


def top_recent_videogames(videogames: DataFrame, ratings: DataFrame) -> DataFrame:
    recent_videogames = videogames.join(ratings, "tconst") \
        .filter(
        (videogames["startYear"] >= 2015) &
        (ratings["averageRating"] >= 7.5) &
        (ratings["numVotes"] >= 10000)
    ) \
        .select(
        videogames["primaryTitle"],
        videogames["startYear"],
        ratings["averageRating"],
        ratings["numVotes"]
    )

    top_recent = recent_videogames.orderBy(F.desc("averageRating"), F.desc("numVotes"))

    return top_recent


def most_localized_videogames(videogames: DataFrame, akas: DataFrame) -> DataFrame:
    localized_games = videogames.join(
        akas, videogames["tconst"] == akas["titleId"], "inner"
    )

    localized_games = localized_games.filter(
        (F.col("language") != "und") &
        (F.col("titleType") == "videoGame")
    )

    localized_games_count = localized_games.groupBy("primaryTitle").agg(
        F.countDistinct("region").alias("localization_count")
    )

    result = localized_games_count.orderBy(desc("localization_count"))

    return result


def top_video_games_by_rating_growth(videogames: DataFrame, ratings: DataFrame) -> DataFrame:
    joined_data = videogames.join(ratings, videogames["tconst"] == ratings["tconst"])

    window_spec = Window.partitionBy("primaryTitle").orderBy("startYear")

    joined_data = joined_data.withColumn(
        "previous_rating",
        F.lag("averageRating").over(window_spec)
    )

    joined_data = joined_data.withColumn(
        "rating_growth", F.round(F.col("averageRating") - F.col("previous_rating"), 2)
    )

    top_growth_games = joined_data.filter(
        (F.col("rating_growth") > 0) & (F.col("numVotes") >= 500)
    ).select(
        "primaryTitle", "startYear", "averageRating", "numVotes", "rating_growth"
    ).orderBy(
        F.desc("rating_growth")
    )
    return top_growth_games


def top_actors_in_highest_rated_tv_episodes(tv_episodes: DataFrame, principals: DataFrame, ratings: DataFrame,
                                            names: DataFrame) -> DataFrame:
    tv_with_ratings = tv_episodes.join(ratings, tv_episodes["tconst"] == ratings["tconst"], "inner") \
        .select(tv_episodes["tconst"].alias("tv_tconst"), tv_episodes["startYear"], ratings["averageRating"])

    window_spec = Window.partitionBy("startYear").orderBy(F.desc("averageRating"))

    tv_with_ratings = tv_with_ratings.withColumn("rank", F.dense_rank().over(window_spec))

    top_episodes = tv_with_ratings.filter(F.col("rank") == 1)

    top_actors = top_episodes.join(principals, top_episodes["tv_tconst"] == principals["tconst"], "inner") \
        .select(principals["nconst"].alias("actor_nconst"), principals["category"], top_episodes["averageRating"],
                top_episodes["startYear"])

    top_actors = top_actors.filter(F.col("category") == "actor")

    top_actors_with_names = top_actors.join(names, top_actors["actor_nconst"] == names["nconst"], "inner") \
        .select(names["primaryName"], top_actors["actor_nconst"], "averageRating", top_actors["startYear"])

    actor_role_count = top_actors_with_names.groupBy("primaryName", "startYear") \
        .agg(F.count("actor_nconst").alias("roleCount"), F.avg("averageRating").alias("avgRating")) \
        .orderBy(F.desc("roleCount"), F.desc("avgRating"))

    return actor_role_count


def execute_analytical_requests(dataframes: Dict[str, DataFrame]):
    # Extract necessary DataFrames
    videogames = dataframes["basics"].filter(F.col("titleType") == "videoGame")
    tv_episodes = dataframes["basics"].filter(F.col("titleType") == "tvEpisode")
    ratings = dataframes["ratings"]
    akas = dataframes["akas"]
    principals = dataframes["principals"]
    names = dataframes["name"]

    # 1. Highest rated titles from Ukraine
    highest_rated_ukraine = highest_rated_ukraine_titles(videogames, tv_episodes, ratings)
    export_result(highest_rated_ukraine, f"{Config.RESULT_DIR}/highest_rated_ukraine_titles.csv",
                  title="Highest rated titles from Ukraine")

    # 2. Most popular video game genres by count
    most_popular_video_games_genres = most_popular_genres_by_count(videogames)
    export_result(most_popular_video_games_genres, f"{Config.RESULT_DIR}/most_popular_video_games_genres.csv",
                  title="Most popular video games genres by count")

    # 3. Top-rated video games released after 2015 with more than 10,000 votes
    top_recent_games = top_recent_videogames(videogames, ratings)
    export_result(top_recent_games, f"{Config.RESULT_DIR}/top_recent_games.csv",
                  title="Top-rated video games released after 2015 with more than 10000 votes")

    # 4. Most localized video games
    localized_games = most_localized_videogames(videogames, akas)
    export_result(localized_games, f"{Config.RESULT_DIR}/localized_games.csv", title="Most localized video games")

    # 5. Top video games by rating growth
    top_rating_growth_games = top_video_games_by_rating_growth(videogames, ratings)
    export_result(top_rating_growth_games, f"{Config.RESULT_DIR}/top_rating_growth_games.csv",
                  title="Top video games by rating growth")

    # 6. Top actors in highest-rated TV episodes
    top_actors_highest_rated = top_actors_in_highest_rated_tv_episodes(tv_episodes, principals, ratings, names)
    export_result(top_actors_highest_rated, f"{Config.RESULT_DIR}/top_actors_highest_rated_tv_episodes.csv",
                  title="Top actors in highest rated TV episodes with names and ratings")
