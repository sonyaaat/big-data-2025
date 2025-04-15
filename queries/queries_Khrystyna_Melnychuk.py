from pyspark.sql import DataFrame
from pyspark.sql.window import Window
import pyspark.sql.functions as F


# Query 1
def top_ua_romantic_comedies(basics: DataFrame, akas: DataFrame, ratings: DataFrame) -> DataFrame:

    romantic_comedies = basics.filter(
        F.array_contains("genres", "Romance") &
        F.array_contains("genres", "Comedy")).select("tconst", "primaryTitle", "titleType", "genres")

    ua_titles = akas.filter((F.col("region") == "UA") & (F.col("isOriginalTitle") == False)) \
        .select("titleId", F.col("title").alias("ukrainianTitle")) \
        .distinct()

    rated = ratings.filter((F.col("averageRating") >= 7) & (F.col("numVotes") >= 1000))

    result = romantic_comedies.join(ua_titles, romantic_comedies.tconst == ua_titles.titleId, "inner") \
        .join(rated, "tconst").select(
            "tconst", "primaryTitle", "ukrainianTitle", "titleType",
            "genres", "averageRating", "numVotes"
        ).orderBy(F.col("averageRating").desc(), F.col("numVotes").desc())

    return result


# Query 2
def videogames_by_director(df_basics: DataFrame, df_crew: DataFrame, df_name: DataFrame) -> DataFrame:

    video_games_with_crew = df_basics.join(df_crew, "tconst", "inner").filter(
        (F.col("directors").isNotNull()) &
        (F.col("titleType") == "videoGame"))

    video_games_with_directors = video_games_with_crew.withColumn("director", F.explode(F.col("directors")))

    video_games_with_directors_and_names = video_games_with_directors.join(
        df_name, video_games_with_directors["director"] == df_name["nconst"], "inner")

    director_video_game_count = video_games_with_directors_and_names.groupBy("primaryName") \
        .agg(F.count("tconst").alias("num_video_games"))

    result = director_video_game_count.orderBy(F.desc("num_video_games"))

    return result


# Query 3
def top_titles_by_randall_ryan(df_name: DataFrame, df_crew: DataFrame, df_basics: DataFrame, df_ratings: DataFrame) -> DataFrame:
    randall_nconst_df = df_name.filter(F.col("primaryName") == "Randall Ryan").select("nconst")
    randall_nconst = [row["nconst"] for row in randall_nconst_df.collect()]

    if not randall_nconst:
        print("Randall Ryan not found in df_name.")
        return
    randall_id = randall_nconst[0]

    relevant_titles = df_crew.filter(
        (F.array_contains(F.col("directors"), randall_id)) |
        (F.array_contains(F.col("writers"), randall_id))
    ).select("tconst")

    titles_with_info = relevant_titles \
        .join(df_basics, "tconst", "inner") \
        .join(df_ratings, "tconst", "inner") \
        .filter((F.col("averageRating") >= 6.5) & (F.col("numVotes") >= 200))

    result = titles_with_info.select("primaryTitle", "titleType", "startYear", "averageRating", "numVotes")\
        .orderBy(F.desc("averageRating"), F.desc("numVotes"))

    return result


# Query 4
def top_rated_series_by_region(df_basics: DataFrame, df_ratings: DataFrame, df_akas: DataFrame) -> DataFrame:

    series_with_ratings = df_basics.join(df_ratings, "tconst", "inner").filter(
        (F.col("averageRating") >= 7) &
        (F.col("numVotes") >= 1000))

    series_with_ratings = series_with_ratings.filter(F.col("titleType") == "tvSeries")

    series_with_region = series_with_ratings \
        .join(df_akas, df_basics["tconst"] == df_akas["titleId"], "inner") \
        .filter(df_akas["region"].isNotNull())

    window_spec = Window.partitionBy("region").orderBy(
        F.desc("averageRating"),
        F.desc("numVotes"),
        F.asc("primaryTitle"))

    ranked_series = series_with_region.withColumn("row_num", F.row_number().over(window_spec))

    top_3_series = ranked_series.filter(F.col("row_num") <= 3)

    result = top_3_series.select("region", "primaryTitle", "averageRating", "numVotes")

    return result


# Query 5
def longest_movie_per_genre(df_basics: DataFrame) -> DataFrame:

    filtered_movies = df_basics.filter(
        (F.col("titleType") == "movie") &
        (F.col("runtimeMinutes").isNotNull()) &
        (F.col("genres").isNotNull()))

    exploded = filtered_movies.withColumn("genre", F.explode("genres"))

    window_spec = Window.partitionBy("genre").orderBy(F.desc("runtimeMinutes"))

    ranked = exploded.withColumn("rank", F.row_number().over(window_spec))

    top_movies = ranked.filter(F.col("rank") == 1)

    result = top_movies.select("genre", "primaryTitle", "runtimeMinutes")

    return result


# Query 6
def longest_series_with_ratings(df_basics: DataFrame, df_episode: DataFrame, df_ratings: DataFrame) -> DataFrame:

    df_basics_renamed = df_basics.withColumnRenamed("tconst", "series_tconst")

    df_series_episodes = df_basics_renamed.join(
        df_episode, df_episode["parentTconst"] == df_basics_renamed["series_tconst"], "left_outer")

    series_episode_count = df_series_episodes.groupBy("primaryTitle", "series_tconst") \
        .agg(F.count("episodeNumber").alias("num_episodes"))

    df_with_ratings = series_episode_count.join(
        df_ratings, series_episode_count["series_tconst"] == df_ratings["tconst"], "inner")

    result = df_with_ratings.select("primaryTitle", "num_episodes", "averageRating")\
        .orderBy(F.desc("num_episodes"))

    return result