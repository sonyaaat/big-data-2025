import pyspark.sql.types as t

# Схема для файлу title.akas.tsv
schema_title_akas = t.StructType([
    t.StructField("titleId", t.StringType(), nullable=False),
    t.StructField("ordering", t.IntegerType(), nullable=True),
    t.StructField("title", t.StringType(), nullable=True),
    t.StructField("region", t.StringType(), nullable=True),
    t.StructField("language", t.StringType(), nullable=True),
    t.StructField("types", t.StringType(), nullable=True),
    t.StructField("attributes", t.StringType(), nullable=True),
    t.StructField("isOriginalTitle", t.StringType(), nullable=True)
])

# Схема для файлу title.basics.tsv
schema_title_basics = t.StructType([
    t.StructField("tconst", t.StringType(), nullable=False),
    t.StructField("titleType", t.StringType(), nullable=True),
    t.StructField("primaryTitle", t.StringType(), nullable=True),
    t.StructField("originalTitle", t.StringType(), nullable=True),
    t.StructField("isAdult", t.BooleanType(), nullable=True),
    t.StructField("startYear", t.IntegerType(), nullable=True),
    t.StructField("endYear", t.IntegerType(), nullable=True),
    t.StructField("runtimeMinutes", t.IntegerType(), nullable=True),
    t.StructField("genres", t.StringType(), nullable=True)
])

# Схема для файлу title.crew.tsv
schema_title_crew = t.StructType([
    t.StructField("tconst", t.StringType(), nullable=False),
    t.StructField("directors", t.StringType(), nullable=True),
    t.StructField("writers", t.StringType(), nullable=True)
])

# Схема для файлу title.episode.tsv
schema_title_episode = t.StructType([
    t.StructField("tconst", t.StringType(), nullable=False),
    t.StructField("parentTconst", t.StringType(), nullable=False),
    t.StructField("seasonNumber", t.IntegerType(), nullable=True),
    t.StructField("episodeNumber", t.IntegerType(), nullable=True)
])

# Схема для файлу title.principals.tsv
schema_title_principals = t.StructType([
    t.StructField("tconst", t.StringType(), nullable=False),
    t.StructField("ordering", t.IntegerType(), nullable=True),
    t.StructField("nconst", t.StringType(), nullable=False),
    t.StructField("category", t.StringType(), nullable=True),
    t.StructField("job", t.StringType(), nullable=True),
    t.StructField("characters", t.StringType(), nullable=True)
])

# Схема для файлу title.ratings.tsv
schema_title_ratings = t.StructType([
    t.StructField("tconst", t.StringType(), nullable=False),
    t.StructField("averageRating", t.DoubleType(), nullable=False),
    t.StructField("numVotes", t.IntegerType(), nullable=True)
])

# Схема для файлу name.basics.tsv
schema_name_basics = t.StructType([
    t.StructField("nconst", t.StringType(), nullable=False),
    t.StructField("primaryName", t.StringType(), nullable=True),
    t.StructField("birthYear", t.IntegerType(), nullable=True),
    t.StructField("deathYear", t.IntegerType(), nullable=True),
    t.StructField("primaryProfession", t.StringType(), nullable=True),
    t.StructField("knownForTitles", t.StringType(), nullable=True)
])
