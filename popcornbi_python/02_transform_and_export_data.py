from modules.config_pyspark import spark
from modules.config_general import languages, logger
from pyspark.sql.functions import (
    col, to_date, year, quarter, month, dayofmonth, weekofyear, dayofweek,
    date_format, when, lit, explode, split, row_number, trim,
    from_json, regexp_replace, array, udf, isnan
)
from pyspark.sql.types import (
    IntegerType, BooleanType, StructType, StructField, StringType,
    ArrayType, DoubleType, FloatType
)
from pyspark.sql.window import Window

# Load data
movies_spark_df = spark.read.parquet("/Users/marcoo_sg/Desktop/PopcornBI/project_data/cleaned_df/movies_df_cleaned.parquet")
movies_extended_spark_df = spark.read.parquet("/Users/marcoo_sg/Desktop/PopcornBI/project_data/cleaned_df/movie_extended_df_cleaned.parquet")
dim_ratings_df = spark.read.parquet("/Users/marcoo_sg/Desktop/PopcornBI/project_data/cleaned_df/ratings_df_cleaned.parquet")

# -------------------- Fact Table and Dimension Tables --------------------

# dim_date
dim_date_df = (
    movies_spark_df
    .select(to_date("release_date").alias("full_date"))
    .filter(col("full_date").isNotNull())
    .distinct()
    .orderBy("full_date")
    .select(
        date_format("full_date", "yyyyMMdd").cast(IntegerType()).alias("date_id"),
        col("full_date").cast(StringType()),
        year("full_date").alias("year"),
        quarter("full_date").alias("quarter"),
        month("full_date").alias("month"),
        date_format("full_date", "MMMM").alias("month_name"),
        dayofmonth("full_date").alias("day"),
        weekofyear("full_date").alias("week_of_year"),
        dayofweek("full_date").alias("day_of_week"),
        date_format("full_date", "EEEE").alias("day_name"),
        (dayofweek("full_date") >= 6).cast(BooleanType()).alias("is_weekend")
    )
)

# fact_movies
fact_movies_df = (
    movies_spark_df
    .withColumn("date_id", date_format(to_date("release_date"), "yyyyMMdd").cast(IntegerType()))
    .drop("release_date")
)

# dim_genre
dim_genre_df = (
    movies_extended_spark_df
    .select(explode(split("genres", ",")).alias("genre_name"))
    .filter(trim(col("genre_name")) != "")
    .distinct()
    .orderBy("genre_name")
    .withColumn("genre_id", row_number().over(Window.orderBy("genre_name")))
)

# dim_production_company
dim_production_company_df = (
    movies_extended_spark_df
    .select(explode(split("production_companies", ",")).alias("company_name"))
    .filter(trim(col("company_name")) != "")
    .distinct()
    .orderBy("company_name")
    .withColumn("company_id", row_number().over(Window.orderBy("company_name")))
    .select("company_id", "company_name")
)

# dim_production_countries
country_schema = ArrayType(StructType([
    StructField("iso_3166_1", StringType(), True),
    StructField("name", StringType(), True)
]))

dim_production_countries_df = (
    movies_extended_spark_df
    .filter(col("production_countries").isNotNull())
    .withColumn("cleaned_production_countries", regexp_replace("production_countries", "'", '"'))
    .select(explode(
        from_json("cleaned_production_countries", country_schema)
    ).alias("country"))
    .filter(col("country.iso_3166_1").isNotNull() & col("country.name").isNotNull())
    .selectExpr("country.iso_3166_1", "country.name")
    .distinct()
    .orderBy("iso_3166_1", "name")
)

# dim_spoken_language
language_schema = ArrayType(StructType([
    StructField("iso_639_1", StringType(), True),
    StructField("name", StringType(), True)
]))

manual_language_mapping = {
    'sh': 'Serbo-Croatian',
    'kn': 'Kannada'
}

def get_language_name(iso_code):
    if iso_code in manual_language_mapping:
        return manual_language_mapping[iso_code]
    try:
        return languages.get(part1=iso_code).name
    except KeyError:
        return None

get_language_name_udf = udf(get_language_name, StringType())

dim_spoken_language_df = (
    movies_extended_spark_df
    .filter(col("spoken_languages").isNotNull())
    .select(explode(
        from_json("spoken_languages", language_schema)
    ).alias("language"))
    .filter(col("language.iso_639_1").isNotNull())
    .withColumn("name", when(
        (col("language.name").isNull()) | (col("language.name") == "") | (col("language.name") == "?????"),
        get_language_name_udf("language.iso_639_1")
    ).otherwise(col("language.name")))
    .selectExpr("language.iso_639_1", "name")
    .distinct()
    .orderBy("iso_639_1", "name")
)

# -------------------- Bridge Tables --------------------

br_movie_genres_df = (
    movies_extended_spark_df.alias("m")
    .select(col("m.id").alias("movie_id"), explode(split("m.genres", ",")).alias("m_genre_name"))
    .join(dim_genre_df.alias("dg"), col("m_genre_name") == col("dg.genre_name"))
    .select("movie_id", "genre_id")
    .distinct()
)

br_movie_companies_df = (
    movies_extended_spark_df.alias("m")
    .select(col("m.id").alias("movie_id"), explode(split("m.production_companies", ",")).alias("m_company_name"))
    .join(dim_production_company_df.alias("dpc"), col("m_company_name") == col("dpc.company_name"))
    .select("movie_id", "company_id")
    .distinct()
)

br_movie_countries_df = (
    movies_extended_spark_df
    .withColumn("cleaned_production_countries", regexp_replace("production_countries", "'", '"'))
    .select(col("id").alias("movie_id"), explode(from_json("cleaned_production_countries", country_schema)).alias("country"))
    .join(dim_production_countries_df.alias("dpcn"), col("country.iso_3166_1") == col("dpcn.iso_3166_1"))
    .select("movie_id", "iso_3166_1")
    .distinct()
)

br_movie_languages_df = (
    movies_extended_spark_df
    .select(col("id").alias("movie_id"), explode(from_json("spoken_languages", language_schema)).alias("language"))
    .join(dim_spoken_language_df.alias("dsl"), col("language.iso_639_1") == col("dsl.iso_639_1"))
    .select("movie_id", "iso_639_1")
    .distinct()
)

# -------------------- Write to MySQL --------------------

# Database connection properties
db_url = "jdbc:mysql://localhost:3306/popcornbi?rewriteBatchedStatements=true"
db_properties = {
    "user": "root",
    "password": "password", 
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Function to execute multiple SQL queries in a single session
def execute_sql(queries):
    try:
        conn = spark._jvm.java.sql.DriverManager.getConnection(db_url, db_properties["user"], db_properties["password"])
        stmt = conn.createStatement()
        for query in queries:
            stmt.execute(query)
        stmt.close()
        conn.close()
    except Exception as e:
        print(f"SQL Execution Error: {e}")

def replace_nan_with_null(df):
    return df.select([
        when(isnan(col(c)) | col(c).isNull(), None).otherwise(col(c)).alias(c)
        if df.schema[c].dataType in [DoubleType(), FloatType()] else col(c)
        for c in df.columns
    ]) if df is not None else df

# Process each DataFrame with upsert logic
dataframes = {
    "dim_date": dim_date_df,
    "dim_genre": dim_genre_df,
    "dim_production_company": dim_production_company_df,
    "dim_production_countries": dim_production_countries_df,
    "dim_spoken_language": dim_spoken_language_df,
    "fact_movies": fact_movies_df,
    "dim_ratings": dim_ratings_df,
    "br_movie_genres": br_movie_genres_df,
    "br_movie_companies": br_movie_companies_df,
    "br_movie_countries": br_movie_countries_df,
    "br_movie_languages": br_movie_languages_df
}

upsert_templates = {
    "dim_date": """
        INSERT INTO dim_date 
        (date_id, full_date, year, quarter, month, month_name, day, 
         week_of_year, day_of_week, day_name, is_weekend)
        SELECT date_id, full_date, year, quarter, month, month_name, 
               day, week_of_year, day_of_week, day_name, is_weekend 
        FROM {temp}
        ON DUPLICATE KEY UPDATE
            full_date = VALUES(full_date),
            year = VALUES(year),
            quarter = VALUES(quarter),
            month = VALUES(month),
            month_name = VALUES(month_name),
            day = VALUES(day),
            week_of_year = VALUES(week_of_year),
            day_of_week = VALUES(day_of_week),
            day_name = VALUES(day_name),
            is_weekend = VALUES(is_weekend);
    """,
    
    "dim_genre": """
        INSERT INTO dim_genre (genre_id, genre_name)
        SELECT genre_id, genre_name
        FROM {temp}
        ON DUPLICATE KEY UPDATE
            genre_name = VALUES(genre_name);
    """,

    "dim_production_company": """
        INSERT INTO dim_production_company (company_id, company_name)
        SELECT company_id, company_name
        FROM {temp}
        ON DUPLICATE KEY UPDATE
            company_name = VALUES(company_name);
    """,

    "dim_production_countries": """
        INSERT INTO dim_production_countries (iso_3166_1, name)
        SELECT iso_3166_1, name
        FROM {temp}
        ON DUPLICATE KEY UPDATE
            name = VALUES(name);
    """,

    "dim_spoken_language": """
        INSERT INTO dim_spoken_language (iso_639_1, name)
        SELECT iso_639_1, name
        FROM {temp}
        ON DUPLICATE KEY UPDATE
            name = VALUES(name);
    """,

    "fact_movies": """
        INSERT INTO fact_movies (id, title, date_id, budget, revenue)
        SELECT id, title, date_id, budget, revenue
        FROM {temp}
        ON DUPLICATE KEY UPDATE
            title = VALUES(title),
            date_id = VALUES(date_id),
            budget = VALUES(budget),
            revenue = VALUES(revenue);
    """,

    "dim_ratings": """
        INSERT INTO dim_ratings (id, avg_rating, total_ratings, std_dev, last_rated)
        SELECT id, avg_rating, total_ratings, std_dev, last_rated
        FROM {temp}
        ON DUPLICATE KEY UPDATE
            avg_rating = VALUES(avg_rating),
            total_ratings = VALUES(total_ratings),
            std_dev = VALUES(std_dev),
            last_rated = VALUES(last_rated);
    """,

    "br_movie_genres": """
        INSERT INTO br_movie_genres (movie_id, genre_id)
        SELECT movie_id, genre_id
        FROM {temp}
        ON DUPLICATE KEY UPDATE
            movie_id = VALUES(movie_id),
            genre_id = VALUES(genre_id);
    """,

    "br_movie_companies": """
        INSERT INTO br_movie_companies (movie_id, company_id)
        SELECT movie_id, company_id
        FROM {temp}
        ON DUPLICATE KEY UPDATE
            movie_id = VALUES(movie_id),
            company_id = VALUES(company_id);
    """,

    "br_movie_countries": """
        INSERT INTO br_movie_countries (movie_id, iso_3166_1)
        SELECT movie_id, iso_3166_1
        FROM {temp}
        ON DUPLICATE KEY UPDATE
            movie_id = VALUES(movie_id),
            iso_3166_1 = VALUES(iso_3166_1);
    """,

    "br_movie_languages": """
        INSERT INTO br_movie_languages (movie_id, iso_639_1)
        SELECT movie_id, iso_639_1
        FROM {temp}
        ON DUPLICATE KEY UPDATE
            movie_id = VALUES(movie_id),
            iso_639_1 = VALUES(iso_639_1);
    """
}

for name, df in dataframes.items():
    try:
        cleaned_df = replace_nan_with_null(df)
        temp_table = f"temp_{name}"

        cleaned_df.write \
            .format("jdbc") \
            .option("url", db_url) \
            .option("dbtable", temp_table) \
            .option("user", db_properties["user"]) \
            .option("password", db_properties["password"]) \
            .option("driver", db_properties["driver"]) \
            .option("batchsize", 5000) \
            .mode("overwrite") \
            .save()

        upsert_query = upsert_templates[name].format(temp=temp_table)
        execute_sql([upsert_query, f"DROP TABLE IF EXISTS {temp_table};"])
        print(f"{name} successfully upserted to MySQL.")
    except Exception as e:
        print(f"Error processing {name}: {e}")
        
spark.stop()