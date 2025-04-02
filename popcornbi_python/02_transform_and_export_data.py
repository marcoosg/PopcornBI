from modules.config_pyspark import spark
from modules.config_general import languages, logger
from pyspark.sql.functions import (
    col, to_date, year, quarter, month, dayofmonth, weekofyear, dayofweek,
    date_format, when, lit, explode, split, row_number, trim,
    from_json, replace, array, udf, isnan
)
from pyspark.sql.types import (
    IntegerType, BooleanType, StructType, StructField, StringType, 
    ArrayType, DoubleType, FloatType
)
from pyspark.sql.window import Window

# import movie_df_cleaned
movies_spark_df = (spark.read.csv("/Users/marcoo_sg/Desktop/PopcornBI/project_data/cleaned_df/movies_df_cleaned.csv", header=True, inferSchema=True))

# Create dim_date
dim_date_df = (movies_spark_df
    .select(to_date(col("release_date")).alias("full_date"))
    .filter(col("full_date").isNotNull())
    .distinct()
    .orderBy("full_date")
    .select(
        date_format(col("full_date"), "yyyyMMdd").cast(IntegerType()).alias("date_id"),
        col("full_date"),
        year(col("full_date")).alias("year"),
        quarter(col("full_date")).alias("quarter"),
        month(col("full_date")).alias("month"),
        date_format(col("full_date"), "MMMM").alias("month_name"),
        dayofmonth(col("full_date")).alias("day"),
        weekofyear(col("full_date")).alias("week_of_year"),
        dayofweek(col("full_date")).alias("day_of_week"),
        date_format(col("full_date"), "EEEE").alias("day_name"),
        (dayofweek(col("full_date")) >= 5).cast(BooleanType()).alias("is_weekend")
    )
)

# Create fact_movies
fact_movies_df = (movies_spark_df
    .withColumn("date_id", date_format(to_date(col("release_date")), "yyyyMMdd").cast(IntegerType()))
    .drop("release_date")
)

# Import movie_extended_df_cleaned
movies_extended_spark_df = (spark.read.csv("/Users/marcoo_sg/Desktop/PopcornBI/project_data/cleaned_df/movie_extended_df_cleaned.csv", header=True, inferSchema=True))

# Create dim_genre
dim_genre_df = (movies_extended_spark_df
    .select(explode(split(col("genres"), ",")).alias("genre_name"))
    .filter(col("genre_name").isNotNull()) 
    .distinct()  
    .select(
        col("genre_name").alias("genre_name").cast("string").alias("genre_name")
    )
    .orderBy("genre_name") 
    .withColumn("genre_id", row_number().over(Window.orderBy("genre_name")))  
)

# Created dim_production_company
dim_production_company_df = (movies_extended_spark_df
    .select(explode(split(col("production_companies"), ",")).alias("company_name"))
    .filter(col("company_name").isNotNull()) 
    .filter(trim(col("company_name")) != "") 
    .distinct() 
    .orderBy("company_name")  
    .withColumn("company_id", row_number().over(Window.orderBy("company_name")))  
    .select(
        col("company_id").cast("integer").alias("company_id"),
        col("company_name").cast("string").alias("company_name")
    )
)

# Create dim_production_countries
country_schema = ArrayType(StructType([
    StructField("iso_3166_1", StringType(), True),
    StructField("name", StringType(), True)
]))

dim_production_countries_df = (movies_extended_spark_df
    .filter(col("production_countries").isNotNull())  
    .select(
        explode(
            when(
                col("production_countries").cast("string").isNotNull(),
                from_json(
                    replace(col("production_countries").cast("string"), lit("'"), lit('"')),  
                    country_schema
                )
            ).otherwise(array().cast(country_schema))  
        ).alias("country")
    )
    .filter(col("country.iso_3166_1").isNotNull() & col("country.name").isNotNull())  
    .select(
        col("country.iso_3166_1").alias("iso_3166_1"),
        col("country.name").alias("name")
    )
    .distinct()  
    .orderBy("iso_3166_1", "name") 
)

# Create dim_spoken_languages
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

dim_spoken_language_df = (movies_extended_spark_df
    .filter(col("spoken_languages").isNotNull()) 
    .select(
        explode(
            when(
                col("spoken_languages").cast("string").isNotNull(),
                from_json(
                    col("spoken_languages"), language_schema
                )
            ).otherwise(array().cast(language_schema))
        ).alias("language")
    )
    .filter(col("language.iso_639_1").isNotNull() & col("language.name").isNotNull()) 
    .select(
        col("language.iso_639_1").alias("iso_639_1"),
        col("language.name").alias("name")
    )
    .distinct() 
    .withColumn("name", when(
        (col("name").isNull()) | (col("name").rlike(r"^\s*$")) | (col("name") == "?????"),
        get_language_name_udf(col("iso_639_1"))
    ).otherwise(col("name")))
    .orderBy("iso_639_1", "name")  
)

# Create bridge tables
company_schema = ArrayType(StructType([
    StructField("company_name", StringType(), True)
]))

country_schema = ArrayType(StructType([
    StructField("iso_3166_1", StringType(), True)
]))

language_schema = ArrayType(StructType([
    StructField("iso_639_1", StringType(), True)
]))

br_movie_genres_df = (movies_extended_spark_df.alias("m")
    .select(col("m.id").alias("movie_id"), explode(split(col("m.genres"), ",")).alias("m_genre_name"))  # Rename to avoid ambiguity
    .join(dim_genre_df.alias("dg"), col("m_genre_name") == col("dg.genre_name"), "inner")  # Use new name
    .select(col("movie_id"), col("dg.genre_id"))
    .distinct()
)

br_movie_companies_df = (movies_extended_spark_df.alias("m")
    .select(col("m.id").alias("movie_id"), explode(split(col("m.production_companies"), ",")).alias("m_company_name"))  # Rename to avoid ambiguity
    .join(dim_production_company_df.alias("dpc"), col("m_company_name") == col("dpc.company_name"), "inner")  # Use new name
    .select(col("movie_id"), col("dpc.company_id"))
    .distinct()
)

br_movie_countries_df = (movies_extended_spark_df.alias("m")
    .select(col("m.id").alias("movie_id"), explode(from_json(col("m.production_countries"), country_schema)).alias("country"))
    .join(dim_production_countries_df.alias("dpcn"), col("country.iso_3166_1") == col("dpcn.iso_3166_1"), "inner")
    .select(col("movie_id"), col("dpcn.iso_3166_1"))
    .distinct()
)

br_movie_languages_df = (movies_extended_spark_df.alias("m")
    .select(col("m.id").alias("movie_id"), explode(from_json(col("m.spoken_languages"), language_schema)).alias("language"))
    .join(dim_spoken_language_df.alias("dsl"), col("language.iso_639_1") == col("dsl.iso_639_1"), "inner")
    .select(col("movie_id"), col("dsl.iso_639_1"))
    .distinct()
)

dim_ratings_df = (spark.read.csv("/Users/marcoo_sg/Desktop/PopcornBI/project_data/cleaned_df/ratings_df_cleaned.csv", header=True, inferSchema=True))

db_url = "jdbc:mysql://localhost:3306/popcornbi"
db_properties = {
    "user": "root",
    "password": "password",  
    "driver": "com.mysql.cj.jdbc.Driver"
}

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

tables = [
    "fact_movies", "dim_date", "dim_ratings", "dim_genre", "dim_production_company", 
    "dim_production_countries", "dim_spoken_language", "br_movie_genres", "br_movie_companies", 
    "br_movie_countries", "br_movie_languages"
]

queries = ["SET FOREIGN_KEY_CHECKS = 0;"] + [f"DELETE FROM {table};" for table in tables] + ["SET FOREIGN_KEY_CHECKS = 1;"]
execute_sql(queries)

def replace_nan_with_null(df):
    if df is not None:
        return df.select([
            when(isnan(col(c)) | col(c).isNull(), None).otherwise(col(c)).alias(c)
            if df.schema[c].dataType in [DoubleType(), FloatType()] else col(c)
            for c in df.columns
        ])
    return df

if dim_date_df is not None:
    dim_date_df = dim_date_df.withColumn("full_date", col("full_date").cast(StringType()))

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

for name, df in dataframes.items():
    try:
        if df is not None:
            cleaned_df = replace_nan_with_null(df)
            cleaned_df.write.jdbc(url=db_url, table=name, mode="append", properties=db_properties)
            print(f"{name} successfully written to MySQL.")
    except Exception as e:
        print(f"Error processing {name}: {e}")

spark.stop()
