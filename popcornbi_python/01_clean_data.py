from modules.config_general import os, pd, json, TMDb, Movie, ThreadPoolExecutor, logger, np
from modules.movies import Movies
from modules.ratings import Rating

logger.info("\n")

# Load Data (csv and json files)
movies_df = pd.read_csv('/Users/marcoo_sg/Desktop/PopcornBI/project_data/movies_main.csv', encoding='utf-8')
movie_extended_df = pd.read_csv('/Users/marcoo_sg/Desktop/PopcornBI/project_data/movie_extended.csv', encoding='utf-8')
ratings_data = json.load(open('/Users/marcoo_sg/Desktop/PopcornBI/project_data/ratings.json'))

# Convert to Movies objects
movies_list = [Movies.from_csv_row(row) for _, row in movies_df.iterrows()]

# Data Cleaning (Processing in Movies objects)
for movie in movies_list:
    movie.budget = pd.to_numeric(movie.budget, errors='coerce')
    movie.revenue = pd.to_numeric(movie.revenue, errors='coerce')

    if pd.isna(movie.budget):
        movie.budget = 0
    if pd.isna(movie.revenue):
        movie.revenue = 0
    
    def parse_date(date_str):
        for fmt in ('%m/%d/%Y', '%d-%m-%Y', '%Y-%m-%d'):
            try:
                return pd.to_datetime(date_str, format=fmt).strftime('%Y-%m-%d')
            except ValueError:
                pass
        return pd.NaT

    movie.release_date = parse_date(movie.release_date)
    
    movie.release_date = pd.to_datetime(movie.release_date, errors='coerce')

# Convert back to DataFrame
movies_df = pd.DataFrame([movie.__dict__ for movie in movies_list])

movies_df = movies_df.drop_duplicates()
movies_df = movies_df.drop_duplicates(subset='id', keep='first')
movies_df = movies_df.drop_duplicates(subset='title', keep='first')

movies_df = movies_df[movies_df['id'].astype(str).str.isdigit()]

movies_df = movies_df.dropna(subset=['title'])

### Fill missing release dates
tmdb = TMDb()
tmdb.api_key = ""
movie_api = Movie()

def fetch_release_date(title):
    try:
        search_results = movie_api.search(title)
        if search_results:
            return search_results[0].release_date 
    except Exception as e:
        logger.error(f"Error fetching release date for '{title}': {e}")
    return None 

def update_release_dates_in_parallel(df):
    with ThreadPoolExecutor(max_workers=10) as executor: 
        titles = df['title'].tolist()
        release_dates = list(executor.map(fetch_release_date, titles))
    df.loc[:, 'release_date'] = release_dates
    return df

missing_mask = movies_df['release_date'].isna()
movies_to_update = movies_df[missing_mask]
updated_movies = update_release_dates_in_parallel(movies_to_update)
movies_df.update(updated_movies)
movies_df['release_date'] = pd.to_datetime(movies_df['release_date'], errors='coerce')
movies_df = movies_df.dropna(subset=['release_date']).reset_index(drop=True)

### Fill missing budgets and revenues
budget_revenue_csv_dir = '/Users/marcoo_sg/Desktop/PopcornBI/project_data/budget_revenue_csv/'
os.makedirs(budget_revenue_csv_dir, exist_ok=True)

def merge_batch_files(batch_files):
    dataframes = []
    for batch_file in batch_files:
        try:
            df = pd.read_csv(batch_file, encoding='utf-8', on_bad_lines='skip', engine='python')
            dataframes.append(df)
        except UnicodeDecodeError:
            logger.info(f"Skipping file due to encoding error: {batch_file}")
        except Exception as e:
            logger.error(f"Error reading file {batch_file}: {e}")
    if dataframes:
        return pd.concat(dataframes, ignore_index=True)
    else:
        logger.error("No valid batch files to merge.")
        return pd.DataFrame()

def selective_update(original_df, updated_df):
    for index, updated_row in updated_df.iterrows():
        original_index = updated_row.name
        for column in ['budget', 'revenue']:
            if original_df.at[original_index, column] == 0:
                original_df.at[original_index, column] = updated_row[column]

if os.listdir(budget_revenue_csv_dir):
    updated_movies = merge_batch_files([os.path.join(budget_revenue_csv_dir, f) for f in os.listdir(budget_revenue_csv_dir)])
    selective_update(movies_df, updated_movies)

### Continue cleaning movies_df
movies_df = movies_df[~((movies_df['revenue'] == 0) | (movies_df['budget'] == 0))]
movies_df = movies_df[(movies_df['budget'] >= 100000) & (movies_df['revenue'] >= 100000)].reset_index(drop=True)

for col in movies_df.select_dtypes(include=[np.datetime64]).columns:
    movies_df[col] = movies_df[col].astype("datetime64[us]")

movies_df.to_parquet("/Users/marcoo_sg/Desktop/PopcornBI/project_data/cleaned_df/movies_df_cleaned.parquet", index=False)
logger.info("Successfully cleaned movies_df and saved to Parquet.")

## Clean movie_extended_df
### Remove rows where id is not in movies_df
movie_extended_df = movie_extended_df[movie_extended_df['id'].isin(movies_df['id'])].reset_index(drop=True)

movie_extended_df = movie_extended_df.drop_duplicates()

def is_null_or_empty_list(value):
    if pd.isnull(value):
        return True
    if isinstance(value, str):
        try:
            parsed_value = json.loads(value.replace("'", '"'))
            return parsed_value == []
        except json.JSONDecodeError:
            return False
    return False

movie_extended_df = movie_extended_df[
    ~(
        movie_extended_df['genres'].isnull() &
        movie_extended_df['production_companies'].isnull() &
        movie_extended_df['production_countries'].apply(is_null_or_empty_list) &
        movie_extended_df['spoken_languages'].apply(is_null_or_empty_list)
    )
].reset_index(drop=True)

movie_extended_df.to_parquet("/Users/marcoo_sg/Desktop/PopcornBI/project_data/cleaned_df/movie_extended_df_cleaned.parquet", index=False, engine="pyarrow")
logger.info("Successfully cleaned movie_extended_df and saved to Parquet.")

# ratings_df
ratings = list(map(lambda item: Rating.from_json(item), ratings_data))

ratings_df = pd.DataFrame([{
    'id': rating.movie_id,
    'avg_rating': rating.avg_rating,
    'total_ratings': rating.total_ratings,
    'std_dev': rating.std_dev,
    'last_rated': rating.last_rated
} for rating in ratings])

ratings_df = ratings_df[ratings_df['id'].isin(movies_df['id'].astype(int))]

ratings_df = ratings_df.drop_duplicates()

ratings_df.to_parquet("/Users/marcoo_sg/Desktop/PopcornBI/project_data/cleaned_df/ratings_df_cleaned.parquet", index=False, engine="pyarrow")
logger.info("Successfully cleaned ratings_df and saved to Parquet.")
