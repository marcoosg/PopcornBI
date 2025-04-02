CREATE TABLE dim_date (
    date_id INT PRIMARY KEY,
    full_date DATE,
    year INT,
    quarter INT,
    month INT,
    month_name VARCHAR(50),
    day INT,
    week_of_year INT,
    day_of_week INT,
    day_name VARCHAR(50),
    is_weekend BOOLEAN
);

CREATE TABLE dim_genre (
    genre_id INT PRIMARY KEY,
    genre_name VARCHAR(255)
);

CREATE TABLE dim_production_company (
    company_id INT PRIMARY KEY,
    company_name VARCHAR(255)
);

CREATE TABLE dim_production_countries (
    iso_3166_1 CHAR(3) PRIMARY KEY,
    name VARCHAR(255)
);

CREATE TABLE dim_spoken_language (
    iso_639_1 CHAR(3) PRIMARY KEY,
    name VARCHAR(255)
);

CREATE TABLE fact_movies (
    id INT PRIMARY KEY,
    title VARCHAR(255),
    date_id INT,
    budget FLOAT,
    revenue FLOAT,
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id)
);

CREATE TABLE dim_ratings (
    id INT PRIMARY KEY,
    avg_rating FLOAT,
    total_ratings FLOAT,
    std_dev FLOAT,
    last_rated INT,
    FOREIGN KEY (id) REFERENCES fact_movies(id)
);

CREATE TABLE br_movie_genres (
    movie_id INT,
    genre_id INT,
    PRIMARY KEY (movie_id, genre_id),
    FOREIGN KEY (movie_id) REFERENCES fact_movies(id),
    FOREIGN KEY (genre_id) REFERENCES dim_genre(genre_id)
);

CREATE TABLE br_movie_companies (
    movie_id INT,
    company_id INT,
    PRIMARY KEY (movie_id, company_id),
    FOREIGN KEY (movie_id) REFERENCES fact_movies(id),
    FOREIGN KEY (company_id) REFERENCES dim_production_company(company_id)
);

CREATE TABLE br_movie_countries (
    movie_id INT,
    iso_3166_1 CHAR(3),
    PRIMARY KEY (movie_id, iso_3166_1),
    FOREIGN KEY (movie_id) REFERENCES fact_movies(id),
    FOREIGN KEY (iso_3166_1) REFERENCES dim_production_countries(iso_3166_1)
);

CREATE TABLE br_movie_languages (
    movie_id INT,
    iso_639_1 CHAR(3),
    PRIMARY KEY (movie_id, iso_639_1),
    FOREIGN KEY (movie_id) REFERENCES fact_movies(id),
    FOREIGN KEY (iso_639_1) REFERENCES dim_spoken_language(iso_639_1)
);


-- Monthly revenue changes
-- if partitioned by year
SELECT 
    d.year,
    d.month,
    SUM(fm.revenue) AS total_revenue,
    LAG(SUM(fm.revenue)) OVER (PARTITION BY d.year ORDER BY d.month) AS prev_month_revenue,
    SUM(fm.revenue) - LAG(SUM(fm.revenue)) OVER (PARTITION BY d.year ORDER BY d.month) AS revenue_change
FROM fact_movies fm
JOIN dim_date d ON fm.date_id = d.date_id
GROUP BY d.year, d.month
ORDER BY d.year, d.month;
-- no partition by year, meaning continous comparison
SELECT 
    d.year,
    d.month,
    SUM(fm.revenue) AS total_revenue,
    LAG(SUM(fm.revenue)) OVER (ORDER BY d.year, d.month) AS prev_month_revenue,
    SUM(fm.revenue) - LAG(SUM(fm.revenue)) OVER (ORDER BY d.year, d.month) AS revenue_change
FROM fact_movies fm
JOIN dim_date d ON fm.date_id = d.date_id
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

-- Sequential movie performance
SELECT 
    fm.id AS movie_id,
    fm.title,
    d.full_date,
    fm.revenue,
    LEAD(fm.revenue) OVER (ORDER BY d.full_date) AS next_movie_revenue,
    fm.revenue - LEAD(fm.revenue) OVER (ORDER BY d.full_date) AS revenue_difference
FROM fact_movies fm
JOIN dim_date d ON fm.date_id = d.date_id
ORDER BY d.full_date;

-- Year-over-year comparisons
SELECT 
    d.year,
    SUM(fm.revenue) AS total_revenue,
    LAG(SUM(fm.revenue)) OVER (ORDER BY d.year) AS prev_year_revenue,
    SUM(fm.revenue) - LAG(SUM(fm.revenue)) OVER (ORDER BY d.year) AS yoy_change
FROM fact_movies fm
JOIN dim_date d ON fm.date_id = d.date_id
GROUP BY d.year
ORDER BY d.year;

-- Movie rankings by genre
SELECT 
    g.genre_name,
    fm.title,
    fm.revenue,
    ROW_NUMBER() OVER (PARTITION BY g.genre_name ORDER BY fm.revenue DESC) AS genre_rank
FROM fact_movies fm
JOIN br_movie_genres mg ON fm.id = mg.movie_id
JOIN dim_genre g ON mg.genre_id = g.genre_id;
-- top 5
SELECT *
FROM (
    SELECT 
		g.genre_name,
		fm.title,
		fm.revenue,
		ROW_NUMBER() OVER (PARTITION BY g.genre_name ORDER BY fm.revenue DESC) AS genre_rank
	FROM fact_movies fm
	JOIN br_movie_genres mg ON fm.id = mg.movie_id
	JOIN dim_genre g ON mg.genre_id = g.genre_id
) ranked_movies
WHERE genre_rank <= 5
ORDER BY genre_name, genre_rank;

-- Top performers by year
-- No limit in numebr of films per year
SELECT 
    d.year,
    fm.title,
    fm.revenue,
    ROW_NUMBER() OVER (PARTITION BY d.year ORDER BY fm.revenue DESC) AS year_rank
FROM fact_movies fm
JOIN dim_date d ON fm.date_id = d.date_id
ORDER BY d.year, year_rank;
-- top 5
SELECT *
FROM (
    SELECT 
        d.year,
        fm.title,
        fm.revenue,
        ROW_NUMBER() OVER (PARTITION BY d.year ORDER BY fm.revenue DESC) AS year_rank
    FROM fact_movies fm
    JOIN dim_date d ON fm.date_id = d.date_id
) ranked_movies
WHERE year_rank <= 5
ORDER BY year, year_rank;

-- Director Success Rate (no director in dataset)

-- Production Company Success Rates
SELECT 
    pc.company_name,
    COUNT(DISTINCT f.id) AS num_movies,
    SUM(f.revenue) AS total_revenue,
    AVG(r.avg_rating) AS avg_rating
FROM fact_movies f
JOIN br_movie_companies mc ON f.id = mc.movie_id
JOIN dim_production_company pc ON mc.company_id = pc.company_id
LEFT JOIN dim_ratings r ON f.id = r.id
GROUP BY pc.company_name
ORDER BY total_revenue DESC;

-- Highest-Grossing Movie Genres
SELECT 
    g.genre_name,
    COUNT(DISTINCT f.id) AS num_movies,
    SUM(f.revenue) AS total_revenue,
    AVG(r.avg_rating) AS avg_rating
FROM fact_movies f
JOIN br_movie_genres mg ON f.id = mg.movie_id
JOIN dim_genre g ON mg.genre_id = g.genre_id
LEFT JOIN dim_ratings r ON f.id = r.id
GROUP BY g.genre_name
ORDER BY total_revenue DESC;

-- Revenue Trends Over Time
-- yearly
SELECT 
    d.year,
    SUM(f.revenue) AS total_revenue
FROM fact_movies f
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY d.year
ORDER BY d.year;
-- monthly
SELECT 
    d.year,
    d.month_name,
    SUM(f.revenue) AS total_revenue
FROM fact_movies f
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;

















