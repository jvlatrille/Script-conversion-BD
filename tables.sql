-- Table all_movies
CREATE TABLE IF NOT EXISTS all_movies (
    id INT PRIMARY KEY,
    name TEXT,
    parent_id INT,
    date DATE,
    FOREIGN KEY (parent_id) REFERENCES all_movies(id)
);

-- Table all_series
CREATE TABLE IF NOT EXISTS all_series (
    id INT PRIMARY KEY,
    name TEXT,
    parent_id INT,
    date DATE,
    FOREIGN KEY (parent_id) REFERENCES all_series(id)
);

-- Table all_seasons
CREATE TABLE IF NOT EXISTS all_seasons (
    id INT PRIMARY KEY,
    name TEXT,
    parent_id INT,
    date DATE,
    FOREIGN KEY (parent_id) REFERENCES all_series(id)
);

-- Table all_episodes
CREATE TABLE IF NOT EXISTS all_episodes (
    id INT PRIMARY KEY,
    name TEXT,
    parent_id INT,
    date DATE,
    series_id INT,
    FOREIGN KEY (parent_id) REFERENCES all_seasons(id),
    FOREIGN KEY (series_id) REFERENCES all_series(id)
);

-- Table all_games
CREATE TABLE IF NOT EXISTS all_games (
    id INT PRIMARY KEY,
    name TEXT,
    parent_id INT,
    date DATE,
    FOREIGN KEY (parent_id) REFERENCES all_games(id)
);

-- Table all_movieseries
CREATE TABLE IF NOT EXISTS all_movieseries (
    id INT PRIMARY KEY,
    name TEXT,
    parent_id INT,
    date DATE,
    FOREIGN KEY (parent_id) REFERENCES all_movieseries(id)
);

-- Table all_people
CREATE TABLE IF NOT EXISTS all_people (
    id INT PRIMARY KEY,
    name TEXT,
    birthday DATE,
    deathday DATE,
    gender TEXT
);

-- Table all_people_aliases
CREATE TABLE IF NOT EXISTS all_people_aliases (
    person_id INT,
    name TEXT,
    FOREIGN KEY (person_id) REFERENCES all_people(id)
);

-- Table people_links
CREATE TABLE IF NOT EXISTS people_links (
    source TEXT,
    link_key TEXT,  -- Renommé pour éviter l'utilisation du mot réservé "key"
    people_id INT,
    language_iso_639_1 CHAR(2),
    FOREIGN KEY (people_id) REFERENCES all_people(id)
);


-- Table all_casts
CREATE TABLE IF NOT EXISTS all_casts (
    movie_id INT,
    person_id INT,
    job_id INT,
    role TEXT,
    position INT,
    FOREIGN KEY (movie_id) REFERENCES all_movies(id),
    FOREIGN KEY (person_id) REFERENCES all_people(id),
    FOREIGN KEY (job_id) REFERENCES job_names(job_id)
);

-- Table job_names
CREATE TABLE IF NOT EXISTS job_names (
    job_id INT PRIMARY KEY,
    name TEXT,
    language_iso_639_1 CHAR(2)
);

-- Table all_characters
CREATE TABLE IF NOT EXISTS all_characters (
    id INT PRIMARY KEY,
    name TEXT
);

-- Table movie_categories
CREATE TABLE IF NOT EXISTS movie_categories (
    movie_id INT,
    category_id INT,
    FOREIGN KEY (movie_id) REFERENCES all_movies(id),
    FOREIGN KEY (category_id) REFERENCES all_categories(id)
);

-- Table movie_keywords
CREATE TABLE IF NOT EXISTS movie_keywords (
    movie_id INT,
    category_id INT,
    FOREIGN KEY (movie_id) REFERENCES all_movies(id),
    FOREIGN KEY (category_id) REFERENCES all_categories(id)
);

-- Table category_names
CREATE TABLE IF NOT EXISTS category_names (
    category_id INT PRIMARY KEY,
    name TEXT,
    language_iso_639_1 CHAR(2)
);

-- Table all_categories
CREATE TABLE IF NOT EXISTS all_categories (
    id INT PRIMARY KEY,
    parent_id INT,
    root_id INT,
    FOREIGN KEY (parent_id) REFERENCES all_categories(id),
    FOREIGN KEY (root_id) REFERENCES all_categories(id)
);

-- Table trailers
CREATE TABLE IF NOT EXISTS trailers (
    trailer_id INT PRIMARY KEY,
    link_key TEXT,  -- Renommé pour éviter le mot réservé "key"
    movie_id INT,
    language_iso_639_1 CHAR(2),
    source TEXT,
    FOREIGN KEY (movie_id) REFERENCES all_movies(id)
);


-- Table movie_links
CREATE TABLE IF NOT EXISTS movie_links (
    source TEXT,
    link_key TEXT,  -- Renommé pour éviter le mot réservé "key"
    movie_id INT,
    language_iso_639_1 CHAR(2),
    FOREIGN KEY (movie_id) REFERENCES all_movies(id)
);


-- Table image_ids
CREATE TABLE IF NOT EXISTS image_ids (
    image_id INT PRIMARY KEY,
    object_id INT,
    object_type TEXT,
    image_version TEXT
);

-- Table image_licenses
CREATE TABLE IF NOT EXISTS image_licenses (
    image_id INT,
    source TEXT,
    license_id INT,
    author TEXT,
    FOREIGN KEY (image_id) REFERENCES image_ids(image_id)
);

-- Table all_movie_aliases_iso
CREATE TABLE IF NOT EXISTS all_movie_aliases_iso (
    movie_id INT,
    name TEXT,
    language_iso_639_1 CHAR(2),
    official_translation BOOLEAN,
    FOREIGN KEY (movie_id) REFERENCES all_movies(id)
);

-- Table all_votes
CREATE TABLE IF NOT EXISTS all_votes (
    movie_id INT,
    vote_average FLOAT,
    votes_count INT,
    FOREIGN KEY (movie_id) REFERENCES all_movies(id)
);

-- Table movie_languages
CREATE TABLE IF NOT EXISTS movie_languages (
    movie_id INT,
    language_iso_639_1 CHAR(2),
    FOREIGN KEY (movie_id) REFERENCES all_movies(id)
);

-- Table movie_countries
CREATE TABLE IF NOT EXISTS movie_countries (
    movie_id INT,
    country_code CHAR(2),
    FOREIGN KEY (movie_id) REFERENCES all_movies(id)
);

-- Table movie_details
CREATE TABLE IF NOT EXISTS movie_details (
    movie_id INT,
    runtime INT,
    budget BIGINT,
    revenue BIGINT,
    homepage TEXT,
    FOREIGN KEY (movie_id) REFERENCES all_movies(id)
);

-- Table movie_references
CREATE TABLE IF NOT EXISTS movie_references (
    movie_id INT,
    referenced_id INT,
    type TEXT,
    FOREIGN KEY (movie_id) REFERENCES all_movies(id),
    FOREIGN KEY (referenced_id) REFERENCES all_movies(id)
);

-- Table movie_abstracts_fr
CREATE TABLE IF NOT EXISTS movie_abstracts_fr (
    movie_id INT,
    abstract TEXT,
    FOREIGN KEY (movie_id) REFERENCES all_movies(id)
);

-- Table movie_content_updates
CREATE TABLE IF NOT EXISTS movie_content_updates (
    movie_id INT,
    last_update DATE,
    FOREIGN KEY (movie_id) REFERENCES all_movies(id)
);
