-- CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;
-- CREATE EXTENSION IF NOT EXISTS pg_trgm;

DROP TABLE IF EXISTS l1_books;
DROP TABLE IF EXISTS l2_books;
DROP TABLE IF EXISTS l2_ratings;

CREATE TABLE l1_books (
    correct BOOLEAN,
    title TEXT,
    description TEXT,
    authors TEXT,
    publisher TEXT,
    publisheddate TEXT,
    categories TEXT
);

CREATE TABLE l2_books (
    isbn TEXT,
    book_title TEXT,
    book_author TEXT
);

CREATE TABLE l2_ratings (
    user_id TEXT,
    isbn TEXT,
    book_rating TEXT
);

-- CREATE INDEX idx_l1_books_title_trgm ON l1_books
-- USING gin (title gin_trgm_ops);

-- Import data from CSVs
COPY l1_books FROM '/docker-entrypoint-initdb.d/L1-books-reviewed.csv' DELIMITER ',' CSV HEADER;
COPY l2_books FROM '/docker-entrypoint-initdb.d/L2-books.csv' DELIMITER ',' CSV HEADER;
COPY l2_ratings FROM '/docker-entrypoint-initdb.d/L2-ratings.csv' DELIMITER ',' CSV HEADER;
